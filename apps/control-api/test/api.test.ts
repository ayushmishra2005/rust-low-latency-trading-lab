import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test, { after, before } from 'node:test';

import type { FastifyInstance } from 'fastify';
import { openDatabase } from '../src/db.js';
import { GatewayClient } from '../src/gateway.js';
import { Projection } from '../src/projection.js';
import { buildServer, websocketWouldBlock } from '../src/server.js';
import { SettlementDispatcher } from '../src/settlement/dispatcher.js';
import { SettlementOutbox } from '../src/settlement/outbox.js';
import { SimulatedVenue } from '../src/settlement/simulated.js';
import { FakeGateway, reportEvent, resetEventIds, tradeEvent } from './fake-gateway.js';

process.env.RLTL_LOG_LEVEL = 'silent';

const gateway = new FakeGateway();
const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'rltl-api-'));
const API_TOKEN = 'test-token';
const auth = { authorization: `Bearer ${API_TOKEN}` };

let app: FastifyInstance;
let client: GatewayClient;
let projection: Projection;

before(async () => {
  await gateway.start();
  resetEventIds();
  gateway.publish([reportEvent(), tradeEvent(), tradeEvent({ aggressor: 'sell' })]);

  const db = openDatabase(path.join(dir, 'api.db'));
  client = new GatewayClient(gateway.socketPath, undefined, 1_000);
  projection = new Projection(db, client);
  await projection.poll();

  const venue = new SimulatedVenue();
  const outbox = new SettlementOutbox(db, venue.venue);
  const dispatcher = new SettlementDispatcher(outbox, venue, 10);
  await dispatcher.tick();

  app = buildServer({
    config: {
      host: '127.0.0.1',
      port: 0,
      socketPath: gateway.socketPath,
      databasePath: ':memory:',
      gatewayToken: undefined,
      apiToken: API_TOKEN,
      pollIntervalMs: 100,
      settlementBatchSize: 10,
    },
    db,
    gateway: client,
    projection,
    outbox,
    dispatcher,
  });
  await app.ready();
});

after(async () => {
  await app.close();
  client.close();
  await gateway.stop();
  fs.rmSync(dir, { recursive: true, force: true });
});

test('read endpoints report how far the projection has read', async () => {
  for (const url of ['/orders', '/trades', '/positions', '/settlements']) {
    const response = await app.inject({ method: 'GET', url, headers: auth });
    assert.equal(response.statusCode, 200, url);
    const body = response.json() as { asOfEngineSeq: string; projectionSeq: string };
    assert.match(body.asOfEngineSeq, /^\d+$/);
    assert.match(body.projectionSeq, /^\d+$/);
  }

  const trades = (await app.inject({ method: 'GET', url: '/trades', headers: auth })).json() as {
    trades: { trade_id: string; quantity_lots: string }[];
  };
  assert.equal(trades.trades.length, 2);
  // Wide values leave the API as strings.
  assert.equal(typeof trades.trades[0]!.trade_id, 'string');
  assert.equal(typeof trades.trades[0]!.quantity_lots, 'string');
});

test('the book comes from the engine snapshot and unknown symbols are 404', async () => {
  const response = await app.inject({ method: 'GET', url: '/book/LAB-USD?depth=1', headers: auth });
  assert.equal(response.statusCode, 200);
  const body = response.json() as {
    bids: unknown[];
    asks: unknown[];
    asOfEngineSeq: string;
  };
  assert.equal(body.bids.length, 1);
  assert.equal(body.asks.length, 1);
  assert.match(body.asOfEngineSeq, /^\d+$/);

  const missing = await app.inject({ method: 'GET', url: '/book/NOPE', headers: auth });
  assert.equal(missing.statusCode, 404);
});

test('metrics expose bounded labels only', async () => {
  const response = await app.inject({ method: 'GET', url: '/metrics', headers: auth });
  assert.equal(response.statusCode, 200);
  assert.match(response.headers['content-type'] as string, /text\/plain/);
  const body = response.body;
  assert.match(body, /# TYPE rltl_projection_events_total counter/);
  assert.match(body, /rltl_settlements\{status="confirmed"\}/);
  assert.match(body, /rltl_feed_source_seq\{symbol="LAB-USD"\}/);

  for (const line of body.split('\n')) {
    if (line.startsWith('#') || line.length === 0) {
      continue;
    }
    const labels = /\{(.*)\}/.exec(line)?.[1] ?? '';
    for (const key of labels.split(',').filter((entry) => entry.length > 0)) {
      const name = key.split('=')[0]!;
      assert.ok(
        ['reason', 'status', 'outcome', 'symbol', 'state'].includes(name),
        `unbounded metric label ${name}`,
      );
    }
  }
});

test('mutations require a bearer token', async () => {
  const denied = await app.inject({
    method: 'POST',
    url: '/engine/kill',
    payload: { engaged: true },
  });
  assert.equal(denied.statusCode, 401);

  const wrong = await app.inject({
    method: 'POST',
    url: '/engine/kill',
    headers: { authorization: 'Bearer nope' },
    payload: { engaged: true },
  });
  assert.equal(wrong.statusCode, 401);

  const accepted = await app.inject({
    method: 'POST',
    url: '/engine/kill',
    headers: { authorization: `Bearer ${API_TOKEN}` },
    payload: { engaged: true },
  });
  assert.equal(accepted.statusCode, 200);
  assert.equal((accepted.json() as { accepted: boolean }).accepted, true);
});

test('request bodies are schema validated', async () => {
  const headers = { authorization: `Bearer ${API_TOKEN}` };

  const badKill = await app.inject({
    method: 'POST',
    url: '/engine/kill',
    headers,
    payload: { engaged: 'yes' },
  });
  assert.equal(badKill.statusCode, 400);

  const badLimits = await app.inject({
    method: 'POST',
    url: '/risk/limits',
    headers,
    payload: { account: 1, maxOrderQuantity: 100 },
  });
  assert.equal(badLimits.statusCode, 400);

  const limits = await app.inject({
    method: 'POST',
    url: '/risk/limits',
    headers,
    payload: {
      account: 1,
      maxOrderQuantity: '1000',
      maxOrderNotional: '100000000',
      maxPositionLots: '5000',
      maxGrossExposure: '900000000000',
      priceCollarTicks: 50,
    },
  });
  assert.equal(limits.statusCode, 200);

  const tooLarge = await app.inject({
    method: 'POST',
    url: '/replay/start',
    headers,
    payload: { seed: 1, events: 10_000_000 },
  });
  assert.equal(tooLarge.statusCode, 400);
});

test('the operational API exposes no order entry', async () => {
  for (const url of ['/orders', '/order', '/orders/new', '/trades']) {
    const response = await app.inject({ method: 'POST', url, payload: {} });
    assert.notEqual(response.statusCode, 200, `${url} must not accept order entry`);
  }
});

test('sensitive reads require a bearer token', async () => {
  for (const url of ['/orders', '/trades', '/positions', '/settlements', '/metrics', '/book/LAB-USD']) {
    const denied = await app.inject({ method: 'GET', url });
    assert.equal(denied.statusCode, 401, url);
  }
  const health = await app.inject({ method: 'GET', url: '/health' });
  assert.equal(health.statusCode, 200);
});

test('a slow websocket client is treated as blocked once the buffer threshold is crossed', () => {
  assert.equal(websocketWouldBlock(1_048_576, 1_048_576), false);
  assert.equal(websocketWouldBlock(1_048_577, 1_048_576), true);
});

test('health degrades instead of failing when the gateway is silent', async () => {
  gateway.silent = true;
  const response = await app.inject({ method: 'GET', url: '/health' });
  gateway.silent = false;
  assert.equal(response.statusCode, 200);
  const body = response.json() as { status: string; gatewayError: string | null };
  assert.equal(body.status, 'degraded');
  assert.notEqual(body.gatewayError, null);
});
