import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test, { after, before } from 'node:test';

import type { FastifyInstance } from 'fastify';
import { openDatabase } from '../src/db.js';
import { GatewayClient } from '../src/gateway.js';
import { Projection } from '../src/projection.js';
import { buildServer } from '../src/server.js';
import { SettlementDispatcher } from '../src/settlement/dispatcher.js';
import { SettlementOutbox } from '../src/settlement/outbox.js';
import { SimulatedVenue } from '../src/settlement/simulated.js';
import { FakeGateway, resetEventIds, tradeEvent } from './fake-gateway.js';

process.env.RLTL_LOG_LEVEL = 'silent';

const gateway = new FakeGateway();
const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'rltl-stream-'));

let app: FastifyInstance;
let client: GatewayClient;
let projection: Projection;
let address: string;

before(async () => {
  await gateway.start();
  resetEventIds();

  const db = openDatabase(path.join(dir, 'stream.db'));
  client = new GatewayClient(gateway.socketPath, undefined, 1_000);
  projection = new Projection(db, client);
  const venue = new SimulatedVenue();
  const outbox = new SettlementOutbox(db, venue.venue);

  app = buildServer({
    config: {
      host: '127.0.0.1',
      port: 0,
      socketPath: gateway.socketPath,
      databasePath: ':memory:',
      gatewayToken: undefined,
      apiToken: 'token',
      pollIntervalMs: 50,
      settlementBatchSize: 10,
    },
    db,
    gateway: client,
    projection,
    outbox,
    dispatcher: new SettlementDispatcher(outbox, venue, 10),
  });
  address = await app.listen({ host: '127.0.0.1', port: 0 });
});

after(async () => {
  await app.close();
  client.close();
  await gateway.stop();
  fs.rmSync(dir, { recursive: true, force: true });
});

test('the stream carries a projection sequence a client can gap-check', async () => {
  const url = `${address.replace('http', 'ws')}/stream`;
  const socket = new WebSocket(url);
  const frames: Record<string, unknown>[] = [];
  socket.addEventListener('message', (event) => {
    frames.push(JSON.parse(String(event.data)) as Record<string, unknown>);
  });
  await new Promise((resolve) => socket.addEventListener('open', resolve, { once: true }));

  gateway.publish([tradeEvent(), tradeEvent()]);
  await projection.poll();
  await new Promise((resolve) => setTimeout(resolve, 100));

  assert.equal(frames[0]?.type, 'hello');
  const events = frames.filter((frame) => frame.type === 'event');
  assert.equal(events.length, 2);
  const sequences = events.map((frame) => BigInt(frame.projectionSeq as string));
  assert.equal(sequences[1]! - sequences[0]!, 1n);

  socket.close();
});
