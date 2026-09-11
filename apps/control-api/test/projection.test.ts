import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { after, before, beforeEach, describe, test } from 'node:test';

import { openDatabase } from '../src/db.js';
import { GatewayClient } from '../src/gateway.js';
import { Projection } from '../src/projection.js';
import { FakeGateway, reportEvent, resetEventIds, tradeEvent } from './fake-gateway.js';

const gateway = new FakeGateway();
let dbDir: string;

describe('projection', { concurrency: 1 }, () => {
before(async () => {
  await gateway.start();
});

after(async () => {
  await gateway.stop();
  if (dbDir !== undefined) {
    fs.rmSync(dbDir, { recursive: true, force: true });
  }
});

beforeEach(() => {
  resetEventIds();
  gateway.reset();
  gateway.runId = '1';
});

function newDb(name: string) {
  dbDir ??= fs.mkdtempSync(path.join(os.tmpdir(), 'rltl-db-'));
  return openDatabase(path.join(dbDir, `${name}.db`));
}

test('trades and reports become a queryable projection', async () => {
  const db = newDb('basic');
  const client = new GatewayClient(gateway.socketPath, undefined);
  const projection = new Projection(db, client);
  gateway.publish([reportEvent(), tradeEvent(), tradeEvent({ aggressor: 'sell' })]);

  assert.equal(await projection.poll(), 3);
  assert.equal(await projection.poll(), 0);

  const trades = db.prepare('SELECT COUNT(*) AS total FROM trades').get() as { total: bigint };
  assert.equal(trades.total, 2n);

  const orders = db.prepare('SELECT * FROM orders').all() as { order_id: bigint }[];
  assert.equal(orders.length, 1);

  // Buy aggressor then sell aggressor on the same pair nets the taker to zero.
  const taker = db
    .prepare('SELECT position_lots FROM positions WHERE account = 2')
    .get() as { position_lots: bigint };
  assert.equal(taker.position_lots, 0n);
  const maker = db
    .prepare('SELECT position_lots FROM positions WHERE account = 1')
    .get() as { position_lots: bigint };
  assert.equal(maker.position_lots, 0n);

  client.close();
  db.close();
});

test('a restarted projection resumes from the stored output sequence', async () => {
  const file = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'rltl-resume-')), 'p.db');
  const client = new GatewayClient(gateway.socketPath, undefined);
  gateway.publish([tradeEvent(), tradeEvent(), tradeEvent()]);

  const first = openDatabase(file);
  const before = new Projection(first, client);
  assert.equal(await before.poll(), 3);
  const position = before.position;
  first.close();

  const second = openDatabase(file);
  const resumed = new Projection(second, client);
  assert.equal(resumed.position.lastOutputSeq, position.lastOutputSeq);

  // Replaying the same events must not double count.
  gateway.rewind();
  assert.equal(await resumed.poll(), 3);
  const trades = second.prepare('SELECT COUNT(*) AS total FROM trades').get() as { total: bigint };
  assert.equal(trades.total, 3n);
  const maker = second
    .prepare('SELECT position_lots FROM positions WHERE account = 1')
    .get() as { position_lots: bigint };
  assert.equal(maker.position_lots, -15n);

  client.close();
  second.close();
});

test('terminal reports remove the order and rejects are counted by reason', async () => {
  const db = newDb('terminal');
  const client = new GatewayClient(gateway.socketPath, undefined);
  const projection = new Projection(db, client);
  gateway.publish([
    reportEvent(),
    reportEvent({ orderState: 'filled', reportKind: 'filled', cumulativeFilledLots: '10' }),
    reportEvent({
      orderId: '0',
      orderState: 'rejected',
      reportKind: 'rejected',
      rejectReason: 'max_order_quantity',
    }),
    reportEvent({
      orderId: '0',
      orderState: 'rejected',
      reportKind: 'rejected',
      rejectReason: 'max_order_quantity',
    }),
  ]);
  await projection.poll();

  const orders = db.prepare('SELECT COUNT(*) AS total FROM orders').get() as { total: bigint };
  assert.equal(orders.total, 0n);
  const reject = db
    .prepare('SELECT count FROM risk_rejects WHERE reason = ?')
    .get('max_order_quantity') as { count: bigint };
  assert.equal(reject.count, 2n);

  client.close();
  db.close();
});

test('a gateway restart is survivable and duplicate output is ignored', async () => {
  const db = newDb('restart');
  const client = new GatewayClient(gateway.socketPath, undefined);
  const projection = new Projection(db, client);
  gateway.publish([tradeEvent(), tradeEvent()]);
  await projection.poll();

  await gateway.restart();
  gateway.rewind();

  // The client reconnects and the already-applied prefix is skipped.
  assert.equal(await projection.poll(), 2);
  const trades = db.prepare('SELECT COUNT(*) AS total FROM trades').get() as { total: bigint };
  assert.equal(trades.total, 2n);
  assert.equal(projection.counters.eventsApplied, 2n);

  client.close();
  db.close();
});

test('a silent gateway times out instead of hanging the control plane', async () => {
  const db = newDb('timeout');
  const client = new GatewayClient(gateway.socketPath, undefined, 150);
  const projection = new Projection(db, client);
  gateway.silent = true;
  await assert.rejects(
    () => projection.poll(),
    (error: Error & { code?: string }) => error.code === 'timeout',
  );
  assert.equal(projection.counters.pollErrors, 1n);
  gateway.silent = false;

  client.close();
  db.close();
});

test('the same run resumes and a different run does not merge', async () => {
  const file = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'rltl-runid-')), 'p.db');
  const client = new GatewayClient(gateway.socketPath, undefined);
  gateway.runId = 'A';
  gateway.publish([tradeEvent({ tradeId: '1', outputSeq: '1' }), tradeEvent({ tradeId: '2', outputSeq: '2' })]);

  const first = openDatabase(file);
  const before = new Projection(first, client);
  assert.equal(await before.poll(), 2);
  first.close();

  const resumedDb = openDatabase(file);
  const resumed = new Projection(resumedDb, client);
  assert.equal(resumed.position.runId, 'A');
  assert.equal(resumed.health, 'healthy');
  gateway.rewind();
  assert.equal(await resumed.poll(), 2);
  assert.equal(
    (resumedDb.prepare('SELECT COUNT(*) AS total FROM trades').get() as { total: bigint }).total,
    2n,
  );

  gateway.reset();
  resetEventIds();
  gateway.runId = 'B';
  gateway.publish([
    tradeEvent({ tradeId: '1', outputSeq: '1', quantityLots: '99' }),
    tradeEvent({ tradeId: '2', outputSeq: '2', quantityLots: '99' }),
  ]);
  assert.equal(await resumed.poll(), 0);
  assert.equal(resumed.health, 'run_mismatch');
  const qty = resumedDb.prepare('SELECT quantity_lots FROM trades WHERE trade_id = 1').get() as {
    quantity_lots: bigint;
  };
  assert.equal(qty.quantity_lots, 5n);

  client.close();
  resumedDb.close();
});

test('a sequence gap latches degraded state until an explicit resync', async () => {
  const db = newDb('gap');
  const client = new GatewayClient(gateway.socketPath, undefined);
  const projection = new Projection(db, client);
  gateway.publish([tradeEvent(), tradeEvent()]);
  assert.equal(await projection.poll(), 2);

  gateway.publish([tradeEvent({ outputSeq: '4', tradeId: '4' })]);
  await projection.poll();
  assert.equal(projection.health, 'degraded');
  assert.equal(projection.counters.gapsObserved, 1n);
  assert.equal((db.prepare('SELECT COUNT(*) AS total FROM trades').get() as { total: bigint }).total, 2n);

  gateway.publish([tradeEvent({ outputSeq: '5', tradeId: '5' })]);
  assert.equal(await projection.poll(), 0);
  assert.equal(projection.health, 'degraded');
  assert.equal((db.prepare('SELECT COUNT(*) AS total FROM trades').get() as { total: bigint }).total, 2n);

  gateway.reset();
  resetEventIds();
  gateway.publish([tradeEvent(), tradeEvent()]);
  await projection.rebuild();
  assert.equal(projection.health, 'healthy');
  assert.equal(await projection.poll(), 2);
  assert.equal(projection.health, 'healthy');
  assert.equal((db.prepare('SELECT COUNT(*) AS total FROM trades').get() as { total: bigint }).total, 2n);

  client.close();
  db.close();
});
});
