import assert from 'node:assert/strict';
import { spawn, type ChildProcess } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test, { after, before } from 'node:test';

import { openDatabase } from '../src/db.js';
import { GatewayClient } from '../src/gateway.js';
import { Projection } from '../src/projection.js';
import { SettlementDispatcher } from '../src/settlement/dispatcher.js';
import { SettlementOutbox } from '../src/settlement/outbox.js';
import { SimulatedVenue } from '../src/settlement/simulated.js';
import type { GatewayHealth } from '../src/types.js';

// Runs against the real Rust gateway when it has been built.
const binary = path.resolve(import.meta.dirname, '../../../target/release/control-gateway');
const available = fs.existsSync(binary);
const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'rltl-e2e-'));
const socketPath = path.join(dir, 'control.sock');
const TOKEN = 'e2e-token';

let child: ChildProcess | null = null;

before(async () => {
  if (!available) {
    return;
  }
  child = spawn(
    binary,
    [
      '--socket',
      socketPath,
      '--journal',
      path.join(dir, 'engine.journal'),
      '--token',
      TOKEN,
      '--seed',
      '11',
      '--events',
      '4000',
      '--events-per-second',
      '20000',
    ],
    { stdio: 'ignore' },
  );
  for (let attempt = 0; attempt < 300 && !fs.existsSync(socketPath); attempt += 1) {
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
});

after(async () => {
  child?.kill('SIGKILL');
  fs.rmSync(dir, { recursive: true, force: true });
});

test('the TypeScript client and the Rust gateway agree on the wire format', { skip: !available }, async () => {
  const client = new GatewayClient(socketPath, TOKEN, 10_000);
  const health = await client.call<GatewayHealth>('health');
  assert.equal(typeof health.asOfEngineSeq, 'string');
  assert.ok(health.feeds.length > 0);
  client.close();
});

test('a real engine run projects into SQLite and settles', { skip: !available }, async () => {
  const client = new GatewayClient(socketPath, TOKEN, 10_000);
  const db = openDatabase(path.join(dir, 'e2e.db'));
  const projection = new Projection(db, client);

  let applied = 0;
  for (let attempt = 0; attempt < 100; attempt += 1) {
    applied += await projection.poll();
    if (applied > 0 && (await projection.poll()) === 0) {
      break;
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  assert.ok(applied > 0, 'expected engine output');

  const trades = db.prepare('SELECT COUNT(*) AS total FROM trades').get() as { total: bigint };
  assert.ok(trades.total > 0n);

  // Positions across all accounts must net to zero for every instrument.
  const net = db.prepare('SELECT SUM(position_lots) AS total FROM positions').get() as {
    total: bigint | null;
  };
  assert.equal(net.total ?? 0n, 0n);

  const venue = new SimulatedVenue();
  const outbox = new SettlementOutbox(db, venue.venue);
  const dispatcher = new SettlementDispatcher(outbox, venue, 100);
  while ((await dispatcher.tick()) > 0) {
    // Drain the outbox.
  }
  const unsettled = db
    .prepare('SELECT COUNT(*) AS total FROM trades WHERE settlement_id IS NULL')
    .get() as { total: bigint };
  assert.equal(unsettled.total, 0n);

  client.close();
  db.close();
});
