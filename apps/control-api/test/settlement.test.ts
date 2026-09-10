import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import type Database from 'better-sqlite3';
import { openDatabase } from '../src/db.js';
import { manifestHash, settlementIdFor } from '../src/settlement/manifest.js';
import { SettlementDispatcher } from '../src/settlement/dispatcher.js';
import { SettlementOutbox } from '../src/settlement/outbox.js';
import { SimulatedVenue } from '../src/settlement/simulated.js';
import { buildVenue } from '../src/settlement/venue.js';

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'rltl-settle-'));

interface Fixture {
  db: Database.Database;
  outbox: SettlementOutbox;
  venue: SimulatedVenue;
  dispatcher: SettlementDispatcher;
  file: string;
}

function fixture(name: string, trades = 3): Fixture {
  const file = path.join(dir, `${name}.db`);
  fs.rmSync(file, { force: true });
  const db = openDatabase(file);
  insertTrades(db, trades);
  const venue = new SimulatedVenue();
  const outbox = new SettlementOutbox(db, venue.venue);
  return { db, outbox, venue, dispatcher: new SettlementDispatcher(outbox, venue, 100), file };
}

function insertTrades(db: Database.Database, count: number, from = 1): void {
  const insert = db.prepare(
    `INSERT INTO trades
       (trade_id, output_seq, engine_seq, engine_time_ns, instrument, maker_account,
        taker_account, aggressor, price_ticks, quantity_lots, settlement_id)
     VALUES (?, ?, ?, ?, 1, 1, 2, 'buy', 10000, 5, NULL)`,
  );
  for (let index = 0; index < count; index += 1) {
    const id = BigInt(from + index);
    insert.run(id, id, id, id);
  }
}

test('the venue factory defaults to the simulated venue and reports missing config', () => {
  assert.equal(buildVenue({}).venue, 'simulated');
  assert.throws(() => buildVenue({ RLTL_SETTLEMENT_VENUE: 'canton' }), /RLTL_CANTON_JSON_API/);
  assert.throws(() => buildVenue({ RLTL_SETTLEMENT_VENUE: 'nope' }), /unknown settlement venue/);
});

test('the settlement identity and manifest hash are stable across rebuilds', () => {
  const { outbox } = fixture('stable');
  const row = outbox.createBatch(10);
  assert.notEqual(row, null);
  assert.equal(row!.settlementId, settlementIdFor('simulated', 1n, 3n));
  assert.equal(manifestHash(outbox.manifestFor(row!.settlementId)), row!.manifestHash);

  // Rebuilding produces byte-identical bytes, so a retry cannot drift.
  assert.deepEqual(outbox.manifestBytes(row!.settlementId), outbox.manifestBytes(row!.settlementId));
});

test('a batch claims trades exactly once', () => {
  const { outbox, db } = fixture('claim');
  const first = outbox.createBatch(2);
  const second = outbox.createBatch(2);
  assert.notEqual(first!.settlementId, second!.settlementId);
  assert.equal(outbox.createBatch(2), null);

  const unsettled = db
    .prepare('SELECT COUNT(*) AS total FROM trades WHERE settlement_id IS NULL')
    .get() as { total: bigint };
  assert.equal(unsettled.total, 0n);
});

test('a confirmed settlement is terminal', async () => {
  const { outbox, dispatcher } = fixture('confirm');
  await dispatcher.tick();
  const rows = outbox.pending(10);
  assert.equal(rows.length, 0);

  const settlementId = settlementIdFor('simulated', 1n, 3n);
  const row = outbox.get(settlementId)!;
  assert.equal(row.status, 'confirmed');
  assert.notEqual(row.receipt, null);

  // A late timeout must not undo a confirmation.
  outbox.recordUnknown(settlementId, 'late timeout');
  assert.equal(outbox.get(settlementId)!.status, 'confirmed');
});

test('a timeout after the venue applied the batch resolves to the original receipt', async () => {
  const { outbox, venue, dispatcher } = fixture('timeout');
  venue.faults.timeoutAfterApply = true;
  await dispatcher.tick();

  const settlementId = settlementIdFor('simulated', 1n, 3n);
  assert.equal(outbox.get(settlementId)!.status, 'unknown');

  venue.faults.timeoutAfterApply = false;
  await dispatcher.tick();

  const row = outbox.get(settlementId)!;
  assert.equal(row.status, 'confirmed');
  assert.equal(dispatcher.counters.alreadyApplied, 1n);
  // The same business identity was reused, so nothing settled twice.
  assert.equal(dispatcher.counters.submitted, 1n);
});

test('an unavailable venue leaves the settlement retryable and the trades intact', async () => {
  const { outbox, venue, dispatcher, db } = fixture('unavailable');
  venue.faults.unavailable = true;
  await dispatcher.tick();
  await dispatcher.tick();

  const settlementId = settlementIdFor('simulated', 1n, 3n);
  assert.equal(outbox.get(settlementId)!.status, 'unknown');
  const trades = db.prepare('SELECT COUNT(*) AS total FROM trades').get() as { total: bigint };
  assert.equal(trades.total, 3n, 'an executed trade stays executed while settlement is delayed');

  venue.faults.unavailable = false;
  await dispatcher.tick();
  assert.equal(outbox.get(settlementId)!.status, 'confirmed');
});

test('a rejected settlement is failed and not retried forever', async () => {
  const { outbox, venue, dispatcher } = fixture('rejected');
  venue.faults.reject = true;
  await dispatcher.tick();
  const settlementId = settlementIdFor('simulated', 1n, 3n);
  assert.equal(outbox.get(settlementId)!.status, 'failed');
  assert.equal(outbox.pending(10).length, 0);
});

test('the same settlement id with a different manifest is refused', async () => {
  const { outbox, db, dispatcher } = fixture('conflict');
  const row = outbox.createBatch(10)!;

  // Rewrite a claimed trade so the stored manifest no longer matches.
  db.prepare('UPDATE trades SET quantity_lots = 99 WHERE trade_id = 1').run();
  assert.throws(() => outbox.manifestFor(row.settlementId), /different manifest/);

  await dispatcher.tick();
  assert.equal(outbox.get(row.settlementId)!.status, 'failed');
  assert.equal(dispatcher.counters.manifestConflicts, 1n);
});

test('the outbox survives a database restart mid-flight', async () => {
  const { venue, dispatcher, file, db } = fixture('recovery');
  venue.faults.unavailable = true;
  await dispatcher.tick();
  db.close();

  const reopened = openDatabase(file);
  const resumedOutbox = new SettlementOutbox(reopened, venue.venue);
  const resumed = new SettlementDispatcher(resumedOutbox, venue, 100);
  venue.faults.unavailable = false;
  await resumed.tick();

  const settlementId = settlementIdFor('simulated', 1n, 3n);
  assert.equal(resumedOutbox.get(settlementId)!.status, 'confirmed');
  const settlements = reopened.prepare('SELECT COUNT(*) AS total FROM settlements').get() as {
    total: bigint;
  };
  assert.equal(settlements.total, 1n, 'recovery must not create a second settlement identity');
  reopened.close();
});
