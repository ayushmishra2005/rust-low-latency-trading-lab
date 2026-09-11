import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test, { after, before } from 'node:test';

import type Database from 'better-sqlite3';
import { openDatabase } from '../src/db.js';
import { CantonVenue } from '../src/settlement/canton.js';
import { SettlementDispatcher } from '../src/settlement/dispatcher.js';
import { SettlementOutbox } from '../src/settlement/outbox.js';
import { settlementIdFor } from '../src/settlement/manifest.js';

// Requires a local Daml sandbox with the settlement DAR loaded:
//   cd adapters/canton && daml start --start-navigator=no --json-api-port 7575
const baseUrl = process.env.RLTL_CANTON_JSON_API;
const packageId = process.env.RLTL_CANTON_PACKAGE_ID;
const enabled = baseUrl !== undefined && packageId !== undefined;

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'rltl-canton-'));
let operator: string;
let alice: string;
let bob: string;
let venue: CantonVenue;

/** Sandbox accepts unsigned tokens; a real deployment would not. */
function token(parties: string[]): string {
  const encode = (value: object): string =>
    Buffer.from(JSON.stringify(value)).toString('base64url');
  const header = encode({ alg: 'none', typ: 'JWT' });
  const payload = encode({
    'https://daml.com/ledger-api': {
      ledgerId: 'sandbox',
      applicationId: 'rltl-control-plane',
      actAs: parties,
      readAs: parties,
    },
  });
  return `${header}.${payload}.`;
}

async function call<T>(endpoint: string, body: unknown, parties: string[]): Promise<T> {
  const response = await fetch(`${baseUrl}${endpoint}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', authorization: `Bearer ${token(parties)}` },
    body: JSON.stringify(body),
  });
  const payload = (await response.json()) as { result?: T; errors?: string[] };
  if (payload.result === undefined) {
    throw new Error(payload.errors?.join('; ') ?? `HTTP ${response.status}`);
  }
  return payload.result;
}

async function allocate(hint: string): Promise<string> {
  const party = await call<{ identifier: string }>(
    '/v1/parties/allocate',
    { identifierHint: hint },
    ['unused'],
  );
  return party.identifier;
}

function newOutbox(name: string, trades: number): { db: Database.Database; outbox: SettlementOutbox } {
  const db = openDatabase(path.join(dir, `${name}.db`));
  const insert = db.prepare(
    `INSERT INTO trades
       (trade_id, output_seq, engine_seq, engine_time_ns, instrument, maker_account,
        taker_account, aggressor, price_ticks, quantity_lots, settlement_id)
     VALUES (?, ?, ?, ?, 1, 1, 2, 'buy', 10, 5, NULL)`,
  );
  for (let index = 1; index <= trades; index += 1) {
    insert.run(BigInt(index), BigInt(index), BigInt(index), BigInt(index));
  }
  return { db, outbox: new SettlementOutbox(db, 'canton') };
}

before(async () => {
  if (!enabled) {
    return;
  }
  const suffix = Date.now().toString(36);
  operator = await allocate(`operator_${suffix}`);
  alice = await allocate(`alice_${suffix}`);
  bob = await allocate(`bob_${suffix}`);

  for (const owner of [alice, bob]) {
    await call(
      '/v1/create',
      {
        templateId: `${packageId}:Settlement:Holding`,
        payload: { operator, owner, amount: '1000' },
      },
      [operator],
    );
  }

  venue = new CantonVenue({
    baseUrl: baseUrl!,
    token: token([operator, alice, bob]),
    operator,
    packageId: packageId!,
    parties: new Map([
      [1, alice],
      [2, bob],
    ]),
  });
});

after(() => {
  fs.rmSync(dir, { recursive: true, force: true });
});

test('a batch settles on the ledger and confirms in the outbox', { skip: !enabled }, async () => {
  const { db, outbox } = newOutbox('confirm', 3);
  const dispatcher = new SettlementDispatcher(outbox, venue, 10);
  await dispatcher.tick();

  const settlementId = settlementIdFor('canton', 1n, 3n);
  const row = outbox.get(settlementId)!;
  assert.equal(row.status, 'confirmed', row.lastError ?? '');

  // The taker (account 2, bob) bought, so bob paid alice 3 * 10 * 5.
  const holdings = await call<{ payload: { owner: string; amount: string } }[]>(
    '/v1/query',
    { templateIds: [`${packageId}:Settlement:Holding`] },
    [operator],
  );
  const aliceHolding = holdings.find((entry) => entry.payload.owner === alice);
  assert.equal(aliceHolding?.payload.amount, '1150');
  db.close();
});

test('resubmitting the same identity resolves to the recorded receipt', { skip: !enabled }, async () => {
  const { db, outbox } = newOutbox('duplicate', 3);
  outbox.createBatch(10);
  const settlementId = settlementIdFor('canton', 1n, 3n);
  const manifest = outbox.manifestFor(settlementId);
  const row = outbox.get(settlementId)!;

  const outcome = await venue.submit(manifest, row.manifestHash);
  assert.equal(outcome.kind, 'alreadyApplied');
  db.close();
});

test('the same identity with different economics is refused', { skip: !enabled }, async () => {
  const { db, outbox } = newOutbox('conflict', 3);
  outbox.createBatch(10);
  const settlementId = settlementIdFor('canton', 1n, 3n);
  const manifest = outbox.manifestFor(settlementId);
  // Same settlement identity, larger amount than the ledger settled.
  manifest.trades[0]!.quantityLots = '50';

  const outcome = await venue.submit(manifest, 'ab'.repeat(32));
  assert.equal(outcome.kind, 'rejected');
  db.close();
});

test('an unreachable participant reports unknown, never failed', { skip: !enabled }, async () => {
  const offline = new CantonVenue({
    baseUrl: 'http://127.0.0.1:1',
    token: token([operator]),
    operator,
    packageId: packageId!,
    parties: new Map([
      [1, alice],
      [2, bob],
    ]),
    timeoutMs: 1_000,
  });
  const { db, outbox } = newOutbox('offline', 2);
  const dispatcher = new SettlementDispatcher(outbox, offline, 10);
  await dispatcher.tick();

  const settlementId = settlementIdFor('canton', 1n, 2n);
  assert.equal(outbox.get(settlementId)!.status, 'unknown');
  const trades = db.prepare('SELECT COUNT(*) AS total FROM trades').get() as { total: bigint };
  assert.equal(trades.total, 2n, 'an executed trade stays executed while settlement is delayed');

  const online = new SettlementDispatcher(outbox, venue, 10);
  await online.tick();
  assert.equal(outbox.get(settlementId)!.status, 'confirmed');
  db.close();
});
