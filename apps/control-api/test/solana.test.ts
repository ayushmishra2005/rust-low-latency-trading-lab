import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test, { after, before } from 'node:test';

// The Anchor package is CommonJS, so it is imported as a default binding.
import anchor from '@coral-xyz/anchor';
import type { Idl } from '@coral-xyz/anchor';

const { AnchorProvider, BN, Program, Wallet } = anchor;
import { createMint, createAssociatedTokenAccount, mintTo, TOKEN_PROGRAM_ID } from '@solana/spl-token';
import { Connection, Keypair, PublicKey } from '@solana/web3.js';
import type Database from 'better-sqlite3';

import { openDatabase } from '../src/db.js';
import { SettlementDispatcher } from '../src/settlement/dispatcher.js';
import { SettlementOutbox } from '../src/settlement/outbox.js';
import { SolanaVenue, settlementManifestHash } from '../src/settlement/solana.js';
import { settlementIdFor } from '../src/settlement/manifest.js';

// Requires a local validator with the settlement program deployed:
//   solana-test-validator --bpf-program <program id> adapters/solana/target/deploy/settlement.so
const rpc = process.env.RLTL_SOLANA_RPC;
const idlPath = path.resolve(import.meta.dirname, '../../../adapters/solana/target/idl/settlement.json');
const enabled = rpc !== undefined && fs.existsSync(idlPath);

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'rltl-solana-'));
const authority = Keypair.generate();
const alice = Keypair.generate();
const bob = Keypair.generate();

let connection: Connection;
let idl: Idl;
let programId: PublicKey;
let exchange: PublicKey;
let venue: SolanaVenue;

interface Builder {
  accounts(accounts: Record<string, PublicKey>): Builder;
  signers(signers: Keypair[]): Builder;
  rpc(): Promise<string>;
}

async function airdrop(target: PublicKey, lamports: number): Promise<void> {
  const signature = await connection.requestAirdrop(target, lamports);
  const latest = await connection.getLatestBlockhash();
  await connection.confirmTransaction({ signature, ...latest }, 'confirmed');
}

function newOutbox(name: string, trades: number): { db: Database.Database; outbox: SettlementOutbox } {
  const db = openDatabase(path.join(dir, `${name}.db`));
  const insert = db.prepare(
    `INSERT INTO trades
       (trade_id, output_seq, engine_seq, engine_time_ns, instrument, maker_account,
        taker_account, aggressor, price_ticks, quantity_lots, settlement_id)
     VALUES (?, ?, ?, ?, 1, 1, 2, 'buy', 100, 5, NULL)`,
  );
  for (let index = 1; index <= trades; index += 1) {
    insert.run(BigInt(index), BigInt(index), BigInt(index), BigInt(index));
  }
  return { db, outbox: new SettlementOutbox(db, 'solana') };
}

before(async () => {
  if (!enabled) {
    return;
  }
  connection = new Connection(rpc!, 'confirmed');
  idl = JSON.parse(fs.readFileSync(idlPath, 'utf8')) as Idl;
  programId = new PublicKey(idl.address);

  await airdrop(authority.publicKey, 5_000_000_000);
  await airdrop(alice.publicKey, 2_000_000_000);
  await airdrop(bob.publicKey, 2_000_000_000);

  const provider = new AnchorProvider(connection, new Wallet(authority), {
    commitment: 'confirmed',
  });
  // The IDL is loaded at runtime, so instruction builders are typed narrowly here.
  const program = new Program(idl, provider);
  const methods = program.methods as unknown as Record<string, (...args: unknown[]) => Builder>;
  const mint = await createMint(connection, authority, authority.publicKey, null, 6);
  exchange = PublicKey.findProgramAddressSync(
    [Buffer.from('exchange'), authority.publicKey.toBuffer(), mint.toBuffer()],
    programId,
  )[0];

  await methods
    .initializeExchange!()
    .accounts({ authority: authority.publicKey, mint, tokenProgram: TOKEN_PROGRAM_ID })
    .rpc();

  for (const owner of [alice, bob]) {
    await methods
      .openCollateral!()
      .accounts({ owner: owner.publicKey, exchange })
      .signers([owner])
      .rpc();
    const tokens = await createAssociatedTokenAccount(connection, authority, mint, owner.publicKey);
    await mintTo(connection, authority, mint, tokens, authority, 10_000_000);
    await methods
      .depositCollateral!(new BN(5_000_000))
      .accounts({
        owner: owner.publicKey,
        exchange,
        mint,
        ownerTokens: tokens,
        tokenProgram: TOKEN_PROGRAM_ID,
      })
      .signers([owner])
      .rpc();
  }

  venue = new SolanaVenue({
    connection,
    idl,
    programId,
    authority,
    exchange,
    owners: new Map([
      [1, alice.publicKey],
      [2, bob.publicKey],
    ]),
  });
});

after(() => {
  fs.rmSync(dir, { recursive: true, force: true });
});

// Golden vector shared with the Rust unit test in the settlement program.
test('the canonical legs hash matches the on-chain golden vector', () => {
  const settlementId = Buffer.from(Array.from({ length: 16 }, (_value, index) => index));
  const legs = [
    {
      payer: new PublicKey(Buffer.alloc(32, 0x21)),
      payee: new PublicKey(Buffer.alloc(32, 0x22)),
      amount: 1_000n,
    },
    {
      payer: new PublicKey(Buffer.alloc(32, 0x22)),
      payee: new PublicKey(Buffer.alloc(32, 0x23)),
      amount: 250_000n,
    },
  ];

  assert.equal(
    settlementManifestHash(settlementId, new PublicKey(Buffer.alloc(32, 0x11)), legs),
    'd11c5555601792674da104cd7a9b35046858ec7d025f9e02cf1c2c83d9e9bd9a',
  );
});

test('a batch settles on chain and the outbox confirms it', { skip: !enabled }, async () => {
  const { db, outbox } = newOutbox('confirm', 3);
  const dispatcher = new SettlementDispatcher(outbox, venue, 10);
  await dispatcher.tick();

  const settlementId = settlementIdFor('solana', 1n, 3n);
  const row = outbox.get(settlementId)!;
  assert.equal(row.status, 'confirmed');
  assert.notEqual(row.receipt, null);
  db.close();
});

test('resubmitting the same identity resolves to the existing receipt', { skip: !enabled }, async () => {
  const { db, outbox } = newOutbox('duplicate', 3);
  const settlementId = settlementIdFor('solana', 1n, 3n);
  outbox.createBatch(10);
  const manifest = outbox.manifestFor(settlementId);
  const row = outbox.get(settlementId)!;

  // The first batch already applied this identity in the previous test.
  const outcome = await venue.submit(manifest, row.manifestHash);
  assert.equal(outcome.kind, 'alreadyApplied');
  db.close();
});

test('the same identity with a different manifest is refused', { skip: !enabled }, async () => {
  const { db, outbox } = newOutbox('conflict', 3);
  const settlementId = settlementIdFor('solana', 1n, 3n);
  outbox.createBatch(10);
  const manifest = outbox.manifestFor(settlementId);
  // Same identity, different economics, so the legs hash cannot match the receipt.
  const altered = {
    ...manifest,
    trades: manifest.trades.map((trade) => ({ ...trade, quantityLots: '99' })),
  };
  const outcome = await venue.submit(altered, 'ff'.repeat(32));
  assert.equal(outcome.kind, 'rejected');
  db.close();
});

test('an unreachable RPC endpoint reports unknown, never failed', { skip: !enabled }, async () => {
  const offline = new SolanaVenue({
    connection: new Connection('http://127.0.0.1:1', 'confirmed'),
    idl,
    programId,
    authority,
    exchange,
    owners: new Map([
      [1, alice.publicKey],
      [2, bob.publicKey],
    ]),
  });
  const { db, outbox } = newOutbox('offline', 2);
  const dispatcher = new SettlementDispatcher(outbox, offline, 10);
  await dispatcher.tick();

  const settlementId = settlementIdFor('solana', 1n, 2n);
  assert.equal(outbox.get(settlementId)!.status, 'unknown');

  // The trades are still recorded; settlement is only delayed.
  const trades = db.prepare('SELECT COUNT(*) AS total FROM trades').get() as { total: bigint };
  assert.equal(trades.total, 2n);

  // Once the endpoint works again the same identity settles.
  const online = new SettlementDispatcher(outbox, venue, 10);
  await online.tick();
  assert.equal(outbox.get(settlementId)!.status, 'confirmed');
  db.close();
});
