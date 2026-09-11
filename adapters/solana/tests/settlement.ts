// @coral-xyz/anchor is CommonJS. Node 22 ESM cannot named-import BN
// (it is a getter, not a static export). ts-mocha compiles this file
// to CommonJS, where a default import has no `.default`. Unwrap both.
import * as anchorImport from '@coral-xyz/anchor';
import type { Program } from '@coral-xyz/anchor';

type AnchorApi = typeof anchorImport;
const loaded = anchorImport as AnchorApi & { default?: AnchorApi };
const anchor: AnchorApi =
  typeof loaded.BN === 'function' ? loaded : (loaded.default as AnchorApi);
const { BN } = anchor;
import {
  createMint,
  createAssociatedTokenAccount,
  mintTo,
  getAccount,
  TOKEN_PROGRAM_ID,
} from '@solana/spl-token';
import { Keypair, PublicKey, SystemProgram } from '@solana/web3.js';
import { createHash } from 'node:crypto';
import { assert } from 'chai';
import type { Settlement } from '../target/types/settlement';

describe('settlement', () => {
  const provider = anchor.AnchorProvider.env();
  anchor.setProvider(provider);
  const program = anchor.workspace.settlement as Program<Settlement>;
  const authority = provider.wallet as anchor.Wallet;

  let mint: PublicKey;
  let exchange: PublicKey;
  let vault: PublicKey;
  const alice = Keypair.generate();
  const bob = Keypair.generate();
  let aliceCollateral: PublicKey;
  let bobCollateral: PublicKey;
  let aliceTokens: PublicKey;
  let bobTokens: PublicKey;

  const collateralFor = (owner: PublicKey): PublicKey =>
    PublicKey.findProgramAddressSync(
      [Buffer.from('collateral'), exchange.toBuffer(), owner.toBuffer()],
      program.programId,
    )[0];

  /**
   * Same canonical bytes as the program: domain tag (16), settlement id (16),
   * exchange key (32), leg count (u16 LE), then payer key (32), payee key (32)
   * and amount (u64 LE) for each leg in order.
   */
  const manifestHashFor = (
    settlementId: Buffer,
    legs: { payer: PublicKey; payee: PublicKey; amount: bigint }[],
  ): Buffer => {
    const count = Buffer.alloc(2);
    count.writeUInt16LE(legs.length);
    const parts = [
      Buffer.from('RLTL-SETTLE-V1  ', 'ascii'),
      settlementId,
      exchange.toBuffer(),
      count,
    ];
    for (const leg of legs) {
      const amount = Buffer.alloc(8);
      amount.writeBigUInt64LE(leg.amount);
      parts.push(leg.payer.toBuffer(), leg.payee.toBuffer(), amount);
    }
    return createHash('sha256').update(Buffer.concat(parts)).digest();
  };

  const receiptFor = (settlementId: Buffer): PublicKey =>
    PublicKey.findProgramAddressSync(
      [Buffer.from('receipt'), exchange.toBuffer(), settlementId],
      program.programId,
    )[0];

  const fund = async (keypair: Keypair): Promise<void> => {
    const signature = await provider.connection.requestAirdrop(keypair.publicKey, 2_000_000_000);
    const latest = await provider.connection.getLatestBlockhash();
    await provider.connection.confirmTransaction({ signature, ...latest });
  };

  before(async () => {
    await fund(alice);
    await fund(bob);

    mint = await createMint(provider.connection, authority.payer, authority.publicKey, null, 6);
    exchange = PublicKey.findProgramAddressSync(
      [Buffer.from('exchange'), authority.publicKey.toBuffer(), mint.toBuffer()],
      program.programId,
    )[0];
    vault = PublicKey.findProgramAddressSync(
      [Buffer.from('vault'), exchange.toBuffer()],
      program.programId,
    )[0];

    await program.methods
      .initializeExchange()
      .accounts({ authority: authority.publicKey, mint, tokenProgram: TOKEN_PROGRAM_ID })
      .rpc();

    for (const owner of [alice, bob]) {
      await program.methods
        .openCollateral()
        .accounts({ owner: owner.publicKey, exchange })
        .signers([owner])
        .rpc();
    }
    aliceCollateral = collateralFor(alice.publicKey);
    bobCollateral = collateralFor(bob.publicKey);

    aliceTokens = await createAssociatedTokenAccount(
      provider.connection,
      authority.payer,
      mint,
      alice.publicKey,
    );
    bobTokens = await createAssociatedTokenAccount(
      provider.connection,
      authority.payer,
      mint,
      bob.publicKey,
    );
    await mintTo(provider.connection, authority.payer, mint, aliceTokens, authority.payer, 1_000_000);
    await mintTo(provider.connection, authority.payer, mint, bobTokens, authority.payer, 1_000_000);
  });

  const deposit = async (owner: Keypair, tokens: PublicKey, amount: number): Promise<void> => {
    await program.methods
      .depositCollateral(new BN(amount))
      .accounts({
        owner: owner.publicKey,
        exchange,
        mint,
        ownerTokens: tokens,
        tokenProgram: TOKEN_PROGRAM_ID,
      })
      .signers([owner])
      .rpc();
  };

  it('moves tokens into the vault on deposit', async () => {
    await deposit(alice, aliceTokens, 500_000);
    await deposit(bob, bobTokens, 200_000);

    const vaultAccount = await getAccount(provider.connection, vault);
    assert.equal(vaultAccount.amount.toString(), '700000');
    const state = await program.account.collateral.fetch(aliceCollateral);
    assert.equal(state.balance.toString(), '500000');
  });

  it('rejects a zero deposit and an oversized withdrawal', async () => {
    try {
      await deposit(alice, aliceTokens, 0);
      assert.fail('zero deposit should be rejected');
    } catch (error) {
      assert.match(String(error), /amount must be greater than zero/);
    }

    try {
      await program.methods
        .withdrawCollateral(new BN(999_999_999))
        .accounts({
          owner: alice.publicKey,
          exchange,
          mint,
          ownerTokens: aliceTokens,
          tokenProgram: TOKEN_PROGRAM_ID,
        })
        .signers([alice])
        .rpc();
      assert.fail('withdrawal above the balance should be rejected');
    } catch (error) {
      assert.match(String(error), /insufficient collateral/);
    }
  });

  it('applies a settlement batch and records a receipt', async () => {
    const settlementId = Buffer.alloc(16, 7);
    const manifestHash = manifestHashFor(settlementId, [
      { payer: aliceCollateral, payee: bobCollateral, amount: 120_000n },
    ]);
    await program.methods
      .settleBatch(Array.from(settlementId), Array.from(manifestHash), [
        { payer: 0, payee: 1, amount: new BN(120_000) },
      ])
      .accounts({ authority: authority.publicKey, exchange })
      .remainingAccounts([
        { pubkey: aliceCollateral, isWritable: true, isSigner: false },
        { pubkey: bobCollateral, isWritable: true, isSigner: false },
      ])
      .rpc();

    const aliceState = await program.account.collateral.fetch(aliceCollateral);
    const bobState = await program.account.collateral.fetch(bobCollateral);
    assert.equal(aliceState.balance.toString(), '380000');
    assert.equal(bobState.balance.toString(), '320000');

    const receipt = await program.account.receipt.fetch(receiptFor(settlementId));
    assert.deepEqual(Buffer.from(receipt.manifestHash), manifestHash);
    assert.equal(receipt.legCount, 1);

    // Collateral is conserved: the vault holds exactly the represented balances.
    const vaultAccount = await getAccount(provider.connection, vault);
    const represented = aliceState.balance.add(bobState.balance);
    assert.equal(vaultAccount.amount.toString(), represented.toString());
    const exchangeState = await program.account.exchange.fetch(exchange);
    assert.equal(exchangeState.totalCollateral.toString(), represented.toString());
  });

  it('rejects a batch that supplies the same collateral account twice', async () => {
    const before = {
      alice: (await program.account.collateral.fetch(aliceCollateral)).balance,
      bob: (await program.account.collateral.fetch(bobCollateral)).balance,
      vault: (await getAccount(provider.connection, vault)).amount,
      total: (await program.account.exchange.fetch(exchange)).totalCollateral,
    };

    try {
      await program.methods
        .settleBatch(Array.from(Buffer.alloc(16, 8)), Array.from(Buffer.alloc(32, 8)), [
          { payer: 0, payee: 1, amount: new BN(50_000) },
          { payer: 2, payee: 1, amount: new BN(50_000) },
        ])
        .accounts({ authority: authority.publicKey, exchange })
        .remainingAccounts([
          { pubkey: aliceCollateral, isWritable: true, isSigner: false },
          { pubkey: bobCollateral, isWritable: true, isSigner: false },
          // The same account again, so one leg would overwrite the other.
          { pubkey: aliceCollateral, isWritable: true, isSigner: false },
        ])
        .rpc();
      assert.fail('a duplicated collateral account must be rejected');
    } catch (error) {
      assert.match(String(error), /the same collateral account was supplied twice/);
    }

    const after = {
      alice: (await program.account.collateral.fetch(aliceCollateral)).balance,
      bob: (await program.account.collateral.fetch(bobCollateral)).balance,
      vault: (await getAccount(provider.connection, vault)).amount,
      total: (await program.account.exchange.fetch(exchange)).totalCollateral,
    };
    assert.equal(after.alice.toString(), before.alice.toString());
    assert.equal(after.bob.toString(), before.bob.toString());
    assert.equal(after.vault.toString(), before.vault.toString());
    assert.equal(after.total.toString(), before.total.toString());
  });

  it('rejects a batch whose collateral account is not writable', async () => {
    try {
      await program.methods
        .settleBatch(Array.from(Buffer.alloc(16, 11)), Array.from(Buffer.alloc(32, 11)), [
          { payer: 0, payee: 1, amount: new BN(1) },
        ])
        .accounts({ authority: authority.publicKey, exchange })
        .remainingAccounts([
          { pubkey: aliceCollateral, isWritable: false, isSigner: false },
          { pubkey: bobCollateral, isWritable: true, isSigner: false },
        ])
        .rpc();
      assert.fail('a read-only collateral account must be rejected');
    } catch (error) {
      assert.match(String(error), /collateral account must be writable/);
    }
  });

  it('refuses to apply the same settlement identity twice', async () => {
    const settlementId = Buffer.alloc(16, 7);
    const manifestHash = manifestHashFor(settlementId, [
      { payer: aliceCollateral, payee: bobCollateral, amount: 120_000n },
    ]);
    try {
      await program.methods
        .settleBatch(Array.from(settlementId), Array.from(manifestHash), [
          { payer: 0, payee: 1, amount: new BN(120_000) },
        ])
        .accounts({ authority: authority.publicKey, exchange })
        .remainingAccounts([
          { pubkey: aliceCollateral, isWritable: true, isSigner: false },
          { pubkey: bobCollateral, isWritable: true, isSigner: false },
        ])
        .rpc();
      assert.fail('a duplicate settlement identity must not apply twice');
    } catch (error) {
      assert.match(String(error), /already in use|custom program error/);
    }

    // The receipt still describes the first application.
    const receipt = await program.account.receipt.fetch(receiptFor(settlementId));
    assert.deepEqual(Buffer.from(receipt.manifestHash), manifestHash);
    const aliceState = await program.account.collateral.fetch(aliceCollateral);
    assert.equal(aliceState.balance.toString(), '380000');
  });

  it('refuses the same settlement identity with a different manifest', async () => {
    const settlementId = Buffer.alloc(16, 7);
    // Different economics, correctly hashed, so only the identity can reject it.
    const otherManifest = manifestHashFor(settlementId, [
      { payer: aliceCollateral, payee: bobCollateral, amount: 1n },
    ]);
    try {
      await program.methods
        .settleBatch(Array.from(settlementId), Array.from(otherManifest), [
          { payer: 0, payee: 1, amount: new BN(1) },
        ])
        .accounts({ authority: authority.publicKey, exchange })
        .remainingAccounts([
          { pubkey: aliceCollateral, isWritable: true, isSigner: false },
          { pubkey: bobCollateral, isWritable: true, isSigner: false },
        ])
        .rpc();
      assert.fail('a conflicting manifest must not be applied');
    } catch (error) {
      assert.match(String(error), /already in use|custom program error/);
    }

    const receipt = await program.account.receipt.fetch(receiptFor(settlementId));
    assert.notDeepEqual(Buffer.from(receipt.manifestHash), otherManifest);
  });

  it('rejects a batch signed by the wrong authority', async () => {
    const intruder = Keypair.generate();
    await fund(intruder);
    try {
      await program.methods
        .settleBatch(Array.from(Buffer.alloc(16, 3)), Array.from(Buffer.alloc(32, 3)), [
          { payer: 0, payee: 1, amount: new BN(10) },
        ])
        .accounts({ authority: intruder.publicKey, exchange })
        .remainingAccounts([
          { pubkey: aliceCollateral, isWritable: true, isSigner: false },
          { pubkey: bobCollateral, isWritable: true, isSigner: false },
        ])
        .signers([intruder])
        .rpc();
      assert.fail('only the exchange authority may settle');
    } catch (error) {
      assert.match(String(error), /has_one|ConstraintHasOne|A has one constraint was violated/);
    }
  });

  it('a frozen account can neither withdraw nor settle', async () => {
    await program.methods
      .freezeAccount(true)
      .accounts({ authority: authority.publicKey, exchange, collateral: bobCollateral })
      .rpc();

    const frozenId = Buffer.alloc(16, 4);
    const frozenHash = manifestHashFor(frozenId, [
      { payer: aliceCollateral, payee: bobCollateral, amount: 10n },
    ]);
    try {
      await program.methods
        .settleBatch(Array.from(frozenId), Array.from(frozenHash), [
          { payer: 0, payee: 1, amount: new BN(10) },
        ])
        .accounts({ authority: authority.publicKey, exchange })
        .remainingAccounts([
          { pubkey: aliceCollateral, isWritable: true, isSigner: false },
          { pubkey: bobCollateral, isWritable: true, isSigner: false },
        ])
        .rpc();
      assert.fail('a frozen account must not settle');
    } catch (error) {
      assert.match(String(error), /collateral account is frozen/);
    }

    try {
      await program.methods
        .withdrawCollateral(new BN(1))
        .accounts({
          owner: bob.publicKey,
          exchange,
          mint,
          ownerTokens: bobTokens,
          tokenProgram: TOKEN_PROGRAM_ID,
        })
        .signers([bob])
        .rpc();
      assert.fail('a frozen account must not withdraw');
    } catch (error) {
      assert.match(String(error), /collateral account is frozen/);
    }

    await program.methods
      .freezeAccount(false)
      .accounts({ authority: authority.publicKey, exchange, collateral: bobCollateral })
      .rpc();
  });

  it('returns collateral on withdrawal', async () => {
    const before = await getAccount(provider.connection, bobTokens);
    await program.methods
      .withdrawCollateral(new BN(100_000))
      .accounts({
        owner: bob.publicKey,
        exchange,
        mint,
        ownerTokens: bobTokens,
        tokenProgram: TOKEN_PROGRAM_ID,
      })
      .signers([bob])
      .rpc();
    const after = await getAccount(provider.connection, bobTokens);
    assert.equal(after.amount - before.amount, 100_000n);

    const exchangeState = await program.account.exchange.fetch(exchange);
    assert.equal(exchangeState.totalCollateral.toString(), '600000');
    assert.isTrue(exchangeState.settlements.gtn(0));
  });

  it('rejects a collateral account from another exchange', async () => {
    const otherMint = await createMint(
      provider.connection,
      authority.payer,
      authority.publicKey,
      null,
      6,
    );
    const otherExchange = PublicKey.findProgramAddressSync(
      [Buffer.from('exchange'), authority.publicKey.toBuffer(), otherMint.toBuffer()],
      program.programId,
    )[0];
    await program.methods
      .initializeExchange()
      .accounts({ authority: authority.publicKey, mint: otherMint, tokenProgram: TOKEN_PROGRAM_ID })
      .rpc();
    await program.methods
      .openCollateral()
      .accounts({ owner: alice.publicKey, exchange: otherExchange })
      .signers([alice])
      .rpc();
    const foreign = PublicKey.findProgramAddressSync(
      [Buffer.from('collateral'), otherExchange.toBuffer(), alice.publicKey.toBuffer()],
      program.programId,
    )[0];

    try {
      await program.methods
        .settleBatch(Array.from(Buffer.alloc(16, 5)), Array.from(Buffer.alloc(32, 5)), [
          { payer: 0, payee: 1, amount: new BN(10) },
        ])
        .accounts({ authority: authority.publicKey, exchange })
        .remainingAccounts([
          { pubkey: foreign, isWritable: true, isSigner: false },
          { pubkey: bobCollateral, isWritable: true, isSigner: false },
        ])
        .rpc();
      assert.fail('collateral from another exchange must be rejected');
    } catch (error) {
      assert.match(String(error), /belongs to another exchange/);
    }
    assert.ok(SystemProgram.programId);
  });

  it('settles a batch whose manifest hash is computed from the legs', async () => {
    const settlementId = Buffer.alloc(16, 20);
    const manifestHash = manifestHashFor(settlementId, [
      { payer: aliceCollateral, payee: bobCollateral, amount: 25_000n },
    ]);
    const before = await program.account.collateral.fetch(aliceCollateral);

    await program.methods
      .settleBatch(Array.from(settlementId), Array.from(manifestHash), [
        { payer: 0, payee: 1, amount: new BN(25_000) },
      ])
      .accounts({ authority: authority.publicKey, exchange })
      .remainingAccounts([
        { pubkey: aliceCollateral, isWritable: true, isSigner: false },
        { pubkey: bobCollateral, isWritable: true, isSigner: false },
      ])
      .rpc();

    const aliceState = await program.account.collateral.fetch(aliceCollateral);
    assert.equal(aliceState.balance.toString(), before.balance.subn(25_000).toString());
    const receipt = await program.account.receipt.fetch(receiptFor(settlementId));
    assert.deepEqual(Buffer.from(receipt.manifestHash), manifestHash);
  });

  it('rejects a changed amount carrying the previous manifest hash', async () => {
    const settlementId = Buffer.alloc(16, 21);
    const manifestHash = manifestHashFor(settlementId, [
      { payer: aliceCollateral, payee: bobCollateral, amount: 25_000n },
    ]);

    try {
      await program.methods
        .settleBatch(Array.from(settlementId), Array.from(manifestHash), [
          { payer: 0, payee: 1, amount: new BN(30_000) },
        ])
        .accounts({ authority: authority.publicKey, exchange })
        .remainingAccounts([
          { pubkey: aliceCollateral, isWritable: true, isSigner: false },
          { pubkey: bobCollateral, isWritable: true, isSigner: false },
        ])
        .rpc();
      assert.fail('a changed amount must not settle under the old hash');
    } catch (error) {
      assert.match(String(error), /manifest hash does not match the supplied legs/);
    }
  });

  it('rejects changed payer and payee accounts carrying the previous manifest hash', async () => {
    const settlementId = Buffer.alloc(16, 22);
    const manifestHash = manifestHashFor(settlementId, [
      { payer: aliceCollateral, payee: bobCollateral, amount: 25_000n },
    ]);

    try {
      await program.methods
        // The amount is unchanged, but the payer and payee are swapped.
        .settleBatch(Array.from(settlementId), Array.from(manifestHash), [
          { payer: 1, payee: 0, amount: new BN(25_000) },
        ])
        .accounts({ authority: authority.publicKey, exchange })
        .remainingAccounts([
          { pubkey: aliceCollateral, isWritable: true, isSigner: false },
          { pubkey: bobCollateral, isWritable: true, isSigner: false },
        ])
        .rpc();
      assert.fail('swapped accounts must not settle under the old hash');
    } catch (error) {
      assert.match(String(error), /manifest hash does not match the supplied legs/);
    }
  });

  it('rejects reordered legs carrying the previous manifest hash', async () => {
    const settlementId = Buffer.alloc(16, 23);
    const manifestHash = manifestHashFor(settlementId, [
      { payer: aliceCollateral, payee: bobCollateral, amount: 4_000n },
      { payer: bobCollateral, payee: aliceCollateral, amount: 1_000n },
    ]);

    try {
      await program.methods
        .settleBatch(Array.from(settlementId), Array.from(manifestHash), [
          { payer: 1, payee: 0, amount: new BN(1_000) },
          { payer: 0, payee: 1, amount: new BN(4_000) },
        ])
        .accounts({ authority: authority.publicKey, exchange })
        .remainingAccounts([
          { pubkey: aliceCollateral, isWritable: true, isSigner: false },
          { pubkey: bobCollateral, isWritable: true, isSigner: false },
        ])
        .rpc();
      assert.fail('leg order is part of the manifest identity');
    } catch (error) {
      assert.match(String(error), /manifest hash does not match the supplied legs/);
    }
  });

  it('refuses a settled identity even with correctly hashed different economics', async () => {
    const settlementId = Buffer.alloc(16, 20);
    const manifestHash = manifestHashFor(settlementId, [
      { payer: aliceCollateral, payee: bobCollateral, amount: 7_000n },
    ]);

    try {
      await program.methods
        .settleBatch(Array.from(settlementId), Array.from(manifestHash), [
          { payer: 0, payee: 1, amount: new BN(7_000) },
        ])
        .accounts({ authority: authority.publicKey, exchange })
        .remainingAccounts([
          { pubkey: aliceCollateral, isWritable: true, isSigner: false },
          { pubkey: bobCollateral, isWritable: true, isSigner: false },
        ])
        .rpc();
      assert.fail('a settled identity must not be reused');
    } catch (error) {
      assert.match(String(error), /already in use|custom program error/);
    }

    // The receipt still describes the economics that were applied first.
    const receipt = await program.account.receipt.fetch(receiptFor(settlementId));
    assert.notDeepEqual(Buffer.from(receipt.manifestHash), manifestHash);
  });

  it('conserves collateral across a hashed settlement', async () => {
    const settlementId = Buffer.alloc(16, 24);
    const manifestHash = manifestHashFor(settlementId, [
      { payer: bobCollateral, payee: aliceCollateral, amount: 3_000n },
    ]);

    await program.methods
      .settleBatch(Array.from(settlementId), Array.from(manifestHash), [
        { payer: 0, payee: 1, amount: new BN(3_000) },
      ])
      .accounts({ authority: authority.publicKey, exchange })
      .remainingAccounts([
        { pubkey: bobCollateral, isWritable: true, isSigner: false },
        { pubkey: aliceCollateral, isWritable: true, isSigner: false },
      ])
      .rpc();

    const aliceState = await program.account.collateral.fetch(aliceCollateral);
    const bobState = await program.account.collateral.fetch(bobCollateral);
    const represented = aliceState.balance.add(bobState.balance);
    const vaultAccount = await getAccount(provider.connection, vault);
    assert.equal(vaultAccount.amount.toString(), represented.toString());
    const exchangeState = await program.account.exchange.fetch(exchange);
    assert.equal(exchangeState.totalCollateral.toString(), represented.toString());
  });
});
