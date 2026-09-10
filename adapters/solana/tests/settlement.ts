import * as anchor from '@coral-xyz/anchor';
import { Program, BN } from '@coral-xyz/anchor';
import {
  createMint,
  createAssociatedTokenAccount,
  mintTo,
  getAccount,
  TOKEN_PROGRAM_ID,
} from '@solana/spl-token';
import { Keypair, PublicKey, SystemProgram } from '@solana/web3.js';
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
    const manifestHash = Buffer.alloc(32, 9);
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

    // Collateral is conserved: the vault balance did not change.
    const vaultAccount = await getAccount(provider.connection, vault);
    assert.equal(vaultAccount.amount.toString(), '700000');
  });

  it('refuses to apply the same settlement identity twice', async () => {
    const settlementId = Buffer.alloc(16, 7);
    const manifestHash = Buffer.alloc(32, 9);
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
    const otherManifest = Buffer.alloc(32, 1);
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

    try {
      await program.methods
        .settleBatch(Array.from(Buffer.alloc(16, 4)), Array.from(Buffer.alloc(32, 4)), [
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
});
