//! Collateral and batch settlement for the trading lab.
//!
//! This program is a settlement boundary. It never matches orders and is never
//! called from the trading hot path. A batch that was already applied is
//! recognised by its receipt, so a retry after a timeout cannot settle twice.

use anchor_lang::prelude::*;
use anchor_lang::solana_program::hash::hash;
use anchor_spl::token::{self, Mint, Token, TokenAccount, Transfer};

declare_id!("HYAoYYczVZE7scJbDP2EGA8FbLYspcxrw9BRLQ4i6ux4");

pub const MAX_LEGS: usize = 32;

/// Domain tag: the 16 ASCII bytes `RLTL-SETTLE-V1  ` (two trailing spaces).
pub const MANIFEST_DOMAIN: &[u8; 16] = b"RLTL-SETTLE-V1  ";

/// Canonical hash of the settlement legs.
///
/// Preimage: domain tag (16), settlement id (16), exchange key (32), leg count
/// as u16 little-endian, then for each leg in the supplied order the payer
/// collateral key (32), the payee collateral key (32) and the amount as u64
/// little-endian. Hashed with sha256.
pub fn manifest_hash(
    settlement_id: &[u8; 16],
    exchange: &Pubkey,
    legs: &[(Pubkey, Pubkey, u64)],
) -> [u8; 32] {
    let mut bytes = Vec::with_capacity(66 + legs.len() * 72);
    bytes.extend_from_slice(MANIFEST_DOMAIN);
    bytes.extend_from_slice(settlement_id);
    bytes.extend_from_slice(exchange.as_ref());
    bytes.extend_from_slice(&(legs.len() as u16).to_le_bytes());
    for (payer, payee, amount) in legs {
        bytes.extend_from_slice(payer.as_ref());
        bytes.extend_from_slice(payee.as_ref());
        bytes.extend_from_slice(&amount.to_le_bytes());
    }
    hash(&bytes).to_bytes()
}

#[program]
pub mod settlement {
    use super::*;

    pub fn initialize_exchange(context: Context<InitializeExchange>) -> Result<()> {
        let exchange = &mut context.accounts.exchange;
        exchange.authority = context.accounts.authority.key();
        exchange.mint = context.accounts.mint.key();
        exchange.vault = context.accounts.vault.key();
        exchange.bump = context.bumps.exchange;
        exchange.vault_bump = context.bumps.vault;
        exchange.total_collateral = 0;
        exchange.settlements = 0;
        Ok(())
    }

    pub fn open_collateral(context: Context<OpenCollateral>) -> Result<()> {
        let collateral = &mut context.accounts.collateral;
        collateral.owner = context.accounts.owner.key();
        collateral.exchange = context.accounts.exchange.key();
        collateral.balance = 0;
        collateral.frozen = false;
        collateral.bump = context.bumps.collateral;
        Ok(())
    }

    pub fn deposit_collateral(context: Context<DepositCollateral>, amount: u64) -> Result<()> {
        require!(amount > 0, SettlementError::ZeroAmount);
        require!(!context.accounts.collateral.frozen, SettlementError::AccountFrozen);

        token::transfer(
            CpiContext::new(
                context.accounts.token_program.to_account_info(),
                Transfer {
                    from: context.accounts.owner_tokens.to_account_info(),
                    to: context.accounts.vault.to_account_info(),
                    authority: context.accounts.owner.to_account_info(),
                },
            ),
            amount,
        )?;

        let collateral = &mut context.accounts.collateral;
        collateral.balance = collateral
            .balance
            .checked_add(amount)
            .ok_or(SettlementError::AmountOverflow)?;
        let exchange = &mut context.accounts.exchange;
        exchange.total_collateral = exchange
            .total_collateral
            .checked_add(amount)
            .ok_or(SettlementError::AmountOverflow)?;
        Ok(())
    }

    pub fn withdraw_collateral(context: Context<WithdrawCollateral>, amount: u64) -> Result<()> {
        require!(amount > 0, SettlementError::ZeroAmount);
        require!(!context.accounts.collateral.frozen, SettlementError::AccountFrozen);
        require!(
            context.accounts.collateral.balance >= amount,
            SettlementError::InsufficientCollateral
        );

        let exchange_key = context.accounts.exchange.key();
        let vault_seeds: &[&[u8]] = &[
            b"vault",
            exchange_key.as_ref(),
            &[context.accounts.exchange.vault_bump],
        ];
        token::transfer(
            CpiContext::new_with_signer(
                context.accounts.token_program.to_account_info(),
                Transfer {
                    from: context.accounts.vault.to_account_info(),
                    to: context.accounts.owner_tokens.to_account_info(),
                    authority: context.accounts.vault.to_account_info(),
                },
                &[vault_seeds],
            ),
            amount,
        )?;

        let collateral = &mut context.accounts.collateral;
        collateral.balance = collateral
            .balance
            .checked_sub(amount)
            .ok_or(SettlementError::AmountOverflow)?;
        let exchange = &mut context.accounts.exchange;
        exchange.total_collateral = exchange
            .total_collateral
            .checked_sub(amount)
            .ok_or(SettlementError::AmountOverflow)?;
        Ok(())
    }

    pub fn freeze_account(context: Context<FreezeAccount>, frozen: bool) -> Result<()> {
        context.accounts.collateral.frozen = frozen;
        Ok(())
    }

    /// Applies collateral transfers for one settlement batch.
    ///
    /// The receipt account is created here, so submitting the same settlement
    /// identity twice fails and the caller resolves the outcome by reading the
    /// receipt instead of paying again.
    pub fn settle_batch<'info>(
        context: Context<'_, '_, 'info, 'info, SettleBatch<'info>>,
        settlement_id: [u8; 16],
        manifest_hash: [u8; 32],
        legs: Vec<Leg>,
    ) -> Result<()> {
        require!(!legs.is_empty(), SettlementError::EmptyBatch);
        require!(legs.len() <= MAX_LEGS, SettlementError::BatchTooLarge);

        let exchange_key = context.accounts.exchange.key();
        let accounts = context.remaining_accounts;

        // Every supplied collateral account must be distinct, writable, owned by
        // this exchange, and at its canonical address. Two entries for the same
        // account would let one leg overwrite another leg's balance.
        for (position, info) in accounts.iter().enumerate() {
            require!(info.is_writable, SettlementError::AccountNotWritable);
            for other in accounts.iter().take(position) {
                require_keys_neq!(*info.key, *other.key, SettlementError::DuplicateAccount);
            }
            let collateral: Account<Collateral> = Account::try_from(info)?;
            require_keys_eq!(
                collateral.exchange,
                exchange_key,
                SettlementError::WrongExchange
            );
            let expected = Pubkey::create_program_address(
                &[
                    b"collateral",
                    exchange_key.as_ref(),
                    collateral.owner.as_ref(),
                    &[collateral.bump],
                ],
                &crate::ID,
            )
            .map_err(|_| SettlementError::InvalidCollateralAddress)?;
            require_keys_eq!(*info.key, expected, SettlementError::InvalidCollateralAddress);
        }

        // Resolve every leg to the collateral account keys it moves value between.
        let mut resolved: Vec<(Pubkey, Pubkey, u64)> = Vec::with_capacity(legs.len());
        for leg in legs.iter() {
            require!(leg.amount > 0, SettlementError::ZeroAmount);
            require!(leg.payer != leg.payee, SettlementError::SelfSettlement);

            let payer_info = accounts
                .get(usize::from(leg.payer))
                .ok_or(SettlementError::MissingAccount)?;
            let payee_info = accounts
                .get(usize::from(leg.payee))
                .ok_or(SettlementError::MissingAccount)?;
            require_keys_neq!(
                *payer_info.key,
                *payee_info.key,
                SettlementError::SelfSettlement
            );
            resolved.push((*payer_info.key, *payee_info.key, leg.amount));
        }

        // The supplied hash must describe the legs that are about to be applied.
        let computed = crate::manifest_hash(&settlement_id, &exchange_key, &resolved);
        require!(computed == manifest_hash, SettlementError::ManifestMismatch);

        for leg in legs.iter() {
            let payer_info = accounts
                .get(usize::from(leg.payer))
                .ok_or(SettlementError::MissingAccount)?;
            let payee_info = accounts
                .get(usize::from(leg.payee))
                .ok_or(SettlementError::MissingAccount)?;

            let mut payer: Account<Collateral> = Account::try_from(payer_info)?;
            let mut payee: Account<Collateral> = Account::try_from(payee_info)?;
            require!(!payer.frozen && !payee.frozen, SettlementError::AccountFrozen);
            require!(
                payer.balance >= leg.amount,
                SettlementError::InsufficientCollateral
            );

            payer.balance = payer
                .balance
                .checked_sub(leg.amount)
                .ok_or(SettlementError::AmountOverflow)?;
            payee.balance = payee
                .balance
                .checked_add(leg.amount)
                .ok_or(SettlementError::AmountOverflow)?;
            payer.exit(&crate::ID)?;
            payee.exit(&crate::ID)?;
        }

        let receipt = &mut context.accounts.receipt;
        receipt.exchange = exchange_key;
        receipt.settlement_id = settlement_id;
        receipt.manifest_hash = manifest_hash;
        receipt.leg_count = legs.len() as u16;
        receipt.applied_slot = Clock::get()?.slot;

        let exchange = &mut context.accounts.exchange;
        exchange.settlements = exchange
            .settlements
            .checked_add(1)
            .ok_or(SettlementError::AmountOverflow)?;
        Ok(())
    }
}

#[derive(AnchorSerialize, AnchorDeserialize, Clone, Copy)]
pub struct Leg {
    /// Index into remaining_accounts of the paying collateral account.
    pub payer: u8,
    pub payee: u8,
    pub amount: u64,
}

#[account]
pub struct Exchange {
    pub authority: Pubkey,
    pub mint: Pubkey,
    pub vault: Pubkey,
    pub bump: u8,
    pub vault_bump: u8,
    pub total_collateral: u64,
    pub settlements: u64,
}

impl Exchange {
    pub const SIZE: usize = 8 + 32 + 32 + 32 + 1 + 1 + 8 + 8;
}

#[account]
pub struct Collateral {
    pub owner: Pubkey,
    pub exchange: Pubkey,
    pub balance: u64,
    pub frozen: bool,
    pub bump: u8,
}

impl Collateral {
    pub const SIZE: usize = 8 + 32 + 32 + 8 + 1 + 1;
}

#[account]
pub struct Receipt {
    pub exchange: Pubkey,
    pub settlement_id: [u8; 16],
    pub manifest_hash: [u8; 32],
    pub leg_count: u16,
    pub applied_slot: u64,
}

impl Receipt {
    pub const SIZE: usize = 8 + 32 + 16 + 32 + 2 + 8;
}

#[derive(Accounts)]
pub struct InitializeExchange<'info> {
    #[account(mut)]
    pub authority: Signer<'info>,
    #[account(
        init,
        payer = authority,
        space = Exchange::SIZE,
        seeds = [b"exchange", authority.key().as_ref(), mint.key().as_ref()],
        bump
    )]
    pub exchange: Account<'info, Exchange>,
    pub mint: Account<'info, Mint>,
    #[account(
        init,
        payer = authority,
        seeds = [b"vault", exchange.key().as_ref()],
        bump,
        token::mint = mint,
        token::authority = vault
    )]
    pub vault: Account<'info, TokenAccount>,
    pub token_program: Program<'info, Token>,
    pub system_program: Program<'info, System>,
    pub rent: Sysvar<'info, Rent>,
}

#[derive(Accounts)]
pub struct OpenCollateral<'info> {
    #[account(mut)]
    pub owner: Signer<'info>,
    pub exchange: Account<'info, Exchange>,
    #[account(
        init,
        payer = owner,
        space = Collateral::SIZE,
        seeds = [b"collateral", exchange.key().as_ref(), owner.key().as_ref()],
        bump
    )]
    pub collateral: Account<'info, Collateral>,
    pub system_program: Program<'info, System>,
}

#[derive(Accounts)]
pub struct DepositCollateral<'info> {
    pub owner: Signer<'info>,
    #[account(mut, has_one = vault, has_one = mint)]
    pub exchange: Account<'info, Exchange>,
    #[account(
        mut,
        has_one = owner,
        seeds = [b"collateral", exchange.key().as_ref(), owner.key().as_ref()],
        bump = collateral.bump
    )]
    pub collateral: Account<'info, Collateral>,
    pub mint: Account<'info, Mint>,
    #[account(mut, token::mint = mint, token::authority = owner)]
    pub owner_tokens: Account<'info, TokenAccount>,
    #[account(mut, seeds = [b"vault", exchange.key().as_ref()], bump = exchange.vault_bump)]
    pub vault: Account<'info, TokenAccount>,
    pub token_program: Program<'info, Token>,
}

#[derive(Accounts)]
pub struct WithdrawCollateral<'info> {
    pub owner: Signer<'info>,
    #[account(mut, has_one = vault, has_one = mint)]
    pub exchange: Account<'info, Exchange>,
    #[account(
        mut,
        has_one = owner,
        seeds = [b"collateral", exchange.key().as_ref(), owner.key().as_ref()],
        bump = collateral.bump
    )]
    pub collateral: Account<'info, Collateral>,
    pub mint: Account<'info, Mint>,
    #[account(mut, token::mint = mint, token::authority = owner)]
    pub owner_tokens: Account<'info, TokenAccount>,
    #[account(mut, seeds = [b"vault", exchange.key().as_ref()], bump = exchange.vault_bump)]
    pub vault: Account<'info, TokenAccount>,
    pub token_program: Program<'info, Token>,
}

#[derive(Accounts)]
pub struct FreezeAccount<'info> {
    pub authority: Signer<'info>,
    #[account(has_one = authority)]
    pub exchange: Account<'info, Exchange>,
    #[account(
        mut,
        seeds = [b"collateral", exchange.key().as_ref(), collateral.owner.as_ref()],
        bump = collateral.bump
    )]
    pub collateral: Account<'info, Collateral>,
}

#[derive(Accounts)]
#[instruction(settlement_id: [u8; 16])]
pub struct SettleBatch<'info> {
    #[account(mut)]
    pub authority: Signer<'info>,
    #[account(mut, has_one = authority)]
    pub exchange: Account<'info, Exchange>,
    #[account(
        init,
        payer = authority,
        space = Receipt::SIZE,
        seeds = [b"receipt", exchange.key().as_ref(), settlement_id.as_ref()],
        bump
    )]
    pub receipt: Account<'info, Receipt>,
    pub system_program: Program<'info, System>,
}

#[error_code]
pub enum SettlementError {
    #[msg("amount must be greater than zero")]
    ZeroAmount,
    #[msg("collateral account is frozen")]
    AccountFrozen,
    #[msg("insufficient collateral")]
    InsufficientCollateral,
    #[msg("arithmetic overflow")]
    AmountOverflow,
    #[msg("batch contains no legs")]
    EmptyBatch,
    #[msg("batch exceeds the supported leg count")]
    BatchTooLarge,
    #[msg("a leg cannot pay itself")]
    SelfSettlement,
    #[msg("a referenced collateral account was not supplied")]
    MissingAccount,
    #[msg("collateral account belongs to another exchange")]
    WrongExchange,
    #[msg("the same collateral account was supplied twice")]
    DuplicateAccount,
    #[msg("collateral account must be writable")]
    AccountNotWritable,
    #[msg("collateral account is not at its canonical address")]
    InvalidCollateralAddress,
    #[msg("manifest hash does not match the supplied legs")]
    ManifestMismatch,
}

#[cfg(test)]
mod tests {
    use super::*;

    // Golden vector shared with the TypeScript client.
    #[test]
    fn manifest_hash_matches_the_golden_vector() {
        let mut settlement_id = [0u8; 16];
        for (index, byte) in settlement_id.iter_mut().enumerate() {
            *byte = index as u8;
        }
        let exchange = Pubkey::new_from_array([0x11; 32]);
        let legs = [
            (
                Pubkey::new_from_array([0x21; 32]),
                Pubkey::new_from_array([0x22; 32]),
                1_000u64,
            ),
            (
                Pubkey::new_from_array([0x22; 32]),
                Pubkey::new_from_array([0x23; 32]),
                250_000u64,
            ),
        ];

        let digest = manifest_hash(&settlement_id, &exchange, &legs);
        assert_eq!(
            hex(&digest),
            "d11c5555601792674da104cd7a9b35046858ec7d025f9e02cf1c2c83d9e9bd9a"
        );
    }

    #[test]
    fn leg_order_changes_the_hash() {
        let settlement_id = [7u8; 16];
        let exchange = Pubkey::new_from_array([0x11; 32]);
        let a = (
            Pubkey::new_from_array([0x21; 32]),
            Pubkey::new_from_array([0x22; 32]),
            1u64,
        );
        let b = (
            Pubkey::new_from_array([0x23; 32]),
            Pubkey::new_from_array([0x24; 32]),
            2u64,
        );
        assert_ne!(
            manifest_hash(&settlement_id, &exchange, &[a, b]),
            manifest_hash(&settlement_id, &exchange, &[b, a])
        );
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}
