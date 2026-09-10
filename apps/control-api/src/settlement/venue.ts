import fs from 'node:fs';
import { Connection, Keypair, PublicKey } from '@solana/web3.js';
import type { Idl } from '@coral-xyz/anchor';

import type { SettlementAdapter } from './adapter.js';
import { CantonVenue } from './canton.js';
import { SimulatedVenue } from './simulated.js';
import { SolanaVenue } from './solana.js';

/**
 * Chooses the settlement venue from the environment. The simulated venue is the
 * default so the control plane runs without any external dependency.
 */
export function buildVenue(env: NodeJS.ProcessEnv = process.env): SettlementAdapter {
  switch (env.RLTL_SETTLEMENT_VENUE ?? 'simulated') {
    case 'simulated':
      return new SimulatedVenue();
    case 'solana':
      return buildSolanaVenue(env);
    case 'canton':
      return buildCantonVenue(env);
    default:
      throw new Error(`unknown settlement venue ${env.RLTL_SETTLEMENT_VENUE}`);
  }
}

function required(env: NodeJS.ProcessEnv, name: string): string {
  const value = env[name];
  if (value === undefined || value.length === 0) {
    throw new Error(`${name} is required for this settlement venue`);
  }
  return value;
}

/** Parses "1=<party or pubkey>,2=..." into an account map. */
function parseAccounts<T>(raw: string, convert: (value: string) => T): Map<number, T> {
  const accounts = new Map<number, T>();
  for (const entry of raw.split(',')) {
    const [account, value] = entry.split('=');
    if (account === undefined || value === undefined) {
      throw new Error(`malformed account mapping entry "${entry}"`);
    }
    accounts.set(Number.parseInt(account, 10), convert(value));
  }
  return accounts;
}

function buildSolanaVenue(env: NodeJS.ProcessEnv): SolanaVenue {
  const idl = JSON.parse(fs.readFileSync(required(env, 'RLTL_SOLANA_IDL'), 'utf8')) as Idl;
  const secret = JSON.parse(
    fs.readFileSync(required(env, 'RLTL_SOLANA_KEYPAIR'), 'utf8'),
  ) as number[];
  return new SolanaVenue({
    connection: new Connection(required(env, 'RLTL_SOLANA_RPC'), 'confirmed'),
    idl,
    programId: new PublicKey(idl.address),
    authority: Keypair.fromSecretKey(Uint8Array.from(secret)),
    exchange: new PublicKey(required(env, 'RLTL_SOLANA_EXCHANGE')),
    owners: parseAccounts(required(env, 'RLTL_SOLANA_ACCOUNTS'), (value) => new PublicKey(value)),
  });
}

function buildCantonVenue(env: NodeJS.ProcessEnv): CantonVenue {
  return new CantonVenue({
    baseUrl: required(env, 'RLTL_CANTON_JSON_API'),
    token: required(env, 'RLTL_CANTON_TOKEN'),
    operator: required(env, 'RLTL_CANTON_OPERATOR'),
    packageId: required(env, 'RLTL_CANTON_PACKAGE_ID'),
    parties: parseAccounts(required(env, 'RLTL_CANTON_PARTIES'), (value) => value),
  });
}
