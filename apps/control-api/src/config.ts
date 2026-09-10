export interface Config {
  host: string;
  port: number;
  socketPath: string;
  databasePath: string;
  gatewayToken: string | undefined;
  apiToken: string | undefined;
  pollIntervalMs: number;
  settlementBatchSize: number;
}

/** Defaults bind to loopback. Nothing here reads secrets from disk. */
export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  return {
    host: env.RLTL_API_HOST ?? '127.0.0.1',
    port: Number.parseInt(env.RLTL_API_PORT ?? '8080', 10),
    socketPath: env.RLTL_CONTROL_SOCKET ?? '/tmp/rltl-control.sock',
    databasePath: env.RLTL_DB_PATH ?? 'control-plane.db',
    gatewayToken: env.RLTL_CONTROL_TOKEN,
    apiToken: env.RLTL_API_TOKEN,
    pollIntervalMs: Number.parseInt(env.RLTL_POLL_INTERVAL_MS ?? '100', 10),
    settlementBatchSize: Number.parseInt(env.RLTL_SETTLEMENT_BATCH ?? '50', 10),
  };
}
