import { loadConfig } from './config.js';
import { openDatabase } from './db.js';
import { GatewayClient } from './gateway.js';
import { Projection } from './projection.js';
import { buildServer } from './server.js';
import { SettlementDispatcher } from './settlement/dispatcher.js';
import { SettlementOutbox } from './settlement/outbox.js';
import { SimulatedVenue } from './settlement/simulated.js';

const config = loadConfig();
const db = openDatabase(config.databasePath);
const gateway = new GatewayClient(config.socketPath, config.gatewayToken);
const projection = new Projection(db, gateway);
const venue = new SimulatedVenue();
const outbox = new SettlementOutbox(db, venue.venue);
const dispatcher = new SettlementDispatcher(outbox, venue, config.settlementBatchSize);

const app = buildServer({ config, db, gateway, projection, outbox, dispatcher });

projection.start(config.pollIntervalMs);
dispatcher.start();

async function shutdown(): Promise<void> {
  projection.stop();
  dispatcher.stop();
  gateway.close();
  await app.close();
  db.close();
}

process.on('SIGINT', () => void shutdown().then(() => process.exit(0)));
process.on('SIGTERM', () => void shutdown().then(() => process.exit(0)));

await app.listen({ host: config.host, port: config.port });
if (config.apiToken === undefined) {
  app.log.warn('no RLTL_API_TOKEN configured; every mutating endpoint will answer 401');
}
