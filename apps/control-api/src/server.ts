import Fastify from 'fastify';
import websocket from '@fastify/websocket';
import type { FastifyInstance, FastifyRequest } from 'fastify';
import type Database from 'better-sqlite3';

import type { Config } from './config.js';
import type { GatewayClient } from './gateway.js';
import { GatewayError } from './gateway.js';
import type { Projection } from './projection.js';
import { fetchSnapshot } from './projection.js';
import { renderMetrics } from './metrics.js';
import type { SettlementOutbox } from './settlement/outbox.js';
import type { SettlementDispatcher } from './settlement/dispatcher.js';
import type { EngineSnapshot, GatewayHealth } from './types.js';

export interface Services {
  config: Config;
  db: Database.Database;
  gateway: GatewayClient;
  projection: Projection;
  outbox: SettlementOutbox;
  dispatcher: SettlementDispatcher;
}

const decimalString = { type: 'string', pattern: '^[0-9]{1,39}$' } as const;

function constantTimeEq(left: string, right: string): boolean {
  const a = Buffer.from(left);
  const b = Buffer.from(right);
  const len = Math.max(a.length, b.length);
  let diff = a.length ^ b.length;
  for (let index = 0; index < len; index += 1) {
    diff |= (a[index] ?? 0) ^ (b[index] ?? 0);
  }
  return diff === 0;
}

function bearerToken(request: FastifyRequest): string | undefined {
  const header = request.headers.authorization;
  if (header !== undefined && header.startsWith('Bearer ')) {
    return header.slice('Bearer '.length);
  }
  const query = (request.query as { token?: string } | undefined)?.token;
  return typeof query === 'string' ? query : undefined;
}

/** Mutations and sensitive reads require a bearer token. Health stays open. */
function authorize(services: Services, request: FastifyRequest): boolean {
  const expected = services.config.apiToken;
  if (expected === undefined) {
    return false;
  }
  const provided = bearerToken(request);
  return provided !== undefined && constantTimeEq(provided, expected);
}

export function buildServer(services: Services): FastifyInstance {
  const app = Fastify({
    logger: {
      level: process.env.RLTL_LOG_LEVEL ?? 'info',
      // Never log credentials.
      redact: ['req.headers.authorization', 'token'],
    },
  });

  app.setErrorHandler((error: unknown, _request, reply) => {
    if (error instanceof Error && 'validation' in error) {
      return reply.code(400).send({ error: 'invalid_request', message: error.message });
    }
    if (error instanceof GatewayError) {
      const status = error.code === 'timeout' || error.code === 'disconnected' ? 503 : 400;
      return reply.code(status).send({ error: error.code, message: error.message });
    }
    app.log.error({ err: error }, 'request failed');
    return reply.code(500).send({ error: 'internal_error' });
  });

  app.addHook('onRequest', async (request, reply) => {
    const path = request.url.split('?')[0] ?? request.url;
    const open = request.method === 'GET' && (path === '/health' || path === '/health/');
    if (!open && !authorize(services, request)) {
      await reply.code(401).send({ error: 'unauthorized' });
    }
  });

  registerReadRoutes(app, services);
  registerControlRoutes(app, services);
  registerStream(app, services);
  return app;
}

function position(services: Services): {
  asOfEngineSeq: string;
  projectionSeq: string;
  runId: string;
  health: string;
} {
  const state = services.projection.position;
  return {
    asOfEngineSeq: state.asOfEngineSeq.toString(),
    projectionSeq: state.projectionSeq.toString(),
    runId: state.runId,
    health: state.health,
  };
}

function registerReadRoutes(app: FastifyInstance, services: Services): void {
  app.get('/health', async () => {
    let gateway: GatewayHealth | null = null;
    let gatewayError: string | null = null;
    try {
      gateway = await services.gateway.call<GatewayHealth>('health');
    } catch (error) {
      gatewayError = error instanceof Error ? error.message : 'gateway unreachable';
    }
    const projected = services.projection.position;
    const projectionUnhealthy = projected.health !== 'healthy';
    return {
      status:
        gateway === null || projectionUnhealthy
          ? projected.health === 'run_mismatch'
            ? 'error'
            : 'degraded'
          : 'ok',
      ...position(services),
      projection: {
        health: projected.health,
        runId: projected.runId,
        eventsApplied: services.projection.counters.eventsApplied.toString(),
        gapsObserved: services.projection.counters.gapsObserved.toString(),
        pollErrors: services.projection.counters.pollErrors.toString(),
      },
      settlements: Object.fromEntries(
        Object.entries(services.outbox.countByStatus()).map(([status, count]) => [
          status,
          count.toString(),
        ]),
      ),
      gateway,
      gatewayError,
    };
  });

  app.get('/metrics', async (_request, reply) => {
    let health: GatewayHealth | null = null;
    let snapshot: EngineSnapshot | null = null;
    try {
      health = await services.gateway.call<GatewayHealth>('health');
      snapshot = await fetchSnapshot(services.gateway);
    } catch {
      // Metrics stay available when the engine is down; engine gauges are omitted.
    }
    const body = renderMetrics({
      db: services.db,
      projection: services.projection,
      outbox: services.outbox,
      dispatcher: services.dispatcher,
      health,
      snapshot,
    });
    return reply.type('text/plain; version=0.0.4; charset=utf-8').send(body);
  });

  app.get(
    '/book/:symbol',
    {
      schema: {
        params: {
          type: 'object',
          required: ['symbol'],
          properties: { symbol: { type: 'string', maxLength: 32 } },
        },
        querystring: {
          type: 'object',
          properties: { depth: { type: 'integer', minimum: 1, maximum: 64, default: 10 } },
        },
      },
    },
    async (request, reply) => {
      const { symbol } = request.params as { symbol: string };
      const { depth } = request.query as { depth?: number };
      const snapshot = await fetchSnapshot(services.gateway);
      const instrument = snapshot.instruments.find((entry) => entry.symbol === symbol);
      if (instrument === undefined) {
        return reply.code(404).send({ error: 'unknown_symbol', symbol });
      }
      const limit = depth ?? 10;
      const bids = instrument.bookLevels.filter((level) => level.side === 'buy').slice(0, limit);
      const asks = instrument.bookLevels.filter((level) => level.side === 'sell').slice(0, limit);
      return {
        symbol,
        asOfEngineSeq: snapshot.asOfEngineSeq,
        feedState: instrument.feedState,
        marketBestBidTicks: instrument.marketBestBidTicks,
        marketBestAskTicks: instrument.marketBestAskTicks,
        bids,
        asks,
      };
    },
  );

  app.get(
    '/orders',
    {
      schema: {
        querystring: {
          type: 'object',
          properties: {
            account: { type: 'integer', minimum: 0 },
            limit: { type: 'integer', minimum: 1, maximum: 1000, default: 200 },
          },
        },
      },
    },
    async (request) => {
      const { account, limit } = request.query as { account?: number; limit?: number };
      const rows =
        account === undefined
          ? services.db
              .prepare('SELECT * FROM orders ORDER BY order_id LIMIT ?')
              .all(BigInt(limit ?? 200))
          : services.db
              .prepare('SELECT * FROM orders WHERE account = ? ORDER BY order_id LIMIT ?')
              .all(BigInt(account), BigInt(limit ?? 200));
      return { ...position(services), orders: (rows as Record<string, unknown>[]).map(stringify) };
    },
  );

  app.get(
    '/trades',
    {
      schema: {
        querystring: {
          type: 'object',
          properties: {
            sinceTradeId: decimalString,
            limit: { type: 'integer', minimum: 1, maximum: 1000, default: 200 },
          },
        },
      },
    },
    async (request) => {
      const { sinceTradeId, limit } = request.query as { sinceTradeId?: string; limit?: number };
      const rows = services.db
        .prepare('SELECT * FROM trades WHERE trade_id > ? ORDER BY trade_id LIMIT ?')
        .all(BigInt(sinceTradeId ?? '0'), BigInt(limit ?? 200));
      return { ...position(services), trades: (rows as Record<string, unknown>[]).map(stringify) };
    },
  );

  app.get(
    '/positions',
    {
      schema: {
        querystring: {
          type: 'object',
          properties: { account: { type: 'integer', minimum: 0 } },
        },
      },
    },
    async (request) => {
      const { account } = request.query as { account?: number };
      const rows =
        account === undefined
          ? services.db.prepare('SELECT * FROM positions ORDER BY account, instrument').all()
          : services.db
              .prepare('SELECT * FROM positions WHERE account = ? ORDER BY instrument')
              .all(BigInt(account));
      return {
        ...position(services),
        positions: (rows as Record<string, unknown>[]).map(stringify),
      };
    },
  );

  app.get(
    '/settlements',
    {
      schema: {
        querystring: {
          type: 'object',
          properties: {
            status: {
              type: 'string',
              enum: ['pending', 'submitted', 'unknown', 'confirmed', 'failed', 'needs_operator'],
            },
            limit: { type: 'integer', minimum: 1, maximum: 500, default: 100 },
          },
        },
      },
    },
    async (request) => {
      const { status, limit } = request.query as { status?: string; limit?: number };
      const rows =
        status === undefined
          ? services.db
              .prepare('SELECT * FROM settlements ORDER BY first_trade_id LIMIT ?')
              .all(BigInt(limit ?? 100))
          : services.db
              .prepare('SELECT * FROM settlements WHERE status = ? ORDER BY first_trade_id LIMIT ?')
              .all(status, BigInt(limit ?? 100));
      return {
        ...position(services),
        settlements: (rows as Record<string, unknown>[]).map(stringify),
      };
    },
  );
}

function registerControlRoutes(app: FastifyInstance, services: Services): void {
  app.post(
    '/risk/limits',
    {
      schema: {
        body: {
          type: 'object',
          additionalProperties: false,
          required: [
            'account',
            'maxOrderQuantity',
            'maxOrderNotional',
            'maxPositionLots',
            'maxGrossExposure',
            'priceCollarTicks',
          ],
          properties: {
            account: { type: 'integer', minimum: 0, maximum: 4294967295 },
            maxOrderQuantity: decimalString,
            maxOrderNotional: decimalString,
            maxPositionLots: decimalString,
            maxGrossExposure: decimalString,
            priceCollarTicks: { type: 'integer', minimum: 0, maximum: 1000000 },
          },
        },
      },
    },
    async (request) => {
      const body = request.body as Record<string, unknown>;
      await services.gateway.call('setRiskLimits', body);
      return { accepted: true, ...position(services) };
    },
  );

  app.post(
    '/engine/kill',
    {
      schema: {
        body: {
          type: 'object',
          additionalProperties: false,
          required: ['engaged'],
          properties: { engaged: { type: 'boolean' } },
        },
      },
    },
    async (request) => {
      const body = request.body as { engaged: boolean };
      await services.gateway.call('engageKill', { engaged: body.engaged });
      app.log.warn({ engaged: body.engaged }, 'kill switch changed');
      return { accepted: true, engaged: body.engaged, ...position(services) };
    },
  );

  app.post(
    '/replay/start',
    {
      schema: {
        body: {
          type: 'object',
          additionalProperties: false,
          required: ['seed', 'events'],
          properties: {
            seed: { type: 'integer', minimum: 0 },
            events: { type: 'integer', minimum: 1, maximum: 1000000 },
          },
        },
      },
    },
    async (request) => {
      const body = request.body as { seed: number; events: number };
      const result = await services.gateway.call<Record<string, unknown>>('startReplay', body);
      return { ...result, ...position(services) };
    },
  );

  app.post('/projection/resync', async () => {
    await services.projection.rebuild();
    return { accepted: true, ...position(services) };
  });

  app.post(
    '/settlements/:id/release',
    {
      schema: {
        params: {
          type: 'object',
          required: ['id'],
          properties: { id: { type: 'string', minLength: 1, maxLength: 80 } },
        },
      },
    },
    async (request, reply) => {
      const { id } = request.params as { id: string };
      try {
        services.outbox.releaseFailed(id);
      } catch (error) {
        return reply.code(409).send({
          error: 'cannot_release',
          message: error instanceof Error ? error.message : 'cannot release',
        });
      }
      return { accepted: true, settlementId: id, ...position(services) };
    },
  );
}

/**
 * Streams projected events. Each frame carries the projection sequence; a
 * client that sees a jump should re-read the REST snapshot.
 */
function registerStream(app: FastifyInstance, services: Services): void {
  // The websocket plugin must finish loading before the route is declared.
  void app.register(async (instance) => {
    await instance.register(websocket);
    instance.get('/stream', { websocket: true }, (socket, request) => {
      if (!authorize(services, request)) {
        socket.close(1008, 'unauthorized');
        return;
      }
      const threshold = services.config.wsMaxBufferedBytes ?? 1_048_576;
      const send = (payload: unknown): void => {
        if (socket.readyState !== socket.OPEN) {
          return;
        }
        if (websocketWouldBlock(socket.bufferedAmount, threshold)) {
          socket.close(1013, 'slow consumer');
          return;
        }
        socket.send(JSON.stringify(payload));
      };
      send({ type: 'hello', ...position(services) });
      const listener = (event: unknown, projectionSeq: bigint): void => {
        send({ type: 'event', projectionSeq: projectionSeq.toString(), event });
      };
      services.projection.onEvent(listener);
      socket.on('close', () => {
        services.projection.offEvent(listener);
      });
    });
  });
}

export function websocketWouldBlock(bufferedAmount: number, threshold: number): boolean {
  return bufferedAmount > threshold;
}

/** better-sqlite3 returns BigInt; JSON gets decimal strings. */
function stringify(row: Record<string, unknown>): Record<string, unknown> {
  const output: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(row)) {
    output[key] = typeof value === 'bigint' ? value.toString() : value;
  }
  return output;
}
