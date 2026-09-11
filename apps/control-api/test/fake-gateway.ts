import net from 'node:net';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import type { OutputEvent } from '../src/types.js';

interface Request {
  schemaVersion: number;
  commandId: string;
  method: string;
  token?: string;
  params?: Record<string, unknown>;
}

/**
 * Stand-in for the Rust control gateway. Speaks the same length-prefixed JSON
 * wire format so the control plane can be tested without the engine, including
 * the failure modes the real gateway can produce.
 */
export class FakeGateway {
  readonly socketPath: string;
  readonly calls: string[] = [];
  /** Stop answering, as if the gateway hung. */
  silent = false;
  /** Refuse every mutation. */
  unauthorized = false;

  private server: net.Server | null = null;
  private readonly sockets = new Set<net.Socket>();
  runId = '1';
  private queued: OutputEvent[] = [];
  private cursor = 0;
  private engineSeq = 0n;

  constructor(private readonly dir = fs.mkdtempSync(path.join(os.tmpdir(), 'rltl-fake-'))) {
    this.socketPath = path.join(this.dir, 'control.sock');
  }

  publish(events: OutputEvent[]): void {
    this.queued.push(...events);
    const last = events.at(-1);
    if (last !== undefined) {
      this.engineSeq = BigInt(last.engineSeq);
    }
  }

  /** Drops all queued output, as a fresh engine run would. */
  reset(): void {
    this.queued = [];
    this.cursor = 0;
    this.engineSeq = 0n;
    this.calls.length = 0;
  }

  /** Rewinds the read cursor, as the real gateway does after a restart. */
  rewind(): void {
    this.cursor = 0;
  }

  async start(): Promise<void> {
    this.server = net.createServer((socket) => {
      this.sockets.add(socket);
      let buffer = Buffer.alloc(0);
      socket.on('data', (chunk) => {
        buffer = Buffer.concat([buffer, chunk]);
        for (;;) {
          if (buffer.length < 4) {
            return;
          }
          const length = buffer.readUInt32LE(0);
          if (buffer.length < 4 + length) {
            return;
          }
          const body = buffer.subarray(4, 4 + length);
          buffer = buffer.subarray(4 + length);
          if (this.silent) {
            continue;
          }
          const response = this.handle(JSON.parse(body.toString('utf8')) as Request);
          const encoded = Buffer.from(JSON.stringify(response), 'utf8');
          const frame = Buffer.allocUnsafe(4 + encoded.length);
          frame.writeUInt32LE(encoded.length, 0);
          encoded.copy(frame, 4);
          socket.write(frame);
        }
      });
      socket.on('close', () => this.sockets.delete(socket));
      socket.on('error', () => this.sockets.delete(socket));
    });
    await new Promise<void>((resolve) => this.server!.listen(this.socketPath, resolve));
  }

  /** Closes connections without removing the socket file, then listens again. */
  async restart(): Promise<void> {
    await this.stop(false);
    fs.rmSync(this.socketPath, { force: true });
    await this.start();
  }

  async stop(removeDir = true): Promise<void> {
    for (const socket of this.sockets) {
      socket.destroy();
    }
    this.sockets.clear();
    if (this.server !== null) {
      await new Promise<void>((resolve) => this.server!.close(() => resolve()));
      this.server = null;
    }
    if (removeDir) {
      fs.rmSync(this.dir, { recursive: true, force: true });
    }
  }

  private handle(request: Request): Record<string, unknown> {
    this.calls.push(request.method);
    const asOfEngineSeq = this.engineSeq.toString();
    const fail = (code: string, message: string): Record<string, unknown> => ({
      schemaVersion: 1,
      commandId: request.commandId,
      ok: false,
      error: { code, message },
      asOfEngineSeq,
    });
    const ok = (result: unknown): Record<string, unknown> => ({
      schemaVersion: 1,
      commandId: request.commandId,
      ok: true,
      result,
      asOfEngineSeq,
    });

    const mutating = [
      'setRiskLimits',
      'engageKill',
      'startReplay',
      'setAccountEnabled',
      'resetJournal',
    ].includes(request.method);
    if (mutating && this.unauthorized) {
      return fail('unauthorized', 'invalid control token');
    }

    switch (request.method) {
      case 'health':
        return ok({
          status: 'ready',
          runId: this.runId,
          uptimeSeconds: 1,
          journalPath: '/tmp/fake.journal',
          deliveredOutputs: String(this.cursor),
          lastOutputSeq: asOfEngineSeq,
          asOfEngineSeq,
          globalKill: false,
          killLatched: false,
          telemetryDropped: '0',
          feeds: [{ instrument: 1, symbol: 'LAB-USD', feedState: 'live', lastSourceSeq: '42' }],
        });
      case 'outputs': {
        const limit = Number((request.params?.limit as number | undefined) ?? 500);
        const events = this.queued.slice(this.cursor, this.cursor + limit);
        this.cursor += events.length;
        return ok({ events });
      }
      case 'resetJournal':
        this.rewind();
        return ok({ accepted: true });
      case 'snapshot':
        return ok({
          runId: this.runId,
          asOfEngineSeq,
          engineTimeNs: '1000',
          globalKill: false,
          metrics: { inputsApplied: '10', trades: '2' },
          instruments: [
            {
              instrument: 1,
              symbol: 'LAB-USD',
              feedState: 'live',
              lastSourceSeq: '42',
              marketBestBidTicks: '9999',
              marketBestAskTicks: '10001',
              bookLevels: [
                { side: 'buy', priceTicks: '9999', quantityLots: '5', orderCount: 1 },
                { side: 'sell', priceTicks: '10001', quantityLots: '7', orderCount: 2 },
              ],
              bookOrders: [],
            },
          ],
          positions: [],
        });
      case 'engageKill':
      case 'setRiskLimits':
      case 'setAccountEnabled':
        return ok({ accepted: true });
      case 'startReplay':
        return ok({ jobId: 'replay-1', status: 'completed', stateDigest: 'deadbeef' });
      default:
        return fail('unknown_method', request.method);
    }
  }
}

let nextOutputSeq = 0;
let nextTradeId = 0;

export function resetEventIds(): void {
  nextOutputSeq = 0;
  nextTradeId = 0;
}

export function tradeEvent(overrides: Partial<OutputEvent> = {}): OutputEvent {
  nextOutputSeq += 1;
  nextTradeId += 1;
  return {
    kind: 'trade',
    outputSeq: String(nextOutputSeq),
    engineSeq: String(nextOutputSeq),
    engineTimeNs: '1000',
    tradeId: String(nextTradeId),
    instrument: 1,
    makerOrderId: '10',
    takerOrderId: '11',
    makerAccount: 1,
    takerAccount: 2,
    aggressor: 'buy',
    priceTicks: '10000',
    quantityLots: '5',
    ...overrides,
  } as OutputEvent;
}

export function reportEvent(overrides: Record<string, unknown> = {}): OutputEvent {
  nextOutputSeq += 1;
  return {
    kind: 'report',
    outputSeq: String(nextOutputSeq),
    engineSeq: String(nextOutputSeq),
    engineTimeNs: '1000',
    account: 1,
    instrument: 1,
    requestId: '7',
    clientOrderId: '100',
    orderId: '10',
    reportKind: 'accepted',
    orderState: 'working',
    side: 'buy',
    orderType: 'limit',
    priceTicks: '9999',
    totalQuantityLots: '10',
    cumulativeFilledLots: '0',
    remainingLots: '10',
    lastFillQuantityLots: '0',
    lastFillPriceTicks: '0',
    rejectReason: null,
    ...overrides,
  } as OutputEvent;
}
