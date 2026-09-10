import net from 'node:net';
import { randomUUID } from 'node:crypto';

// Must match crates/control-gateway/src/wire.rs.
export const SCHEMA_VERSION = 1;
const MAX_FRAME_BYTES = 8 * 1024 * 1024;

export interface GatewayResponse<T> {
  schemaVersion: number;
  commandId: string;
  ok: boolean;
  result?: T;
  error?: { code: string; message: string };
  asOfEngineSeq: string;
}

export class GatewayError extends Error {
  constructor(
    readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = 'GatewayError';
  }
}

interface Pending {
  resolve: (value: GatewayResponse<unknown>) => void;
  reject: (error: Error) => void;
  timer: NodeJS.Timeout;
}

/**
 * Length-prefixed JSON client for the Rust control gateway. One connection,
 * command IDs correlate responses, every call has a timeout.
 */
export class GatewayClient {
  private socket: net.Socket | null = null;
  private buffer = Buffer.alloc(0);
  private readonly pending = new Map<string, Pending>();
  private connecting: Promise<net.Socket> | null = null;

  constructor(
    private readonly socketPath: string,
    private readonly token: string | undefined,
    private readonly timeoutMs = 5_000,
  ) {}

  /** One reconnect attempt, because a gateway restart drops the connection. */
  async call<T>(method: string, params: Record<string, unknown> = {}): Promise<T> {
    try {
      return await this.send<T>(method, params);
    } catch (error) {
      if (error instanceof GatewayError && error.code === 'disconnected') {
        this.socket?.destroy();
        this.socket = null;
        return this.send<T>(method, params);
      }
      throw error;
    }
  }

  private async send<T>(method: string, params: Record<string, unknown>): Promise<T> {
    const socket = await this.connect();
    const commandId = randomUUID();
    const request: Record<string, unknown> = {
      schemaVersion: SCHEMA_VERSION,
      commandId,
      method,
      params,
    };
    if (this.token !== undefined) {
      request.token = this.token;
    }

    const body = Buffer.from(JSON.stringify(request), 'utf8');
    if (body.length > MAX_FRAME_BYTES) {
      throw new GatewayError('frame_too_large', 'request exceeds the IPC frame bound');
    }
    const frame = Buffer.allocUnsafe(4 + body.length);
    frame.writeUInt32LE(body.length, 0);
    body.copy(frame, 4);

    const response = await new Promise<GatewayResponse<unknown>>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(commandId);
        reject(new GatewayError('timeout', `no response for ${method} within ${this.timeoutMs}ms`));
      }, this.timeoutMs);
      timer.unref();
      this.pending.set(commandId, { resolve, reject, timer });
      socket.write(frame, (error) => {
        if (error !== undefined && error !== null) {
          clearTimeout(timer);
          this.pending.delete(commandId);
          reject(new GatewayError('disconnected', error.message));
        }
      });
    });

    if (response.schemaVersion !== SCHEMA_VERSION) {
      throw new GatewayError('unsupported_schema', `gateway replied with schema ${response.schemaVersion}`);
    }
    if (!response.ok) {
      throw new GatewayError(response.error?.code ?? 'unknown', response.error?.message ?? 'gateway rejected the command');
    }
    return response.result as T;
  }

  close(): void {
    for (const [commandId, pending] of this.pending) {
      clearTimeout(pending.timer);
      pending.reject(new GatewayError('closed', 'client closed'));
      this.pending.delete(commandId);
    }
    this.socket?.destroy();
    this.socket = null;
  }

  private connect(): Promise<net.Socket> {
    if (this.socket !== null && !this.socket.destroyed) {
      return Promise.resolve(this.socket);
    }
    if (this.connecting !== null) {
      return this.connecting;
    }

    this.connecting = new Promise<net.Socket>((resolve, reject) => {
      const socket = net.createConnection(this.socketPath);
      socket.once('connect', () => {
        this.socket = socket;
        this.buffer = Buffer.alloc(0);
        this.connecting = null;
        resolve(socket);
      });
      socket.on('data', (chunk) => this.onData(chunk));
      socket.on('error', (error) => {
        this.connecting = null;
        this.failAll(error);
        reject(error);
      });
      socket.on('close', () => {
        this.socket = null;
        this.failAll(new GatewayError('disconnected', 'gateway connection closed'));
      });
    });
    return this.connecting;
  }

  private onData(chunk: Buffer): void {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    for (;;) {
      if (this.buffer.length < 4) {
        return;
      }
      const length = this.buffer.readUInt32LE(0);
      if (length === 0 || length > MAX_FRAME_BYTES) {
        this.socket?.destroy();
        this.failAll(new GatewayError('bad_frame', 'gateway sent an out-of-bounds frame'));
        return;
      }
      if (this.buffer.length < 4 + length) {
        return;
      }
      const body = this.buffer.subarray(4, 4 + length);
      this.buffer = this.buffer.subarray(4 + length);

      let message: GatewayResponse<unknown>;
      try {
        message = JSON.parse(body.toString('utf8')) as GatewayResponse<unknown>;
      } catch {
        this.socket?.destroy();
        this.failAll(new GatewayError('bad_frame', 'gateway sent invalid JSON'));
        return;
      }
      const pending = this.pending.get(message.commandId);
      if (pending !== undefined) {
        clearTimeout(pending.timer);
        this.pending.delete(message.commandId);
        pending.resolve(message);
      }
    }
  }

  private failAll(error: Error): void {
    for (const [commandId, pending] of this.pending) {
      clearTimeout(pending.timer);
      pending.reject(error);
      this.pending.delete(commandId);
    }
  }
}
