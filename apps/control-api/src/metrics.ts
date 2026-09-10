import type Database from 'better-sqlite3';
import type { Projection } from './projection.js';
import type { SettlementOutbox } from './settlement/outbox.js';
import type { SettlementDispatcher } from './settlement/dispatcher.js';
import type { EngineSnapshot, GatewayHealth } from './types.js';

interface Sample {
  name: string;
  help: string;
  type: 'counter' | 'gauge';
  values: { labels?: Record<string, string>; value: bigint | number }[];
}

function render(samples: Sample[]): string {
  const lines: string[] = [];
  for (const sample of samples) {
    lines.push(`# HELP ${sample.name} ${sample.help}`);
    lines.push(`# TYPE ${sample.name} ${sample.type}`);
    for (const entry of sample.values) {
      const labels =
        entry.labels === undefined
          ? ''
          : `{${Object.entries(entry.labels)
              .map(([key, value]) => `${key}="${escapeLabel(value)}"`)
              .join(',')}}`;
      lines.push(`${sample.name}${labels} ${entry.value.toString()}`);
    }
  }
  return `${lines.join('\n')}\n`;
}

function escapeLabel(value: string): string {
  return value.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, ' ');
}

/**
 * OpenMetrics exposition built from the cold path only.
 *
 * Labels are bounded: instrument symbols, feed states, settlement statuses and
 * the fixed reject-reason enum. Order, account and trade identifiers are never
 * used as labels.
 */
export function renderMetrics(input: {
  db: Database.Database;
  projection: Projection;
  outbox: SettlementOutbox;
  dispatcher: SettlementDispatcher;
  health: GatewayHealth | null;
  snapshot: EngineSnapshot | null;
}): string {
  const { db, projection, outbox, dispatcher, health, snapshot } = input;
  const position = projection.position;
  const samples: Sample[] = [];

  samples.push({
    name: 'rltl_projection_events_total',
    help: 'Engine output events applied to the projection.',
    type: 'counter',
    values: [{ value: projection.counters.eventsApplied }],
  });
  samples.push({
    name: 'rltl_projection_trades_total',
    help: 'Trades applied to the projection.',
    type: 'counter',
    values: [{ value: projection.counters.tradesApplied }],
  });
  samples.push({
    name: 'rltl_projection_gaps_total',
    help: 'Output sequence gaps observed by the projection.',
    type: 'counter',
    values: [{ value: projection.counters.gapsObserved }],
  });
  samples.push({
    name: 'rltl_projection_poll_errors_total',
    help: 'Failed reads from the control gateway.',
    type: 'counter',
    values: [{ value: projection.counters.pollErrors }],
  });
  samples.push({
    name: 'rltl_projection_last_output_seq',
    help: 'Highest engine output sequence stored in the projection.',
    type: 'gauge',
    values: [{ value: position.lastOutputSeq }],
  });
  samples.push({
    name: 'rltl_projection_as_of_engine_seq',
    help: 'Engine sequence the projection is current to.',
    type: 'gauge',
    values: [{ value: position.asOfEngineSeq }],
  });

  const rejects = db.prepare('SELECT reason, count FROM risk_rejects').all() as {
    reason: string;
    count: bigint;
  }[];
  if (rejects.length > 0) {
    samples.push({
      name: 'rltl_risk_rejects_total',
      help: 'Pre-trade risk rejections by reason.',
      type: 'counter',
      values: rejects.map((row) => ({ labels: { reason: row.reason }, value: row.count })),
    });
  }

  const activeOrders = db.prepare('SELECT COUNT(*) AS total FROM orders').get() as {
    total: bigint;
  };
  samples.push({
    name: 'rltl_active_orders',
    help: 'Working orders in the projection.',
    type: 'gauge',
    values: [{ value: activeOrders.total }],
  });

  const statuses = outbox.countByStatus();
  samples.push({
    name: 'rltl_settlements',
    help: 'Settlements by status.',
    type: 'gauge',
    values: Object.entries(statuses).map(([status, count]) => ({
      labels: { status },
      value: count,
    })),
  });
  samples.push({
    name: 'rltl_settlement_attempts_total',
    help: 'Settlement submissions by outcome.',
    type: 'counter',
    values: [
      { labels: { outcome: 'submitted' }, value: dispatcher.counters.submitted },
      { labels: { outcome: 'confirmed' }, value: dispatcher.counters.confirmed },
      { labels: { outcome: 'already_applied' }, value: dispatcher.counters.alreadyApplied },
      { labels: { outcome: 'unknown' }, value: dispatcher.counters.unknown },
      { labels: { outcome: 'rejected' }, value: dispatcher.counters.rejected },
      { labels: { outcome: 'manifest_conflict' }, value: dispatcher.counters.manifestConflicts },
    ],
  });

  if (health !== null) {
    samples.push({
      name: 'rltl_engine_kill_engaged',
      help: 'Whether the engine kill switch is latched.',
      type: 'gauge',
      values: [{ value: health.killLatched ? 1 : 0 }],
    });
    samples.push({
      name: 'rltl_telemetry_dropped_total',
      help: 'Best-effort telemetry snapshots dropped by the engine.',
      type: 'counter',
      values: [{ value: BigInt(health.telemetryDropped) }],
    });
    samples.push({
      name: 'rltl_feed_source_seq',
      help: 'Last market-data source sequence per instrument.',
      type: 'gauge',
      values: health.feeds.map((feed) => ({
        labels: { symbol: feed.symbol },
        value: BigInt(feed.lastSourceSeq),
      })),
    });
    samples.push({
      name: 'rltl_feed_live',
      help: 'Whether the instrument feed is live.',
      type: 'gauge',
      values: health.feeds.map((feed) => ({
        labels: { symbol: feed.symbol, state: feed.feedState },
        value: feed.feedState === 'live' ? 1 : 0,
      })),
    });
  }

  if (snapshot !== null) {
    samples.push({
      name: 'rltl_engine_inputs_total',
      help: 'Inputs applied by the engine at the last snapshot.',
      type: 'counter',
      values: [{ value: BigInt(snapshot.metrics.inputsApplied ?? '0') }],
    });
    samples.push({
      name: 'rltl_engine_trades_total',
      help: 'Trades executed by the engine at the last snapshot.',
      type: 'counter',
      values: [{ value: BigInt(snapshot.metrics.trades ?? '0') }],
    });
  }

  samples.push({
    name: 'rltl_process_uptime_seconds',
    help: 'Control plane uptime.',
    type: 'gauge',
    values: [{ value: Math.floor(process.uptime()) }],
  });

  return render(samples);
}
