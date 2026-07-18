import type { HealthIndicatorService } from '@nestjs/terminus';
import type { AppConfig } from '../config/app.config';
import type { ListenerLiveness } from '../listeners/chain-event-listener';
import type { ListenersManager } from '../listeners/listeners.manager';
import { ListenerHealthIndicator } from './listener.health-indicator';

/** Minimal fake of Terminus' HealthIndicatorService.check(key). */
const fakeHealth = {
  check: (key: string) => ({
    up: (details: object) => ({ [key]: { status: 'up', ...details } }),
    down: (details: object) => ({ [key]: { status: 'down', ...details } }),
  }),
} as unknown as HealthIndicatorService;

const STALL_SECONDS = 90;
const NOW = 1_000_000_000_000;

function make(
  snapshots: ListenerLiveness[],
  expected = snapshots.length,
): ListenerHealthIndicator {
  const manager = {
    expectedCount: () => expected,
    liveness: () => snapshots,
  } as unknown as ListenersManager;
  const appConfig = {
    listenerStallSeconds: STALL_SECONDS,
  } as unknown as AppConfig;
  return new ListenerHealthIndicator(fakeHealth, manager, appConfig);
}

interface ListenerDetail {
  network: string;
  head: number;
  idleSeconds: number;
  sawHead: boolean;
  stalled: boolean;
}
interface ListenersEntry {
  status: string;
  expected: number;
  thresholdSeconds: number;
  listeners: ListenerDetail[];
}

const entry = (r: object): ListenersEntry =>
  (r as Record<string, ListenersEntry>).listeners;
const status = (r: object) => entry(r).status;

describe('ListenerHealthIndicator', () => {
  beforeEach(() => jest.spyOn(Date, 'now').mockReturnValue(NOW));
  afterEach(() => jest.restoreAllMocks());

  const live = (over: Partial<ListenerLiveness> = {}): ListenerLiveness => ({
    network: 'Finney',
    head: 100,
    lastHeadAtMs: NOW - 12_000, // one block ago
    startedAtMs: NOW - 600_000,
    stopped: false,
    ...over,
  });

  it('is UP when no listeners are configured', () => {
    expect(status(make([], 0).check('listeners'))).toBe('up');
  });

  it('is UP when every listener saw a head within the window', () => {
    const r = make([live(), live({ network: 'Devnet' })]).check('listeners');
    expect(status(r)).toBe('up');
  });

  it('is DOWN when a listener has not seen a head past the stall threshold', () => {
    const r = make([
      live(),
      live({
        network: 'Devnet',
        lastHeadAtMs: NOW - (STALL_SECONDS + 5) * 1000,
      }),
    ]).check('listeners');
    expect(status(r)).toBe('down');
    const stuck = entry(r).listeners.find((l) => l.network === 'Devnet');
    expect(stuck?.stalled).toBe(true);
    expect(stuck?.idleSeconds).toBe(STALL_SECONDS + 5);
  });

  it('gives a fresh listener the same grace before its first head (measured from start)', () => {
    // Started 30s ago, no head yet — inside the 90s window, so not stalled.
    const r = make([
      live({ lastHeadAtMs: null, startedAtMs: NOW - 30_000, head: -1 }),
    ]).check('listeners');
    expect(status(r)).toBe('up');
    expect(entry(r).listeners[0].sawHead).toBe(false);
  });

  it('is DOWN when a listener never saw a head and the grace has elapsed', () => {
    const r = make([
      live({
        lastHeadAtMs: null,
        startedAtMs: NOW - (STALL_SECONDS + 1) * 1000,
        head: -1,
      }),
    ]).check('listeners');
    expect(status(r)).toBe('down');
  });

  it('is DOWN when a configured listener has not started (fewer running than expected)', () => {
    // Two configured, only one running and healthy.
    expect(status(make([live()], 2).check('listeners'))).toBe('down');
  });

  it('does not flag a stopped listener as stalled', () => {
    const r = make([
      live({ stopped: true, lastHeadAtMs: NOW - 10 * 60_000 }),
    ]).check('listeners');
    expect(status(r)).toBe('up');
    expect(entry(r).listeners[0].stalled).toBe(false);
  });
});
