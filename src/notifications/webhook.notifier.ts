import { Injectable, Logger } from '@nestjs/common';

/** Where to deliver a rendered message. */
export interface WebhookTarget {
  url: string;
  /** JSON key the message text is sent under (defaults to `text`). */
  field?: string;
}

/**
 * What a delivery attempt lets us honestly conclude — the distinction the
 * caller needs before deciding to retry.
 *
 * - `delivered` — the webhook accepted it (2xx).
 * - `rejected` — it definitively did **not** take the message: the request
 *   never reached the server (DNS failure, connection refused, TLS), or the
 *   server refused it outright (4xx, including a rate-limited 429, which is a
 *   "not processed" answer). A retry cannot duplicate what was never posted.
 * - `unknown` — the attempt failed *after* the request went out (5xx, read
 *   timeout, socket reset mid-flight). The message may already be in the
 *   channel, so a retry can duplicate it.
 */
export type DeliveryStatus = 'delivered' | 'rejected' | 'unknown';

export interface DeliveryResult {
  status: DeliveryStatus;
  /** Short cause, for the caller's log line (absent when delivered). */
  reason?: string;
}

/**
 * Error codes that mean the request never reached the server, so nothing can
 * have been posted. Anything *not* listed here is classified `unknown`: an
 * error raised once the request was on the wire (`ECONNRESET`, a headers/body
 * timeout, an abort) may have been served anyway. Erring towards `unknown`
 * costs at most a loud log line; erring the other way costs a duplicate alert.
 */
const NEVER_SENT_CODES = new Set([
  'ENOTFOUND',
  'EAI_AGAIN',
  'ECONNREFUSED',
  'EHOSTUNREACH',
  'ENETUNREACH',
  'UND_ERR_CONNECT_TIMEOUT',
  'CERT_HAS_EXPIRED',
  'DEPTH_ZERO_SELF_SIGNED_CERT',
  'UNABLE_TO_VERIFY_LEAF_SIGNATURE',
  'ERR_TLS_CERT_ALTNAME_INVALID',
]);

/** One POST, classified, plus the backoff when the server asked for one. */
interface Attempt {
  result: DeliveryResult;
  /** Present only on a 429: how long the server told us to wait. */
  rateLimitedForMs?: number;
}

/**
 * Posts a rendered message to a generic webhook (a Slack workflow webhook,
 * BetterStack, Discord, etc. — anything that accepts a JSON POST). Delivery is
 * best-effort: a failure is logged and swallowed so a flaky webhook never
 * crashes the listener. Honours one retry on HTTP 429 (`Retry-After`).
 *
 * Every outcome is reported as a {@link DeliveryStatus} rather than a bare
 * boolean, because "it failed" is not one thing: only a failure that provably
 * posted nothing may be retried by the caller.
 */
@Injectable()
export class WebhookNotifier {
  private readonly logger = new Logger(WebhookNotifier.name);

  async send(target: WebhookTarget, text: string): Promise<DeliveryResult> {
    const field = target.field ?? 'text';
    const body = JSON.stringify({ [field]: text });

    let attempt = await this.attempt(target.url, body);
    if (attempt.rateLimitedForMs !== undefined) {
      this.logger.warn(
        `Webhook rate-limited; retrying in ${attempt.rateLimitedForMs}ms.`,
      );
      await delay(attempt.rateLimitedForMs);
      // One retry only. A second 429 is still `rejected` (nothing posted), so
      // the caller stays free to try again on its own schedule.
      attempt = await this.attempt(target.url, body);
    }

    // Logged once, on the outcome that is actually returned — a 429 that the
    // retry then delivered is not a failure and must not read like one.
    const { result } = attempt;
    if (result.status !== 'delivered') {
      this.logger.error(`Webhook delivery ${result.status}: ${result.reason}.`);
    }
    return result;
  }

  private async attempt(url: string, body: string): Promise<Attempt> {
    try {
      const res = await this.post(url, body);
      const result = classifyStatus(res.status);
      // 429 is classified like any other 4xx (nothing posted); the header only
      // tells us how long to hold off before the one retry.
      return res.status === 429
        ? {
            result,
            rateLimitedForMs: retryAfterMs(res.headers.get('retry-after')),
          }
        : { result };
    } catch (err) {
      return { result: classifyError(err as Error) };
    }
  }

  private post(url: string, body: string): Promise<Response> {
    return fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
    });
  }
}

function classifyStatus(status: number): DeliveryResult {
  if (status >= 200 && status < 300) return { status: 'delivered' };
  // 4xx (429 included): the server read the request and refused it — nothing
  // was posted. Anything else (5xx above all) may have been accepted before it
  // broke.
  return {
    status: status >= 400 && status < 500 ? 'rejected' : 'unknown',
    reason: `HTTP ${status}`,
  };
}

function classifyError(err: Error): DeliveryResult {
  const code = errorCode(err);
  return {
    status:
      code !== undefined && NEVER_SENT_CODES.has(code) ? 'rejected' : 'unknown',
    reason: code ? `${err.message} (${code})` : err.message,
  };
}

/**
 * Digs the OS/undici error code out of a `fetch` rejection. Node wraps those in
 * a `TypeError: fetch failed` whose `cause` carries the real code.
 */
function errorCode(err: Error): string | undefined {
  const direct = (err as { code?: unknown }).code;
  if (typeof direct === 'string') return direct;
  const cause = (err as { cause?: unknown }).cause;
  const nested = (cause as { code?: unknown } | undefined)?.code;
  return typeof nested === 'string' ? nested : undefined;
}

function retryAfterMs(header: string | null): number {
  const seconds = header ? Number(header) : NaN;
  return Number.isFinite(seconds) && seconds > 0
    ? Math.min(seconds * 1000, 30_000)
    : 1000;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
