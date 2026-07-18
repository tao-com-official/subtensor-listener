/**
 * How close (in blocks) two matches of the same event (type + arguments) must
 * be to count as duplicates. Covers both the backfill/live overlap and a reorg
 * moving an already-alerted event to a nearby block. Subtensor reorgs are 1–2
 * blocks deep, so 5 is comfortably above that. Deliberately not
 * env-configurable.
 */
export const DEDUP_WINDOW_BLOCKS = 5;

/**
 * In-memory dedup: remembers the block number of the last alert per key
 * (event type + arguments) and suppresses further matches within the window.
 * Suppressed matches do NOT move the marker, so a continuous stream of
 * matching blocks still alerts once per window rather than never. Bounded:
 * the oldest entries are evicted past `maxSize`.
 *
 * The service is stateless, so this only dedups within a single process
 * lifetime: a restart may re-alert an event still inside the backfill window.
 * That trade-off is intentional (no DB / no persistent disk).
 */
export class DedupCache {
  /** Insertion-ordered; re-alerting refreshes the entry's position. */
  private readonly lastAlerted = new Map<string, number>();

  constructor(
    private readonly windowBlocks = DEDUP_WINDOW_BLOCKS,
    private readonly maxSize = 5000,
  ) {}

  /**
   * Reports whether an alert at `blockNumber` should fire for `key`, and
   * **records it synchronously if so** — check and record are one atomic step,
   * so two blocks scanned concurrently (same batch) carrying the same event
   * can't both slip through. The window is symmetric (absolute distance) so a
   * reorg re-including the event in a slightly *earlier* block is also caught.
   * Suppressed matches do NOT move the marker, so a continuous stream still
   * alerts once per window rather than never.
   *
   * A reservation made here can be released with {@link forget} if the delivery
   * it was for then fails, so a transient webhook outage doesn't cost the alert.
   */
  shouldAlert(key: string, blockNumber: number): boolean {
    const last = this.lastAlerted.get(key);
    if (
      last !== undefined &&
      Math.abs(blockNumber - last) <= this.windowBlocks
    ) {
      return false;
    }
    this.lastAlerted.delete(key);
    this.lastAlerted.set(key, blockNumber);
    if (this.lastAlerted.size > this.maxSize) {
      for (const oldest of this.lastAlerted.keys()) {
        this.lastAlerted.delete(oldest);
        break;
      }
    }
    return true;
  }

  /**
   * Undoes a {@link shouldAlert} reservation for a delivery that then failed,
   * so the block can be retried. Only clears the marker if it still points at
   * this block — a later reservation for the same key must not be dropped.
   *
   * The cache keeps a single marker per key, so this cannot restore an *earlier*
   * suppressed position: if a delivery beyond the window moved the marker and
   * then failed, forgetting it drops the marker entirely, and a reorg landing
   * within the window of a still-earlier delivery could re-alert. That needs a
   * failed out-of-window delivery followed by an in-window reorg before the
   * retry — rare, and it duplicates rather than loses — so the single-marker
   * simplicity is kept deliberately.
   */
  forget(key: string, blockNumber: number): void {
    if (this.lastAlerted.get(key) === blockNumber) this.lastAlerted.delete(key);
  }

  get size(): number {
    return this.lastAlerted.size;
  }
}
