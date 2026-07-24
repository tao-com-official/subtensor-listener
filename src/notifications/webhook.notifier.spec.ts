import { WebhookNotifier } from './webhook.notifier';

/** A `fetch` rejection shaped like Node's: a TypeError wrapping the real code. */
const fetchFailure = (code?: string): TypeError => {
  const err = new TypeError('fetch failed');
  const cause = new Error('boom');
  if (code) Object.assign(cause, { code });
  Object.assign(err, { cause });
  return err;
};

describe('WebhookNotifier', () => {
  afterEach(() => jest.restoreAllMocks());

  it('posts the message under the configured field and reports delivered on 2xx', async () => {
    const fetchSpy = jest
      .spyOn(globalThis, 'fetch')
      .mockResolvedValue(new Response('{"ok":true}', { status: 200 }));
    const notifier = new WebhookNotifier();

    const result = await notifier.send(
      { url: 'https://hooks.example/triggers/x', field: 'text' },
      'hello',
    );

    expect(result.status).toBe('delivered');
    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const [, init] = fetchSpy.mock.calls[0] as [string, RequestInit];
    expect(init.body).toBe(JSON.stringify({ text: 'hello' }));
  });

  it('defaults the field to "text"', async () => {
    const fetchSpy = jest
      .spyOn(globalThis, 'fetch')
      .mockResolvedValue(new Response('{}', { status: 200 }));
    const notifier = new WebhookNotifier();

    await notifier.send({ url: 'https://hooks.example/x' }, 'hi');

    const [, init] = fetchSpy.mock.calls[0] as [string, RequestInit];
    expect(init.body).toBe(JSON.stringify({ text: 'hi' }));
  });

  it('reports a refused connection as rejected — nothing was posted, so a retry is safe', async () => {
    jest
      .spyOn(globalThis, 'fetch')
      .mockRejectedValue(fetchFailure('ECONNREFUSED'));
    const notifier = new WebhookNotifier();

    const result = await notifier.send(
      { url: 'https://hooks.example/x' },
      'hi',
    );

    expect(result.status).toBe('rejected');
  });

  it('reports a 4xx as rejected', async () => {
    jest
      .spyOn(globalThis, 'fetch')
      .mockResolvedValue(new Response('bad', { status: 400 }));
    const notifier = new WebhookNotifier();

    expect(
      (await notifier.send({ url: 'https://hooks.example/x' }, 'hi')).status,
    ).toBe('rejected');
  });

  it('reports a 5xx as unknown — the message may already have been posted', async () => {
    jest
      .spyOn(globalThis, 'fetch')
      .mockResolvedValue(new Response('oops', { status: 502 }));
    const notifier = new WebhookNotifier();

    const result = await notifier.send(
      { url: 'https://hooks.example/x' },
      'hi',
    );

    expect(result.status).toBe('unknown');
    expect(result.reason).toBe('HTTP 502');
  });

  it('reports a socket dying mid-flight as unknown', async () => {
    // The request was already on the wire; the server may well have served it.
    jest
      .spyOn(globalThis, 'fetch')
      .mockRejectedValue(fetchFailure('ECONNRESET'));
    const notifier = new WebhookNotifier();

    expect(
      (await notifier.send({ url: 'https://hooks.example/x' }, 'hi')).status,
    ).toBe('unknown');
  });

  it('reports an uncoded failure as unknown rather than assuming nothing was sent', async () => {
    jest.spyOn(globalThis, 'fetch').mockRejectedValue(fetchFailure());
    const notifier = new WebhookNotifier();

    expect(
      (await notifier.send({ url: 'https://hooks.example/x' }, 'hi')).status,
    ).toBe('unknown');
  });

  it('retries once on HTTP 429 then succeeds', async () => {
    const fetchSpy = jest
      .spyOn(globalThis, 'fetch')
      .mockResolvedValueOnce(
        new Response('rate', { status: 429, headers: { 'retry-after': '0' } }),
      )
      .mockResolvedValueOnce(new Response('{}', { status: 200 }));
    const notifier = new WebhookNotifier();

    const result = await notifier.send(
      { url: 'https://hooks.example/x' },
      'hi',
    );

    expect(result.status).toBe('delivered');
    expect(fetchSpy).toHaveBeenCalledTimes(2);
  });

  it('reports a still-rate-limited retry as rejected, not unknown', async () => {
    const fetchSpy = jest
      .spyOn(globalThis, 'fetch')
      .mockResolvedValue(
        new Response('rate', { status: 429, headers: { 'retry-after': '0' } }),
      );
    const notifier = new WebhookNotifier();

    const result = await notifier.send(
      { url: 'https://hooks.example/x' },
      'hi',
    );

    // A 429 means the request was not processed, so the caller may retry it.
    expect(result.status).toBe('rejected');
    expect(fetchSpy).toHaveBeenCalledTimes(2);
  });
});
