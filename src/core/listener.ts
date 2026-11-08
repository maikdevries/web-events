interface Event {
	'data': string;
	'type': string;
}

type Handler = (event: MessageEvent<Event['data']>) => void;

interface Options {
	'id': string;
	'reconnect': number;
}

export class Listener {
	#controller: AbortController = new AbortController();
	#handlers: Map<Event['type'], Handler> = new Map();
	#status: 'IDLE' | 'CONNECTING' | 'OPEN' | 'CLOSED' = 'IDLE';
	#stream: ReadableStream<string> | null = null;

	#options: Options;
	#url: URL;

	constructor(url: URL, options: Partial<Options> = {}) {
		this.#options = {
			'id': '',
			'reconnect': 15 * 1000,
			...options,
		};

		this.#url = url;

		void this.#connect();
	}

	get status(): 'IDLE' | 'CONNECTING' | 'OPEN' | 'CLOSED' {
		return this.#status;
	}

	get url(): URL {
		return this.#url;
	}

	async #connect(): Promise<void> {
		if (this.status === 'IDLE') this.#status = 'CONNECTING';
		else return;

		let response;
		try {
			response = await self.fetch(this.#url, {
				'cache': 'no-store',
				'headers': {
					'Accept': 'text/event-stream',
					...(this.#options.id ? { 'Last-Event-ID': this.#options.id } : {}),
				},
				'redirect': 'follow',
				'signal': this.#controller.signal,
			});
		} catch (error: unknown) {
			if (error instanceof DOMException && error.name === 'AbortError') return this.close();
			else return this.#reconnect();
		}

		// [TODO] Aborted network error -> fail connection
		// [TODO] Network error -> re-establish connection

		if (response.status === 204) return this.close();
		else if (response.status !== 200 || response.headers.get('Content-Type') !== 'text/event-stream') {
			return this.close({ 'notify': true });
		}

		this.#stream = response.body?.pipeThrough(new TextDecoderStream()) ?? null;
		if (!this.#stream) return this.#reconnect();

		// [FIX] Events without any data are dropped by default
		this.#status = 'OPEN';
		this.#emit({ 'type': 'open', 'data': '' });

		return this.#consume();
	}

	async close(options: { 'notify': boolean } = { 'notify': false }): Promise<void> {
		if (this.status === 'CLOSED') return;

		this.#controller.abort();
		await this.#stream?.cancel();

		// [FIX] Events without any data are dropped by default
		this.#status = 'CLOSED';
		if (options.notify) this.#emit({ 'type': 'error', 'data': '' });
	}

	async #reconnect(): Promise<void> {
		if (this.status !== 'CLOSED') this.#status = 'IDLE';
		else return;

		// [FIX] Events without any data are dropped by default
		this.#emit({ 'type': 'error', 'data': '' });

		await new Promise((r) => setTimeout(r, this.#options.reconnect));
		return this.#connect();
	}

	async #consume(): Promise<void> {
		if (!this.#stream?.locked) return;

		let buffer = '';
		for await (const chunk of this.#stream) {
			buffer += chunk;

			const frames = buffer.split('\n\n');
			buffer = frames.pop() ?? '';

			for (const frame of frames) this.#parse(frame);
		}

		return this.#reconnect();
	}

	#parse(frame: string): void {
		const event = {
			'data': '',
			'type': 'message',
		};

		for (const line of frame.split('\n')) {
			if (!line.length) return this.#emit(event);

			if (line.startsWith(':')) continue;

			const index = line.includes(':') ? line.indexOf(':') : line.length;
			const [field, value] = [line.slice(0, index), line.slice(index + (line.at(index + 1) === ' ' ? 2 : 1))];

			switch (field) {
				case 'event':
					event.type = value;
					break;
				case 'data':
					event.data += value + '\n';
					break;
				case 'id':
					this.#options.id = value;
					break;
				case 'retry':
					if (/^[0-9]+$/.test(value)) this.#options.reconnect = Number(value);
					break;
			}
		}
	}

	#emit(event: Event): void {
		if (!event.data.length) return;

		if (event.data.endsWith('\n')) event.data = event.data.slice(0, event.data.length - '\n'.length);

		const ev = new MessageEvent(event.type, {
			'data': event.data,
			'lastEventId': this.#options.id,
			'origin': this.url.origin,
		});

		return this.#handlers.get(ev.type)?.(ev);
	}

	register(type: Event['type'], handler: Handler): void {
		this.#handlers.set(type, handler);
	}
}
