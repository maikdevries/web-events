export interface Event {
	'data': JSON;
	'type': string;
}

type JSON = string | number | boolean | null | JSON[] | { [key: string]: JSON } | { toJSON(): JSON };

export class Emitter {
	#controller: ReadableStreamDefaultController<string> | null = null;
	#heartbeat: number | null = null;
	#stream: ReadableStream<Uint8Array>;

	constructor() {
		this.#stream = new ReadableStream({
			'start': (controller) => {
				this.#controller = controller;
				this.#heartbeat = setInterval(() => this.#write(':'), 30000);

				this.#write(':');
			},
			'cancel': this.close.bind(this, { 'notify': false }),
		}).pipeThrough(new TextEncoderStream());
	}

	get connected(): boolean {
		return this.#controller !== null;
	}

	get response(): Response {
		if (this.#stream.locked) throw new Error();

		return new Response(this.#stream, {
			'headers': {
				'Cache-Control': 'no-cache',
				'Content-Type': 'text/event-stream',
				'X-Accel-Buffering': 'no',
			},
		});
	}

	close(options: { 'notify': boolean } = { 'notify': true }): void {
		if (!this.connected) return;

		try {
			if (options.notify) this.send({ 'data': 'server closed connection', 'type': 'close' });
			this.#controller?.close();
		} catch (_: unknown) {}

		if (this.#heartbeat) clearInterval(this.#heartbeat);
		this.#controller = this.#heartbeat = null;
	}

	send(event: Event): void {
		const message = `event: ${event.type}\ndata: ${
			typeof event.data === 'string' ? event.data : JSON.stringify(event.data)
		}`;

		return this.#write(message);
	}

	#write(message: string): void {
		if (!this.connected) throw new Error();
		this.#controller?.enqueue(message + '\n\n');
	}
}
