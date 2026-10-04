interface Event {
	'data': string;
	'type': string;
}

export class Listener {
	#stream: ReadableStream<string> | null = null;
	#url: URL;

	constructor(url: URL) {
		this.#url = url;

		void this.#connect();
	}

	async #connect(): Promise<void> {
		let response;
		try {
			response = await self.fetch(this.#url, {
				'cache': 'no-store',
				'headers': {
					'Accept': 'text/event-stream',
				},
			});
		} catch (error: unknown) {
			// [TODO] Network error -> re-establish connection
		}

		// [TODO] Aborted network error -> fail connection
		// [TODO] Network error -> re-establish connection

		if (response?.status !== 200 || response.headers.get('Content-Type') !== 'text/event-stream') {
			// [TODO] Response is invalid -> fail connection
		}

		this.#stream = response?.body?.pipeThrough(new TextDecoderStream()) ?? null;
		// [TODO] Response has no body -> re-establish connection

		// [TODO] Response is valid -> announce connection
		return this.#consume();
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
			}
		}
	}

	#emit(event: Event): void {
		if (!event.data.length) return;

		if (event.data.endsWith('\n')) event.data = event.data.slice(0, event.data.length - '\n'.length);

		const ev = new MessageEvent(event.type, {
			'data': event.data,
		});

		// [TODO] Call respective registered event handler
	}
}
