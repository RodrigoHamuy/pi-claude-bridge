// Isolated compression fork (billion-context-pi #614): runs a pi session's last
// served request again in a throwaway Claude Code session with one extra prompt,
// and returns the arguments of the model's first call to one tool.
//
// - Never routed through streamSimple: replayed tool results would match the
//   main query (contextForToolResults) and steer the prompt into it.
// - Nothing executes: the fork refuses to run where external tools could load,
//   and its own tool server refuses every call.
// - The fork session is deleted only once its CC process has exited.
// - pi.events is a synchronous emitter, so the instance that served the session
//   accepts in the emit tick or there is no fork.

import type { Context, Model, SimpleStreamOptions } from "@earendil-works/pi-ai";

export const ISOLATED_FORK_CHANNEL = "claude-bridge:isolated-fork";

export interface ForkUsage {
	input: number;
	output: number;
	cacheRead: number;
	cacheWrite: number;
}

export type ForkFailure = "no-capture-tool" | "no-capture" | "unsafe-config" | "unsupported-context" | "aborted" | "error";

export type ForkResult =
	| { ok: true; args: Record<string, unknown>; usage: ForkUsage }
	| { ok: false; reason: ForkFailure; usage?: ForkUsage };

export interface ForkRequest {
	version: 1;
	piSessionId: string;
	prompt: string;
	captureTool: string;
	signal: AbortSignal;
	/** `false` means another acceptor was taken first. */
	accept(result: Promise<ForkResult>): unknown;
}

export interface ServedRequest {
	readonly seq: number;
	readonly piSessionId: string;
	readonly model: Model<any>;
	readonly reasoning: SimpleStreamOptions["reasoning"];
	readonly cwd: string;
	readonly context: Context;
}

/** Thrown by fork deps to decline without counting as an error. */
export class ForkRefused extends Error {
	constructor(readonly reason: ForkFailure) {
		super(reason);
		this.name = "ForkRefused";
	}
}

export function parseForkRequest(data: unknown): ForkRequest | undefined {
	if (!data || typeof data !== "object") return undefined;
	const r = data as Record<string, unknown>;
	if (r.version !== 1) return undefined;
	if (typeof r.piSessionId !== "string" || !r.piSessionId) return undefined;
	if (typeof r.prompt !== "string" || !r.prompt) return undefined;
	if (typeof r.captureTool !== "string" || !r.captureTool) return undefined;
	if (!(r.signal instanceof AbortSignal)) return undefined;
	if (typeof r.accept !== "function") return undefined;
	return r as unknown as ForkRequest;
}

/** A private deep copy of the last provider call each pi session made. */
export class ServedRequests {
	private readonly bySession = new Map<string, ServedRequest>();
	private seq = 0;

	/** False when the request could not be copied; the session then has no record. */
	record(piSessionId: string | null | undefined, model: Model<any>, context: Context, reasoning: SimpleStreamOptions["reasoning"], cwd: string): boolean {
		if (!piSessionId) return true;
		try {
			const copy = structuredClone({
				model,
				reasoning,
				context: {
					systemPrompt: context.systemPrompt,
					messages: context.messages,
					...(context.tools ? { tools: context.tools } : {}),
				},
			});
			this.bySession.set(piSessionId, { seq: ++this.seq, piSessionId, cwd, ...copy });
			return true;
		} catch {
			this.bySession.delete(piSessionId);
			return false;
		}
	}

	get(piSessionId: string): ServedRequest | undefined {
		return this.bySession.get(piSessionId);
	}

	drop(piSessionId: string | null | undefined): void {
		if (piSessionId) this.bySession.delete(piSessionId);
	}

	clear(): void {
		this.bySession.clear();
	}
}

/** A CC message as far as the fork reads it. */
export interface ForkStreamMessage {
	type: string;
	message?: unknown;
}

export interface ForkQuery extends AsyncIterable<ForkStreamMessage> {
	close(): void;
}

/** The fork's CC process, as seen by its spawner. */
export interface ForkProcess {
	/** Resolves once the process has exited, or once it can no longer be started after `close`.
	 *  The SDK's own close() returns before that, while CC may still write the session. */
	readonly exited: Promise<void>;
	close(): void;
	/** SIGKILL now, for a shutdown that cannot wait for `close`'s own timer. */
	kill(): void;
}

export interface ForkDeps {
	/** Why this request must not fork, checked before anything is written. */
	refusal(served: ServedRequest): ForkFailure | undefined;
	/** Writes the served history into a new CC session and returns its id. May throw ForkRefused. */
	createSession(served: ServedRequest): string;
	/** Starts the fork query on `sessionId` with the main query's options. */
	startQuery(served: ServedRequest, sessionId: string, prompt: string, abortController: AbortController): { query: ForkQuery; process: ForkProcess };
	/** The SDK-side name CC uses for a pi tool. */
	sdkToolName(piToolName: string): string;
	deleteSession(sessionId: string, cwd: string): void;
	debug(...args: unknown[]): void;
}

function usageOf(usage: unknown): ForkUsage | undefined {
	if (!usage || typeof usage !== "object") return undefined;
	const raw = usage as Record<string, number | undefined>;
	return {
		input: raw.input_tokens ?? 0,
		output: raw.output_tokens ?? 0,
		cacheRead: raw.cache_read_input_tokens ?? 0,
		cacheWrite: raw.cache_creation_input_tokens ?? 0,
	};
}

function firstToolUse(content: unknown, sdkName: string): Record<string, unknown> | undefined {
	if (!Array.isArray(content)) return undefined;
	for (const block of content) {
		if (!block || typeof block !== "object") continue;
		const b = block as { type?: unknown; name?: unknown; input?: unknown };
		if (b.type !== "tool_use" || b.name !== sdkName) continue;
		return b.input && typeof b.input === "object" && !Array.isArray(b.input) ? b.input as Record<string, unknown> : {};
	}
	return undefined;
}

function errorKind(error: unknown): string {
	return error instanceof Error ? error.name : typeof error;
}

/** Owns every fork this bridge instance started, so shutdown can stop them. */
export class IsolatedForks {
	private readonly running = new Set<() => void>();
	private readonly runs = new Set<Promise<unknown>>();
	private generation = 0;
	private readonly settling = new Map<Promise<void>, () => void>();
	private readonly handled = new WeakSet<object>();
	readonly unsettled = new Set<string>();

	constructor(private readonly served: ServedRequests, private readonly deps: ForkDeps) {}

	/** pi.events handler. Accepts only for sessions this instance served. */
	handle(data: unknown): void {
		const request = parseForkRequest(data);
		if (!request || this.handled.has(request)) return;
		const served = this.served.get(request.piSessionId);
		if (!served) return;
		this.handled.add(request);
		let start!: () => void;
		const go = new Promise<void>((resolve) => { start = resolve; });
		const generation = this.generation;
		const result = go.then(() => (generation === this.generation ? this.run(request, served) : { ok: false as const, reason: "aborted" as const }));
		if (request.accept(result) === false) return;
		this.runs.add(result);
		const forget = () => { this.runs.delete(result); };
		result.then(forget, forget);
		start();
	}

	/** Stops running forks, and accepted ones that have not started yet. */
	abortAll(): void {
		this.generation++;
		for (const abort of this.running) abort();
	}

	/** Stops every fork and waits, at most `deadlineMs`, for each process to exit
	 *  and its session to be deleted. Processes still running at `killAfterMs`
	 *  get SIGKILL. A session whose process never exits is left on disk. */
	async shutdown(deadlineMs: number, killAfterMs: number): Promise<void> {
		this.abortAll();
		const start = Date.now();
		const within = async (promises: Promise<unknown>[], ms: number) => {
			let timer: ReturnType<typeof setTimeout> | undefined;
			await Promise.race([
				Promise.allSettled(promises),
				new Promise<void>((resolve) => { timer = setTimeout(resolve, Math.max(0, ms)); }),
			]);
			clearTimeout(timer);
		};
		await within([...this.runs], killAfterMs);
		await within([...this.settling.keys()], killAfterMs - (Date.now() - start));
		for (const kill of this.settling.values()) kill();
		await within([...this.runs, ...this.settling.keys()], deadlineMs - (Date.now() - start));
		if (this.unsettled.size > 0) this.deps.debug(`isolated-fork: shutdown left ${this.unsettled.size} session(s) whose process did not exit`);
	}

	private async run(request: ForkRequest, served: ServedRequest): Promise<ForkResult> {
		if (request.signal.aborted) return { ok: false, reason: "aborted" };
		if (!served.context.tools?.some((tool) => tool.name === request.captureTool)) {
			return { ok: false, reason: "no-capture-tool" };
		}
		const refusal = this.deps.refusal(served);
		if (refusal) return { ok: false, reason: refusal };

		const controller = new AbortController();
		let wake!: () => void;
		const stopped = new Promise<void>((resolve) => { wake = resolve; });
		const abort = () => {
			controller.abort();
			wake();
		};
		request.signal.addEventListener("abort", abort, { once: true });
		this.running.add(abort);
		let sessionId: string | undefined;
		let started: ReturnType<ForkDeps["startQuery"]> | undefined;
		let consumed: Promise<void> | undefined;
		let usage: ForkUsage | undefined;
		let args: Record<string, unknown> | undefined;
		const withUsage = () => (usage ? { usage } : {});
		try {
			sessionId = this.deps.createSession(served);
			if (controller.signal.aborted) return { ok: false, reason: "aborted" };
			const sdkName = this.deps.sdkToolName(request.captureTool);
			started = this.deps.startQuery(served, sessionId, request.prompt, controller);
			const q = started.query;
			consumed = (async () => {
				for await (const message of q) {
					if (controller.signal.aborted) return;
					if (message.type !== "assistant") continue;
					const body = message.message && typeof message.message === "object" ? message.message as { content?: unknown; usage?: unknown } : {};
					usage ??= usageOf(body.usage);
					const captured = firstToolUse(body.content, sdkName);
					if (captured) {
						args = captured;
						return;
					}
				}
			})();
			await Promise.race([consumed, stopped]);
			if (args) return { ok: true, args, usage: usage ?? { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } };
			return { ok: false, reason: controller.signal.aborted ? "aborted" : "no-capture", ...withUsage() };
		} catch (error) {
			if (error instanceof ForkRefused) return { ok: false, reason: error.reason, ...withUsage() };
			this.deps.debug(`isolated-fork: failed (${errorKind(error)})`);
			return { ok: false, reason: controller.signal.aborted ? "aborted" : "error", ...withUsage() };
		} finally {
			// The result is known, so stop the process instead of waiting for it.
			controller.abort();
			request.signal.removeEventListener("abort", abort);
			this.running.delete(abort);
			this.cleanup(served.cwd, sessionId, started);
		}
	}

	private cleanup(cwd: string, sessionId: string | undefined, started: ReturnType<ForkDeps["startQuery"]> | undefined): void {
		try {
			started?.query.close();
		} catch {}
		try {
			started?.process.close();
		} catch {}
		if (!sessionId) return;
		const id = sessionId;
		this.unsettled.add(id);
		const remove = () => {
			try {
				this.deps.deleteSession(id, cwd);
			} catch (error) {
				this.deps.debug(`isolated-fork: delete ${id.slice(0, 8)} failed (${errorKind(error)})`);
			}
			this.unsettled.delete(id);
		};
		if (!started) {
			remove();
			return;
		}
		const child = started.process;
		// Only the process writes the session, so its exit is what deletion waits for.
		const settled = child.exited.then(remove);
		this.settling.set(settled, () => child.kill());
		const forget = () => { this.settling.delete(settled); };
		settled.then(forget, forget);
	}
}
