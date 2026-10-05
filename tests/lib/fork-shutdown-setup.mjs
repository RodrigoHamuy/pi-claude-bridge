// Activates the extension with a recording pi stub and starts an isolated fork
// whose Claude Code process is a real node child. Shared by the in-process and
// SIGTERM-subprocess shutdown tests (tests/unit-fork-shutdown.mjs).
//
// CLAUDE_CONFIG_DIR must point at a throwaway dir before this is imported.
import { appendFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { createEventBus } from "@earendil-works/pi-coding-agent";
import { openSession } from "cc-session-io";

const mod = await import("../../src/index.js");
const { __test } = mod;
const { ISOLATED_FORK_CHANNEL } = await import("../../src/isolated-fork.js");

export const handlers = new Map();
const bus = createEventBus();
let providerConfig;
mod.default({
	on: (event, handler) => handlers.set(event, handler),
	registerProvider: (_name, config) => { providerConfig = config; },
	events: bus,
	registerTool: () => {},
});
const model = providerConfig.models[0];
export const cwd = process.cwd();

const tools = [{ name: "compress", description: "Compress a range", parameters: { type: "object", properties: {} } }];
const scripts = [];
__test.setQuery(({ options }) => {
	const script = scripts.shift();
	if (!script) throw new Error("no fake script queued");
	script.onStart?.(options);
	const gen = (async function* () {
		for (const step of script.steps) {
			if (typeof step === "function") { await step(options); continue; }
			yield step.type === "system" && options.resume ? { ...step, session_id: options.resume } : step;
		}
	})();
	gen.interrupt = async () => {};
	gen.close = () => {};
	return gen;
});

export const sessionExists = (sessionId) => {
	try { openSession({ sessionId, projectPath: cwd, claudeDir: process.env.CLAUDE_CONFIG_DIR }); return true; } catch { return false; }
};

/** Starts a main turn, then a fork whose CC process runs `childSource` and
 *  stays alive until killed. Resolves once the child has printed "ready". */
export async function startForkWithChild(childSource) {
	let clock = 0;
	const prompt = "[m00002] next";
	const history = [
		{ role: "user", content: "[m00001] hello", timestamp: clock++ },
		{ role: "assistant", content: [{ type: "text", text: "hi" }], api: "claude-bridge", provider: "claude-bridge", model: model.id,
			usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } }, stopReason: "stop", timestamp: clock++ },
		{ role: "user", content: prompt, timestamp: clock++ },
	];
	let releaseMain;
	const mainHeld = new Promise((r) => { releaseMain = r; });
	scripts.push({
		onStart: (options) => {
			const session = openSession({ sessionId: options.resume, projectPath: cwd, claudeDir: process.env.CLAUDE_CONFIG_DIR });
			const last = session.records.at(-1);
			appendFileSync(session.jsonlPath, JSON.stringify({ uuid: randomUUID(), parentUuid: last?.uuid ?? null, sessionId: options.resume, timestamp: new Date().toISOString(), type: "user", message: { role: "user", content: prompt } }) + "\n");
		},
		steps: [{ type: "system", subtype: "init" }, () => mainHeld, { type: "result", subtype: "success", is_error: false, result: "ok" }],
	});
	const main = providerConfig.streamSimple(model, { messages: history, tools }, { sessionId: "pi-shutdown" });

	let child;
	let ready;
	const childReady = new Promise((r) => { ready = r; });
	let forkId;
	scripts.push({
		onStart: (options) => {
			forkId = options.resume;
			child = options.spawnClaudeCodeProcess({ command: process.execPath, args: ["-e", childSource], cwd, env: process.env, signal: options.abortController.signal });
			child.stdout.once("data", () => ready());
		},
		steps: [{ type: "system", subtype: "init" }, (options) => new Promise((r) => options.abortController.signal.addEventListener("abort", r, { once: true }))],
	});
	for (let i = 0; i < 1000 && !__test.servedRequests.get("pi-shutdown"); i++) await new Promise((r) => setImmediate(r));
	let accepted;
	bus.emit(ISOLATED_FORK_CHANNEL, {
		version: 1, piSessionId: "pi-shutdown", prompt: "NUDGE", captureTool: "compress", signal: new AbortController().signal,
		accept: (result) => { accepted = Promise.resolve(result); accepted.catch(() => {}); return true; },
	});
	if (!accepted) throw new Error("fork was not accepted");
	await childReady;
	return { child, forkId, accepted, finishMain: async () => { releaseMain(); await main.result(); } };
}
