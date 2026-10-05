/**
 * Unit tests for the isolated compression fork (billion-context-pi #614).
 *
 * A compression extension asks the bridge, over pi.events, to run the last
 * request a pi session served once more with an extra prompt, capture the
 * arguments of the model's first call to one tool, and execute nothing. The
 * failure modes are all silent: a fork built from a request mutated after it was
 * served, a fork whose tools or system prompt differ from the main query's, a
 * fork that runs where external tools could load, a fork missing an @file the
 * main session has, a fork session deleted while CC still writes it, or a fork
 * that reaches the main session's mirror or query state. These pin each of them.
 */
import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { getEventListeners } from "node:events";
import { randomUUID } from "node:crypto";
import { appendFileSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { createEventBus } from "@earendil-works/pi-coding-agent";
import { createSession, openSession, repairToolPairing } from "cc-session-io";
import { ForkRefused, IsolatedForks, ISOLATED_FORK_CHANNEL, ServedRequests, parseForkRequest } from "../src/isolated-fork.js";

const claudeDir = mkdtempSync(join(tmpdir(), "claude-bridge-isolated-fork-cc-"));
process.env.CLAUDE_CONFIG_DIR = claudeDir;
process.on("exit", () => rmSync(claudeDir, { recursive: true, force: true }));

const settle = () => new Promise((r) => setTimeout(r, 20));
const gate = () => { let open; const p = new Promise((r) => { open = r; }); return { wait: () => p, open }; };

function request(overrides = {}) {
	const accepted = [];
	const controller = new AbortController();
	return {
		accepted,
		controller,
		data: {
			version: 1,
			piSessionId: "pi-a",
			prompt: "NUDGE",
			captureTool: "compress",
			signal: controller.signal,
			accept: (result) => { accepted.push(result); return accepted.length === 1; },
			...overrides,
		},
	};
}

const compressTool = { name: "compress", description: "Compress", parameters: { type: "object", properties: {} } };
const readTool = { name: "read", description: "Read", parameters: { type: "object", properties: { path: { type: "string" } } } };
const ctxWith = (text, tools = [readTool, compressTool]) => ({ systemPrompt: undefined, messages: [{ role: "user", content: text, timestamp: 0 }], tools });

function assistant(content, usage = { input_tokens: 10, output_tokens: 2, cache_read_input_tokens: 7, cache_creation_input_tokens: 1 }) {
	return { type: "assistant", message: { content, usage } };
}

function fakeDeps(script = () => [], extra = {}) {
	const log = { created: [], started: [], deleted: [], closed: 0, processClosed: 0, debug: [] };
	const deps = {
		refusal: () => undefined,
		createSession(served) {
			const id = `fork-${log.created.length + 1}`;
			log.created.push({ id, served });
			return id;
		},
		startQuery(served, sessionId, prompt, abortController) {
			log.started.push({ served, sessionId, prompt, abortController });
			const steps = script(abortController);
			const gen = (async function* () {
				for (const step of steps) {
					if (typeof step === "function") { await step(); continue; }
					yield step;
				}
			})();
			gen.close = () => { log.closed++; };
			return { query: gen, process: { exited: extra.exited ?? Promise.resolve(), close: () => { log.processClosed++; } } };
		},
		sdkToolName: (name) => `mcp__custom-tools__${name}`,
		deleteSession(id) { log.deleted.push(id); },
		debug: (...args) => log.debug.push(args.join(" ")),
		...extra,
	};
	return { deps, log };
}

const COMPRESS_CALL = assistant([{ type: "tool_use", name: "mcp__custom-tools__compress", input: { startId: "m1" } }]);

describe("parseForkRequest", () => {
	it("accepts only the v1 shape", () => {
		assert.ok(parseForkRequest(request().data));
		for (const bad of [
			null, "x", { ...request().data, version: 2 }, { ...request().data, piSessionId: "" },
			{ ...request().data, prompt: 1 }, { ...request().data, captureTool: "" },
			{ ...request().data, signal: {} }, { ...request().data, accept: undefined },
		]) assert.equal(parseForkRequest(bad), undefined);
	});
});

describe("ServedRequests", () => {
	it("keeps a deep copy taken at provider entry, so later mutation of the request never reaches a fork", () => {
		const served = new ServedRequests();
		const model = { id: "m", thinkingLevelMap: { high: "high" } };
		const ctx = ctxWith("A1");
		assert.equal(served.record("pi-a", model, ctx, "high", "/cwd"), true);
		ctx.messages[0].content = "mutated";
		ctx.messages.push({ role: "user", content: "appended", timestamp: 1 });
		ctx.tools[0].parameters.properties.path.type = "number";
		model.thinkingLevelMap.high = "low";
		const copy = served.get("pi-a");
		assert.deepEqual(copy.context.messages, [{ role: "user", content: "A1", timestamp: 0 }]);
		assert.equal(copy.context.tools[0].parameters.properties.path.type, "string");
		assert.equal(copy.model.thinkingLevelMap.high, "high");
		assert.equal(copy.piSessionId, "pi-a");
	});

	it("drops the previous record when a request cannot be copied, rather than forking a stale one", () => {
		const served = new ServedRequests();
		served.record("pi-a", { id: "m" }, ctxWith("old"), undefined, "/cwd");
		assert.equal(served.record("pi-a", { id: "m", hook: () => {} }, ctxWith("new"), undefined, "/cwd"), false);
		assert.equal(served.get("pi-a"), undefined);
	});
});

describe("IsolatedForks", () => {
	it("does not accept a session this instance never served, or a malformed request", () => {
		const { deps, log } = fakeDeps();
		const forks = new IsolatedForks(new ServedRequests(), deps);
		const r = request();
		forks.handle(r.data);
		forks.handle({ ...r.data, version: 2 });
		assert.equal(r.accepted.length, 0);
		assert.equal(log.created.length, 0);
	});

	it("accepts once even when the handler is registered twice", async () => {
		const served = new ServedRequests();
		served.record("pi-a", { id: "m" }, ctxWith("a"), undefined, "/cwd");
		const { deps, log } = fakeDeps(() => [COMPRESS_CALL]);
		const forks = new IsolatedForks(served, deps);
		const r = request();
		forks.handle(r.data);
		forks.handle(r.data);
		assert.equal(r.accepted.length, 1);
		await r.accepted[0];
		assert.equal(log.created.length, 1);
	});

	it("starts no work when its acceptance is not the one taken", async () => {
		const served = new ServedRequests();
		served.record("pi-a", { id: "m" }, ctxWith("a"), undefined, "/cwd");
		const { deps, log } = fakeDeps(() => [COMPRESS_CALL]);
		const forks = new IsolatedForks(served, deps);
		forks.handle(request({ accept: () => false }).data);
		await settle();
		assert.equal(log.created.length, 0);
		assert.equal(log.started.length, 0);
	});

	it("forks the request served before the accept, not a later one or another session's", async () => {
		const served = new ServedRequests();
		served.record("pi-a", { id: "m" }, ctxWith("A1"), "high", "/cwd");
		served.record("pi-b", { id: "m" }, ctxWith("B1"), undefined, "/cwd");
		const hold = gate();
		const { deps, log } = fakeDeps(() => [hold.wait, COMPRESS_CALL]);
		const forks = new IsolatedForks(served, deps);
		const r = request();
		forks.handle(r.data);
		served.record("pi-a", { id: "m" }, ctxWith("A2"), "low", "/cwd");
		hold.open();
		const result = await r.accepted[0];
		assert.equal(result.ok, true);
		assert.equal(log.created[0].served.context.messages[0].content, "A1");
		assert.equal(log.created[0].served.reasoning, "high");
		assert.equal(served.get("pi-a").context.messages[0].content, "A2");
	});

	it("captures the first capture-tool call and its usage, and leaves no abort listener behind", async () => {
		const served = new ServedRequests();
		served.record("pi-a", { id: "m" }, ctxWith("a"), undefined, "/cwd");
		const { deps, log } = fakeDeps(() => [
			{ type: "system", subtype: "init" },
			assistant([
				{ type: "tool_use", name: "mcp__custom-tools__read", input: { path: "x" } },
				{ type: "tool_use", name: "mcp__custom-tools__compress", input: { startId: "m1", endId: "m2", summary: "s" } },
				{ type: "tool_use", name: "mcp__custom-tools__compress", input: { startId: "m9" } },
			]),
		]);
		const forks = new IsolatedForks(served, deps);
		const r = request();
		forks.handle(r.data);
		const result = await r.accepted[0];
		assert.deepEqual(result, { ok: true, args: { startId: "m1", endId: "m2", summary: "s" }, usage: { input: 10, output: 2, cacheRead: 7, cacheWrite: 1 } });
		assert.equal(log.started[0].prompt, "NUDGE");
		assert.equal(log.closed, 1);
		assert.equal(log.processClosed, 1);
		assert.equal(getEventListeners(r.controller.signal, "abort").length, 0);
		await settle();
		assert.deepEqual(log.deleted, ["fork-1"]);
	});

	it("returns the capture at once but deletes the session only after the CC process exits", async () => {
		const served = new ServedRequests();
		served.record("pi-a", { id: "m" }, ctxWith("a"), undefined, "/cwd");
		const exit = gate();
		const { deps, log } = fakeDeps(() => [COMPRESS_CALL], { exited: exit.wait() });
		const forks = new IsolatedForks(served, deps);
		const r = request();
		forks.handle(r.data);
		const result = await r.accepted[0];
		assert.equal(result.ok, true);
		assert.equal(log.closed, 1, "the reader ended and the query was closed");
		await settle();
		assert.deepEqual(log.deleted, [], "the reader ending is not proof the process stopped writing");
		assert.deepEqual([...forks.unsettled], ["fork-1"]);
		exit.open();
		await settle();
		assert.deepEqual(log.deleted, ["fork-1"]);
		assert.equal(forks.unsettled.size, 0);
	});

	it("reports no-capture when the model never calls the capture tool", async () => {
		const served = new ServedRequests();
		served.record("pi-a", { id: "m" }, ctxWith("a"), undefined, "/cwd");
		const { deps } = fakeDeps(() => [assistant([{ type: "text", text: "no" }])]);
		const forks = new IsolatedForks(served, deps);
		const r = request();
		forks.handle(r.data);
		const result = await r.accepted[0];
		assert.equal(result.ok, false);
		assert.equal(result.reason, "no-capture");
	});

	it("refuses before writing anything: missing capture tool, aborted signal, or an unsafe configuration", async () => {
		const served = new ServedRequests();
		served.record("pi-a", { id: "m" }, ctxWith("a", [readTool]), undefined, "/cwd");
		const { deps, log } = fakeDeps();
		const forks = new IsolatedForks(served, deps);
		const missing = request();
		forks.handle(missing.data);
		assert.deepEqual(await missing.accepted[0], { ok: false, reason: "no-capture-tool" });
		served.record("pi-a", { id: "m" }, ctxWith("a"), undefined, "/cwd");
		const aborted = request();
		aborted.controller.abort();
		forks.handle(aborted.data);
		assert.deepEqual(await aborted.accepted[0], { ok: false, reason: "aborted" });
		const unsafe = new IsolatedForks(served, { ...deps, refusal: () => "unsafe-config" });
		const r = request();
		unsafe.handle(r.data);
		assert.deepEqual(await r.accepted[0], { ok: false, reason: "unsafe-config" });
		assert.equal(log.created.length, 0);
		assert.equal(log.started.length, 0);
	});

	it("passes a refusal from session setup through as its reason, with nothing to delete", async () => {
		const served = new ServedRequests();
		served.record("pi-a", { id: "m" }, ctxWith("a"), undefined, "/cwd");
		const { deps, log } = fakeDeps(() => [], { createSession() { throw new ForkRefused("unsupported-context"); } });
		const forks = new IsolatedForks(served, deps);
		const r = request();
		forks.handle(r.data);
		assert.deepEqual(await r.accepted[0], { ok: false, reason: "unsupported-context" });
		assert.equal(log.started.length, 0);
		assert.deepEqual(log.deleted, []);
	});

	it("stops promptly on abort but keeps the session until the process exits", async () => {
		const served = new ServedRequests();
		served.record("pi-a", { id: "m" }, ctxWith("a"), undefined, "/cwd");
		const stuck = gate();
		const exit = gate();
		const { deps, log } = fakeDeps(() => [stuck.wait], { exited: exit.wait() });
		const forks = new IsolatedForks(served, deps);
		const r = request();
		forks.handle(r.data);
		await settle();
		r.controller.abort();
		assert.deepEqual(await r.accepted[0], { ok: false, reason: "aborted" });
		assert.ok(log.started[0].abortController.signal.aborted, "the request signal must reach the query");
		assert.equal(log.processClosed, 1);
		stuck.open();
		await settle();
		assert.deepEqual(log.deleted, []);
		exit.open();
		await settle();
		assert.deepEqual(log.deleted, ["fork-1"]);
	});

	it("abortAll stops running forks", async () => {
		const served = new ServedRequests();
		served.record("pi-a", { id: "m" }, ctxWith("a"), undefined, "/cwd");
		const { deps } = fakeDeps(() => [() => new Promise(() => {})]);
		const forks = new IsolatedForks(served, deps);
		const r = request();
		forks.handle(r.data);
		await settle();
		forks.abortAll();
		assert.deepEqual(await r.accepted[0], { ok: false, reason: "aborted" });
	});

	it("reports a setup failure by kind only", async () => {
		const served = new ServedRequests();
		served.record("pi-a", { id: "m" }, ctxWith("a"), undefined, "/cwd");
		const { deps, log } = fakeDeps(() => [], { createSession() { throw new Error("secret sk-ant-123 in path"); } });
		const forks = new IsolatedForks(served, deps);
		const r = request();
		forks.handle(r.data);
		assert.deepEqual(await r.accepted[0], { ok: false, reason: "error" });
		assert.ok(log.debug.every((line) => !line.includes("sk-ant")), log.debug.join("\n"));
	});
});

describe("isolated fork through the provider", async () => {
	const mod = await import("../src/index.js");
	const { __test } = mod;
	const { convertPiMessages } = await import("../src/convert.js");
	let providerConfig;
	const bus = createEventBus();
	mod.default({
		on: () => {},
		registerProvider: (_name, config) => { providerConfig = config; },
		events: bus,
		registerTool: () => {},
	});
	const streamSimple = providerConfig.streamSimple;
	const model = providerConfig.models[0];
	const cwd = process.cwd();

	// Exactly what billion-context-pi's AsyncCompressor emits (src/async-compress.ts launchBridge).
	function acpRequest(piSessionId) {
		let accepted;
		const controller = new AbortController();
		bus.emit(ISOLATED_FORK_CHANNEL, {
			version: 1,
			piSessionId,
			prompt: "NUDGE",
			captureTool: "compress",
			signal: controller.signal,
			accept: (result) => {
				if (!result || typeof result.then !== "function") return false;
				const promise = Promise.resolve(result);
				promise.catch(() => {});
				if (accepted) return false;
				accepted = promise;
				return true;
			},
		});
		return { accepted, controller };
	}

	const queries = [];
	const scripts = [];
	beforeEach(() => {
		__test.resetSharedSession();
		__test.setProviderSettings({});
		queries.length = 0;
		scripts.length = 0;
		__test.setQuery(({ options, prompt }) => {
			const script = scripts.shift();
			if (!script) throw new Error("no fake script queued");
			const entry = { label: script.label, options, prompt, closed: 0, interrupted: 0 };
			if (script.onStart) script.onStart(entry);
			queries.push(entry);
			const gen = (async function* () {
				for (const step of script.steps) {
					if (typeof step === "function") { await step(); continue; }
					yield step.type === "system" && options.resume ? { ...step, session_id: options.resume } : step;
				}
			})();
			gen.interrupt = async () => { entry.interrupted++; };
			gen.close = () => { entry.closed++; };
			return gen;
		});
	});
	afterEach(() => {
		__test.setQuery(null);
		__test.setProviderSettings({});
	});

	let clock = 0;
	const usage = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } };
	const asst = (content, stopReason = "stop") => ({ role: "assistant", content, api: "claude-bridge", provider: "claude-bridge", model: "claude-bridge/opus", usage, stopReason, timestamp: clock++ });
	const tools = [
		{ name: "read", description: "Read a file", parameters: { type: "object", properties: { path: { type: "string" } } } },
		{ name: "compress", description: "Compress a range", parameters: { type: "object", properties: { startId: { type: "string" } }, required: ["startId"] } },
	];
	const historyFor = (tag) => [
		{ role: "user", content: `[m00001] ${tag} read it`, timestamp: clock++ },
		asst([
			{ type: "thinking", thinking: "plan", thinkingSignature: "sig-verbatim-123" },
			{ type: "toolCall", id: "call_1", name: "read", arguments: { path: "\x3cacp tokens=\"2\"\x3em00002\x3c/acp\x3e.txt" } },
		], "toolUse"),
		{ role: "toolResult", toolCallId: "call_1", toolName: "read", content: [{ type: "text", text: "[m00003] body" }], isError: false, timestamp: clock++ },
		asst([{ type: "text", text: "[m00004] done" }]),
		{ role: "user", content: `[m00005] ${tag} next`, timestamp: clock++ },
	];

	const sessionRecords = (sessionId) => openSession({ sessionId, projectPath: cwd, claudeDir }).records;
	const sessionExists = (sessionId) => { try { sessionRecords(sessionId); return true; } catch { return false; } };
	const appendRecord = (sessionId, record) => {
		const session = openSession({ sessionId, projectPath: cwd, claudeDir });
		const last = session.records.at(-1);
		const uuid = randomUUID();
		appendFileSync(session.jsonlPath, JSON.stringify({ uuid, parentUuid: last?.uuid ?? null, sessionId, timestamp: new Date().toISOString(), ...record }) + "\n");
		return uuid;
	};
	// Claude Code records the prompt it was handed (the latest pi prompt) before it streams a reply.
	const ccRecordsPrompt = (text) => (entry) => appendRecord(entry.options.resume, { type: "user", message: { role: "user", content: text } });

	async function startMain(piSessionId, history) {
		const hold = gate();
		scripts.push({ label: `main:${piSessionId}`, onStart: ccRecordsPrompt(history.at(-1).content), steps: [{ type: "system", subtype: "init" }, hold.wait, { type: "result", subtype: "success", is_error: false, result: "ok" }] });
		const stream = streamSimple(model, { systemPrompt: undefined, messages: history, tools }, { sessionId: piSessionId });
		await settle();
		return { query: queries.at(-1), finish: async () => { hold.open(); await stream.result(); } };
	}

	const forkScript = (onStart) => ({
		label: "fork",
		onStart,
		steps: [{ type: "system", subtype: "init" }, assistant([
			{ type: "tool_use", name: "mcp__custom-tools__read", input: { path: "x" } },
			{ type: "tool_use", name: "mcp__custom-tools__compress", input: { startId: "m00001" } },
		])],
	});

	it("runs on the served context with the main query's options and tools, refuses every tool, and leaves the main session alone", async () => {
		const history = historyFor("A");
		const historyBefore = structuredClone(history);
		const toolsBefore = structuredClone(tools);
		const main = await startMain("pi-main", history);
		const mainShared = { ...__test.getSharedSession("pi-main") };
		const activeBefore = [...__test.activeQueryContexts];
		const servedBefore = structuredClone(__test.servedRequests.get("pi-main"));

		let forkMessages;
		scripts.push(forkScript((entry) => { forkMessages = openSession({ sessionId: entry.options.resume, projectPath: cwd, claudeDir }).messages.map((m) => m.message ?? m); }));
		const r = acpRequest("pi-main");
		assert.ok(r.accepted, "the serving instance accepts in the emit tick");
		const result = await r.accepted;
		assert.deepEqual(result.ok && result.args, { startId: "m00001" });

		const fork = queries[1];
		assert.equal(fork.label, "fork");
		assert.equal(fork.prompt, "NUDGE");
		assert.notEqual(fork.options.resume, main.query.options.resume);
		assert.notEqual(fork.options.resume, mainShared.sessionId);
		assert.equal(fork.options.maxTurns, 2);
		assert.ok(fork.options.abortController instanceof AbortController);
		assert.equal(typeof fork.options.spawnClaudeCodeProcess, "function");
		assert.equal(main.query.options.spawnClaudeCodeProcess, undefined, "the main query keeps the SDK's own spawner");
		for (const key of ["cwd", "tools", "permissionMode", "includePartialMessages", "settings", "systemPrompt", "extraArgs", "env", "effort"]) {
			assert.deepEqual(fork.options[key], main.query.options[key], `fork ${key} differs from the main query's`);
		}

		const client = new Client({ name: "test", version: "1" });
		const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
		await fork.options.mcpServers["custom-tools"].instance.connect(serverTransport);
		await client.connect(clientTransport);
		assert.deepEqual((await client.listTools()).tools, tools.map(({ name, description, parameters }) => ({ name, description, inputSchema: parameters })));
		for (const name of ["read", "compress"]) {
			const called = await client.callTool({ name, arguments: {}, _meta: { "claudecode/toolUseId": `t_${name}` } });
			assert.equal(called.isError, true, `${name} must be refused in the fork`);
		}
		await client.close();

		// What a main-session rebuild of the same history writes.
		const rebuilt = createSession({ projectPath: cwd, claudeDir });
		rebuilt.importMessages(repairToolPairing(convertPiMessages(history, new Map([["read", "mcp__custom-tools__read"], ["compress", "mcp__custom-tools__compress"]])).anthropicMessages));
		const expected = rebuilt.messages.map((m) => m.message ?? m);
		assert.deepEqual(forkMessages.map((m) => ({ role: m.role, content: m.content })), expected.map((m) => ({ role: m.role, content: m.content })));
		const blocks = forkMessages.flatMap((m) => Array.isArray(m.content) ? m.content : []);
		assert.equal(blocks.find((b) => b.type === "thinking").signature, "sig-verbatim-123");
		assert.deepEqual(blocks.find((b) => b.type === "tool_use").input, history[1].content[1].arguments);

		assert.deepEqual(history, historyBefore, "the caller's history is not mutated");
		assert.deepEqual(tools, toolsBefore, "the caller's tools are not mutated");
		assert.deepEqual(__test.servedRequests.get("pi-main"), servedBefore, "the fork does not mutate the recorded request");
		assert.equal(fork.closed, 1);
		await settle();
		assert.equal(sessionExists(fork.options.resume), false, "the fork session is deleted");
		assert.deepEqual({ ...__test.getSharedSession("pi-main") }, mainShared, "the main session mirror is untouched");
		assert.deepEqual([...__test.activeQueryContexts], activeBefore, "the fork never joins the routed query contexts");
		assert.equal(main.query.closed + main.query.interrupted, 0, "the main query is not stopped");
		assert.ok(sessionExists(mainShared.sessionId), "the main CC session is still on disk");
		await main.finish();
	});

	it("routes by session: a fork for one session uses its own request while another session streams", async () => {
		const a = await startMain("pi-a", historyFor("A"));
		const b = await startMain("pi-b", historyFor("B"));
		const sharedA = { ...__test.getSharedSession("pi-a") };
		let forkRecords;
		scripts.push(forkScript((entry) => { forkRecords = sessionRecords(entry.options.resume); }));
		const result = await acpRequest("pi-b").accepted;
		assert.equal(result.ok, true);
		const prompts = forkRecords.filter((r) => r.type === "user" && typeof r.message.content === "string").map((r) => r.message.content);
		assert.deepEqual(prompts, ["[m00001] B read it", "[m00005] B next"]);
		assert.deepEqual({ ...__test.getSharedSession("pi-a") }, sharedA);
		await a.finish();
		await b.finish();
	});

	it("carries the main session's @file expansions into the fork", async () => {
		const main = await startMain("pi-att", historyFor("A"));
		const mainId = __test.getSharedSession("pi-att").sessionId;
		const prompt = sessionRecords(mainId).find((r) => r.type === "user" && r.message.content === "[m00001] A read it");
		appendRecord(mainId, { type: "attachment", parentUuid: prompt.uuid, attachment: { type: "file", filename: "/a.js", content: { type: "text", file: { filePath: "/a.js", content: "x" } } } });
		let forkRecords;
		scripts.push(forkScript((entry) => { forkRecords = sessionRecords(entry.options.resume); }));
		const result = await acpRequest("pi-att").accepted;
		assert.equal(result.ok, true);
		assert.deepEqual(forkRecords.filter((r) => r.type === "attachment").map((r) => r.attachment.filename), ["/a.js"]);
		await main.finish();
	});

	it("refuses when an @file expansion cannot be placed, or the main transcript has not recorded the latest prompt", async () => {
		const main = await startMain("pi-skip", historyFor("A"));
		const mainId = __test.getSharedSession("pi-skip").sessionId;
		const orphanParent = appendRecord(mainId, { type: "user", message: { role: "user", content: "a prompt pi never had" } });
		appendRecord(mainId, { type: "attachment", parentUuid: orphanParent, attachment: { type: "file", filename: "/lost.js" } });
		assert.deepEqual(await acpRequest("pi-skip").accepted, { ok: false, reason: "unsupported-context" });
		await main.finish();

		const hold = gate();
		scripts.push({ label: "main:pi-behind", steps: [{ type: "system", subtype: "init" }, hold.wait, { type: "result", subtype: "success", is_error: false, result: "ok" }] });
		const behind = streamSimple(model, { systemPrompt: undefined, messages: historyFor("B"), tools }, { sessionId: "pi-behind" });
		await settle();
		assert.deepEqual(await acpRequest("pi-behind").accepted, { ok: false, reason: "unsupported-context" });
		assert.deepEqual(queries.map((q) => q.label), ["main:pi-skip", "main:pi-behind"], "no fork query is started");
		hold.open();
		await behind.result();
	});

	it("refuses without spawning anything when strict MCP config is off", async () => {
		__test.setProviderSettings({ strictMcpConfig: false });
		const main = await startMain("pi-loose", historyFor("A"));
		assert.deepEqual(await acpRequest("pi-loose").accepted, { ok: false, reason: "unsafe-config" });
		assert.deepEqual(queries.map((q) => q.label), ["main:pi-loose"]);
		await main.finish();
	});

	it("deletes the fork session only after its real CC process exits, and starts none after close", async () => {
		const main = await startMain("pi-proc", historyFor("A"));
		let spawned;
		let spawner;
		scripts.push(forkScript((entry) => {
			spawner = entry.options.spawnClaudeCodeProcess;
			spawned = spawner({ command: process.execPath, args: ["-e", "setTimeout(() => {}, 300)"], cwd, env: process.env, signal: new AbortController().signal });
		}));
		const result = await acpRequest("pi-proc").accepted;
		assert.equal(result.ok, true);
		const forkId = queries[1].options.resume;
		await settle();
		assert.equal(spawned.exitCode, null, "the child is still running");
		assert.ok(sessionExists(forkId), "its session is kept while it can still write");
		await new Promise((resolve) => spawned.once("exit", resolve));
		await settle();
		assert.equal(sessionExists(forkId), false, "deleted once the child exited");
		assert.throws(() => spawner({ command: process.execPath, args: ["-e", ""], cwd, env: process.env, signal: new AbortController().signal }), /closed/);
		await main.finish();
	});
});
