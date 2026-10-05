// Pi exports PI_CODING_AGENT_DIR to the tools it launches, so a suite run from
// inside pi inherits the user's real agent dir. Run the config tests that way,
// against a sentinel dir, and check none of their fixture writes reach it.
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, utimesSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

describe("config tests under an inherited PI_CODING_AGENT_DIR", () => {
	it("leave the inherited agent dir's claude-bridge.json untouched", () => {
		const root = mkdtempSync(join(tmpdir(), "claude-bridge-sentinel-"));
		try {
			const agentDir = join(root, "agent");
			const home = join(root, "home");
			const config = join(agentDir, "claude-bridge.json");
			const original = `{ "sentinel": ${JSON.stringify(root)} }\n`;
			mkdirSync(agentDir, { recursive: true });
			mkdirSync(home);
			writeFileSync(config, original);
			const past = new Date(Date.now() - 60_000);
			utimesSync(config, past, past);
			const before = statSync(config).mtimeMs;

			// NODE_TEST_CONTEXT would make the nested runner report to this one
			// instead of running the file.
			const { NODE_TEST_CONTEXT: _, ...env } = process.env;
			const run = spawnSync(process.execPath, ["--import", "tsx", "--import", "./tests/lib/setup.mjs", "--test", "tests/unit-config.mjs"], {
				cwd: process.cwd(),
				env: { ...env, PI_CODING_AGENT_DIR: agentDir, HOME: home },
				encoding: "utf8",
			});
			assert.equal(run.status, 0, run.stdout + run.stderr);
			assert.equal(readFileSync(config, "utf8"), original);
			assert.equal(statSync(config).mtimeMs, before);
		} finally {
			rmSync(root, { recursive: true, force: true });
		}
	});
});
