import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { defineConfig } from "vitest/config";

// Isolate all momo cache writes from the developer ~/.cache during plain `npm test`.
const testCacheRoot = mkdtempSync(path.join(tmpdir(), "momo-vitest-cache-"));
process.env.XDG_CACHE_HOME = testCacheRoot;
process.env.MOMO_TEST_CACHE_ROOT = testCacheRoot;

export default defineConfig({
	test: {
		environment: "node",
		include: ["test/**/*.test.ts"],
		clearMocks: true,
		restoreMocks: true,
		fileParallelism: false,
		maxConcurrency: 4,
	},
});
