import { mkdtempSync, rmSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { getPackageVersion, isEntryPoint, parseCliArgs } from "../src/cli.js";

describe("parseCliArgs", () => {
  it("opens the TUI without an initial message", () => {
    expect(parseCliArgs([])).toEqual({ action: "run", rawArgs: [] });
  });

  it("joins positional arguments into one initial message", () => {
    expect(parseCliArgs(["fix", "the", "tests"])).toEqual({
      action: "run",
      initialMessage: "fix the tests",
      rawArgs: ["fix", "the", "tests"],
    });
  });

  it.each([
    ["--help", "help"],
    ["-h", "help"],
    ["--version", "version"],
    ["-v", "version"],
  ] as const)("recognizes %s", (flag, action) => {
    expect(parseCliArgs([flag])).toEqual({ action, rawArgs: [flag] });
  });

  it("rejects unknown options", () => {
    expect(() => parseCliArgs(["--cwd", "/tmp"])).toThrow("Unknown option: --cwd");
  });
});

describe("getPackageVersion", () => {
  it("reads the version from package metadata", () => {
    expect(getPackageVersion()).toMatch(/^\d+\.\d+\.\d+/);
  });
});

describe("isEntryPoint", () => {
  it("recognizes an executable reached through a symlink", () => {
    const tempDirectory = mkdtempSync(path.join(tmpdir(), "momo-cli-"));
    const moduleUrl = new URL("../src/cli.ts", import.meta.url).href;
    const executableLink = path.join(tempDirectory, "momo");

    try {
      symlinkSync(fileURLToPath(moduleUrl), executableLink);
      expect(isEntryPoint(moduleUrl, executableLink)).toBe(true);
    } finally {
      rmSync(tempDirectory, { recursive: true, force: true });
    }
  });
});
