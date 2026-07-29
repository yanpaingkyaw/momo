import { existsSync } from "node:fs";
import path from "node:path";
import type { HerdrClient } from "./client.js";
import { PaneRegistry, type PaneRecord } from "./registry.js";
import { momoCacheRoot } from "../ipc/spool.js";

export interface LegacyMigrationResult {
	closed: string[];
	refused: Array<{ workerId: string; status: string; reason: string }>;
	notes: string[];
}

/**
 * One-time migration: close terminal/ready legacy v1 per-task panes safely.
 * Active/uncertain legacy panes fail clearly for operator cleanup — never adopted.
 */
export async function migrateLegacyPaneRegistry(
	legacy: PaneRegistry,
	client: HerdrClient,
	options: { notify?: (message: string) => void } = {},
): Promise<LegacyMigrationResult> {
	const result: LegacyMigrationResult = { closed: [], refused: [], notes: [] };
	const { snapshot, corruption } = legacy.tryRead();
	if (corruption) {
		result.notes.push(`Legacy registry corrupt (not migrated): ${corruption}`);
		options.notify?.(result.notes[0]!);
		return result;
	}
	if (snapshot.panes.length === 0) return result;

	result.notes.push(
		`Found ${snapshot.panes.length} legacy v1 pane-per-task record(s); migrating (never adopting as pool workers).`,
	);

	for (const pane of snapshot.panes) {
		if (pane.status === "starting" || pane.status === "running") {
			result.refused.push({
				workerId: pane.workerId,
				status: pane.status,
				reason: "active legacy pane — close manually then /momo-cleanup; not adopted into role pool",
			});
			continue;
		}
		if (pane.status === "uncertain") {
			result.refused.push({
				workerId: pane.workerId,
				status: pane.status,
				reason: "uncertain legacy implementer — supervised force cleanup required; not adopted",
			});
			continue;
		}
		// ready / completed / failed / aborted — close safely
		try {
			await client.closePane(pane.paneId);
			legacy.remove(pane.workerId);
			result.closed.push(pane.workerId);
		} catch (error) {
			result.refused.push({
				workerId: pane.workerId,
				status: pane.status,
				reason: `close failed: ${error instanceof Error ? error.message : String(error)}`,
			});
		}
	}

	const summary = [
		`Legacy migration closed ${result.closed.length} pane(s).`,
		result.refused.length
			? `Refused ${result.refused.length}: ${result.refused
					.map((item) => `${item.workerId}(${item.status})`)
					.join(", ")}`
			: "",
		...result.notes,
	]
		.filter(Boolean)
		.join(" ");
	options.notify?.(summary);
	return result;
}

export function legacyRegistryExists(parentId: string, cacheRoot = momoCacheRoot()): boolean {
	return existsSync(path.join(cacheRoot, "parents", parentId, "registry.json"));
}

export function summarizeLegacyRefusals(panes: readonly PaneRecord[]): string[] {
	return panes
		.filter((pane) => pane.status === "starting" || pane.status === "running" || pane.status === "uncertain")
		.map(
			(pane) =>
				`${pane.workerId} status=${pane.status} pane=${pane.paneId} — close manually; not pool-adoptable`,
		);
}
