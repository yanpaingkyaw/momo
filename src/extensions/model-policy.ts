/**
 * In-process Momo model policy extension (commands + parent apply).
 */
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { registerModelPolicyCommands } from "../config/model-commands.js";

export default function (pi: ExtensionAPI): void {
	registerModelPolicyCommands(pi);
}
