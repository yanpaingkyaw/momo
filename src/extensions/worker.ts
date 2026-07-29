/**
 * Momo worker Pi extension entrypoint.
 */
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { installMomoWorker } from "./worker-runtime.js";

export default function (pi: ExtensionAPI): void {
	installMomoWorker(pi);
}
