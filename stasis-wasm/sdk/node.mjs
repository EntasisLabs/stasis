import bindings from "../pkg-node/stasis_wasm.js";
import { Stasis, StasisSession, createStasisWith, extension, memoryLifecycleStore, tool, webStorageLifecycleStore } from "./core.mjs";

const { StasisWasmClient } = bindings;
export { Stasis, StasisSession, extension, memoryLifecycleStore, tool, webStorageLifecycleStore };

export async function createStasis(options = {}) {
  const client = await StasisWasmClient.create(options.llm);
  return createStasisWith(client, options);
}
