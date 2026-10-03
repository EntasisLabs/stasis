import bindings from "../pkg-node/stasis_wasm.js";
import { Stasis, StasisSession, createStasisWith, extension, memoryLifecycleStore, schema, tool, typeboxSchema, valibotSchema, webStorageLifecycleStore, zodSchema } from "./core.mjs";

const { StasisWasmClient } = bindings;
export { Stasis, StasisSession, extension, memoryLifecycleStore, schema, tool, typeboxSchema, valibotSchema, webStorageLifecycleStore, zodSchema };

export async function createStasis(options = {}) {
  const client = await StasisWasmClient.create(options.llm);
  return createStasisWith(client, options);
}
