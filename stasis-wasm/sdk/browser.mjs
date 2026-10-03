import init, { StasisWasmClient } from "../pkg/stasis_wasm.js";
import { Stasis, StasisSession, createStasisWith, extension, memoryLifecycleStore, schema, tool, typeboxSchema, valibotSchema, webStorageLifecycleStore, zodSchema } from "./core.mjs";

export { Stasis, StasisSession, extension, memoryLifecycleStore, schema, tool, typeboxSchema, valibotSchema, webStorageLifecycleStore, zodSchema };

export async function createStasis(options = {}) {
  await init();
  const client = await StasisWasmClient.create(options.llm);
  return createStasisWith(client, options);
}
