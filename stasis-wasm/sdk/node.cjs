const { StasisWasmClient } = require("../pkg-node/stasis_wasm.js");
const { Stasis, StasisSession, createStasisWith, extension, memoryLifecycleStore, schema, tool, typeboxSchema, valibotSchema, webStorageLifecycleStore, zodSchema } = require("./core.cjs");

async function createStasis(options = {}) {
  const client = await StasisWasmClient.create(options.llm);
  return createStasisWith(client, options);
}

module.exports = { Stasis, StasisSession, createStasis, extension, memoryLifecycleStore, schema, tool, typeboxSchema, valibotSchema, webStorageLifecycleStore, zodSchema };
