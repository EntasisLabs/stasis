const { StasisWasmClient } = require("../pkg-node/stasis_wasm.js");
const { Stasis, StasisSession, createStasisWith, extension, memoryLifecycleStore, tool, webStorageLifecycleStore } = require("./core.cjs");

async function createStasis(options = {}) {
  const client = await StasisWasmClient.create(options.llm);
  return createStasisWith(client, options);
}

module.exports = { Stasis, StasisSession, createStasis, extension, memoryLifecycleStore, tool, webStorageLifecycleStore };
