let operationSequence = 0;
const TERMINAL_STATES = new Set(["succeeded", "failed", "dead_letter", "canceled"]);
const SNAPSHOT_VERSION = 1;

function cloneJson(value) {
  return value === undefined ? undefined : JSON.parse(JSON.stringify(value));
}

function memoryLifecycleStore() {
  const snapshots = new Map();
  return {
    async load(operationId) {
      return cloneJson(snapshots.get(operationId));
    },
    async save(snapshot) {
      snapshots.set(snapshot.operationId, cloneJson(snapshot));
    },
    async remove(operationId) {
      snapshots.delete(operationId);
    },
  };
}

function webStorageLifecycleStore(storage, options = {}) {
  if (!storage || typeof storage.getItem !== "function" || typeof storage.setItem !== "function") {
    throw new TypeError("storage must implement getItem() and setItem()");
  }
  const prefix = options.prefix ?? "stasis:lifecycle:";
  return {
    async load(operationId) {
      const value = storage.getItem(`${prefix}${operationId}`);
      return value == null ? undefined : JSON.parse(value);
    },
    async save(snapshot) {
      storage.setItem(`${prefix}${snapshot.operationId}`, JSON.stringify(snapshot));
    },
    async remove(operationId) {
      storage.removeItem?.(`${prefix}${operationId}`);
    },
  };
}

function requireObject(value, label) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError(`${label} must be an object`);
  }
}

function tool(definition) {
  requireObject(definition, "tool definition");
  if (typeof definition.name !== "string" || !definition.name.trim()) {
    throw new TypeError("tool.name must be a non-empty string");
  }
  if (typeof definition.description !== "string" || !definition.description.trim()) {
    throw new TypeError("tool.description must be a non-empty string");
  }
  requireObject(definition.parameters, "tool.parameters");
  if (typeof definition.execute !== "function") {
    throw new TypeError("tool.execute must be a function");
  }
  const replay = definition.replay ?? "unsafe";
  if (replay !== "safe" && replay !== "unsafe") {
    throw new TypeError('tool.replay must be "safe" or "unsafe"');
  }
  return Object.freeze({ ...definition, replay });
}

function extension(definition) {
  requireObject(definition, "extension");
  if (typeof definition.name !== "string" || !definition.name.trim()) {
    throw new TypeError("extension.name must be a non-empty string");
  }
  if (definition.tools !== undefined && !Array.isArray(definition.tools)) {
    throw new TypeError("extension.tools must be an array");
  }
  if (definition.sections !== undefined && !Array.isArray(definition.sections)) {
    throw new TypeError("extension.sections must be an array");
  }
  return Object.freeze({ ...definition });
}

function operationId() {
  if (globalThis.crypto?.randomUUID) return globalThis.crypto.randomUUID();
  operationSequence += 1;
  return `stasis-${Date.now().toString(36)}-${operationSequence.toString(36)}`;
}

function abortError() {
  if (typeof DOMException === "function") {
    return new DOMException("The wait was aborted", "AbortError");
  }
  const error = new Error("The wait was aborted");
  error.name = "AbortError";
  return error;
}

function parseJson(raw) {
  return typeof raw === "string" ? JSON.parse(raw) : raw;
}

function isMissingJob(error) {
  return String(error?.message ?? error).includes("job not found");
}

class StasisSession {
  #stasis;
  id;

  constructor(stasis, id) {
    if (typeof id !== "string" || !id.trim()) {
      throw new TypeError("session id must be a non-empty string");
    }
    this.#stasis = stasis;
    this.id = id;
  }

  submit(input, options = {}) {
    return this.#stasis.submit(input, { ...options, session: this.id });
  }

  prompt(input, options = {}) {
    return this.#stasis.prompt(input, { ...options, session: this.id });
  }

  wait(operationId, options = {}) {
    return this.#stasis.wait(operationId, options);
  }

  stream(input, options = {}) {
    return this.#stasis.stream(input, { ...options, session: this.id });
  }

  events(operationId, options = {}) {
    return this.#stasis.events(operationId, options);
  }

  cancel(operationId) {
    return this.#stasis.cancel(operationId);
  }

  resume(operationId, options = {}) {
    return this.#stasis.resume(operationId, options);
  }
}

class Stasis {
  #client;
  #instructions;
  #sections;
  #defaults;
  #toolNames;
  #toolReplay;
  #lifecycle;

  constructor(client, options = {}) {
    this.#client = client;
    this.#instructions = options.instructions;
    this.#defaults = options.defaults ?? {};
    this.#sections = [];
    this.#lifecycle = options.lifecycleStore ?? memoryLifecycleStore();
    if (typeof this.#lifecycle.load !== "function" || typeof this.#lifecycle.save !== "function") {
      throw new TypeError("lifecycleStore must implement load() and save()");
    }

    const definitions = [...(options.tools ?? [])];
    for (const item of options.extensions ?? []) {
      const installed = extension(item);
      definitions.push(...(installed.tools ?? []));
      this.#sections.push(...(installed.sections ?? []));
    }

    this.#toolNames = [];
    this.#toolReplay = new Map();
    for (const definition of definitions) {
      const registered = tool(definition);
      const callback = (input, context) => registered.execute(input, context);
      if (registered.timeoutMs === undefined) {
        client.register_tool(
          registered.name,
          registered.description,
          registered.parameters,
          callback,
        );
      } else {
        client.register_tool_with_options(
          registered.name,
          registered.description,
          registered.parameters,
          callback,
          { timeoutMs: registered.timeoutMs },
        );
      }
      this.#toolNames.push(registered.name);
      this.#toolReplay.set(registered.name, registered.replay);
    }
  }

  /** Escape hatch for advanced queue, memory, replay, and Grapheme APIs. */
  get raw() {
    return this.#client;
  }

  session(id = this.#defaults.session ?? "default") {
    return new StasisSession(this, id);
  }

  async #systemPrompt(input, session) {
    const chunks = [];
    if (typeof this.#instructions === "function") {
      chunks.push(await this.#instructions({ input, session }));
    } else if (this.#instructions) {
      chunks.push(this.#instructions);
    }
    for (const section of this.#sections) {
      if (!section || typeof section.render !== "function") {
        throw new TypeError("extension sections require a render function");
      }
      const rendered = await section.render({ input, session });
      if (!rendered) continue;
      chunks.push(section.tag === false ? String(rendered) : `<${section.key}>\n${rendered}\n</${section.key}>`);
    }
    const prompt = chunks.filter(Boolean).join("\n\n");
    return prompt || undefined;
  }

  async #save(snapshot) {
    await this.#lifecycle.save({ version: SNAPSHOT_VERSION, updatedAt: new Date().toISOString(), ...snapshot });
  }

  async #load(id) {
    const snapshot = await this.#lifecycle.load(id);
    if (snapshot && snapshot.version !== SNAPSHOT_VERSION) {
      throw new Error(`unsupported lifecycle snapshot version: ${snapshot.version}`);
    }
    return snapshot;
  }

  async #appendEvent(snapshot, type, data = {}) {
    const events = snapshot.events ?? [];
    const previous = events.at(-1);
    if (previous?.type === type && type === "state" && previous.state === data.state) return previous;
    if (previous?.type === type && ["completed", "failed", "cancelled"].includes(type)) {
      const event = { ...previous, ...data };
      snapshot.events = [...events.slice(0, -1), event];
      await this.#save(snapshot);
      return event;
    }
    const event = {
      id: events.length ? events.at(-1).id + 1 : 1,
      operationId: snapshot.operationId,
      type,
      timestamp: new Date().toISOString(),
      ...data,
    };
    snapshot.events = [...events, event];
    await this.#save(snapshot);
    return event;
  }

  async submit(input, options = {}) {
    if (typeof input !== "string" || !input.trim()) {
      throw new TypeError("prompt input must be a non-empty string");
    }
    const id = options.operationId ?? operationId();
    try {
      await this.#client.job_state(id);
      const session = options.session ?? this.#defaults.session ?? "default";
      if (!(await this.#load(id))) {
        await this.#save({ operationId: id, session, status: "enqueued", events: [] });
      }
      return { operationId: id, accepted: false, session };
    } catch (error) {
      if (!isMissingJob(error)) throw error;
    }

    const selectedTool = options.tool ?? this.#defaults.tool ?? this.#toolNames[0] ?? "echo";
    const session = options.session ?? this.#defaults.session ?? "default";
    const systemPrompt = options.instructions ?? await this.#systemPrompt(input, session);
    const enqueueOptions = {
      tool: selectedTool,
      toolInput: options.toolInput ?? {},
      sessionId: session,
    };
    if (systemPrompt) enqueueOptions.systemPrompt = systemPrompt;
    await this.#client.enqueue_tool_loop_with_options(id, input, enqueueOptions);
    const snapshot = { operationId: id, session, tool: selectedTool, status: "enqueued", events: [] };
    await this.#appendEvent(snapshot, "accepted", { session });
    return { operationId: id, accepted: true, session };
  }

  async #processOnce(id, options) {
    const processing = this.#client.process_once(
      options.queue ?? "default",
      options.worker ?? "stasis-sdk",
    );
    if (!options.signal) return processing;
    return new Promise((resolve, reject) => {
      let settled = false;
      const finish = (callback, value) => {
        if (settled) return;
        settled = true;
        options.signal.removeEventListener("abort", onAbort);
        callback(value);
      };
      const onAbort = async () => {
        try {
          if (options.cancelOnAbort !== false) await this.cancel(id);
          finish(reject, abortError());
        } catch (error) {
          finish(reject, error);
        }
      };
      options.signal.addEventListener("abort", onAbort, { once: true });
      if (options.signal.aborted) void onAbort();
      processing.then(
        (value) => finish(resolve, value),
        (error) => finish(reject, error),
      );
    });
  }

  async *events(id, options = {}) {
    const limit = options.processLimit ?? 32;
    if (!Number.isInteger(limit) || limit < 1) {
      throw new TypeError("processLimit must be a positive integer");
    }
    const after = options.after ?? 0;
    if (!Number.isInteger(after) || after < 0) throw new TypeError("after must be a non-negative integer");

    let snapshot = await this.#load(id);
    if (snapshot?.events) {
      for (const event of snapshot.events) if (event.id > after) yield event;
      if (snapshot.result) return;
    }

    try {
      let state = await this.#client.job_state(id);
      snapshot ??= { operationId: id, session: "default", status: state, events: [] };
      let cursor = Math.max(after, snapshot.events?.at(-1)?.id ?? 0);
      for (let step = 0; !TERMINAL_STATES.has(state); step += 1) {
        if (options.signal?.aborted) {
          if (options.cancelOnAbort !== false) await this.cancel(id);
          throw abortError();
        }
        if (step >= limit) throw new Error(`operation ${id} did not finish within ${limit} processing steps`);
        snapshot.status = state;
        const stateEvent = await this.#appendEvent(snapshot, "state", { state });
        if (stateEvent.id > cursor) { cursor = stateEvent.id; yield stateEvent; }
        await this.#processOnce(id, options);
        state = await this.#client.job_state(id);
      }

      const history = parseJson(await this.#client.job_history(id));
      const attempt = history.attempts?.at(-1);
      const diagnostics = attempt?.diagnostics ?? null;
      const result = {
        operationId: id,
        status: state === "succeeded" ? "done" : state === "canceled" ? "cancelled" : "failed",
        text: diagnostics?.output_text ?? diagnostics?.output_preview,
        diagnostics,
        history,
      };
      const reason = attempt?.error_message ?? (state === "canceled" ? "operation cancelled" : undefined);
      if (reason !== undefined) result.reason = reason;
      snapshot.status = state;
      snapshot.result = result;
      const finalEvent = await this.#appendEvent(snapshot, result.status === "done" ? "completed" : result.status, { result });
      if (finalEvent.id > cursor) yield finalEvent;
    } catch (error) {
      if (snapshot?.result) return;
      if (isMissingJob(error) && snapshot) {
        const unavailable = await this.#appendEvent(snapshot, "unavailable", {
          reason: "operation is not present in this runtime; only completed snapshots can resume after runtime replacement",
        });
        if (unavailable.id > after) yield unavailable;
        return;
      }
      throw error;
    }
  }

  async wait(id, options = {}) {
    let result;
    for await (const event of this.events(id, options)) {
      if (event.result) result = event.result;
    }
    if (result) return result;
    const snapshot = await this.#load(id);
    if (snapshot?.result) return snapshot.result;
    throw new Error(`operation ${id} is unavailable`);
  }

  stream(input, options = {}) {
    const self = this;
    return (async function* () {
      const receipt = await self.submit(input, options);
      yield* self.events(receipt.operationId, options);
    })();
  }

  async cancel(id) {
    const cancelled = await this.#client.cancel_job(id);
    let snapshot = await this.#load(id);
    if (snapshot && cancelled) {
      snapshot.status = "canceled";
      await this.#appendEvent(snapshot, "cancelled", { reason: "operation cancelled" });
    }
    return cancelled;
  }

  async resume(id, options = {}) {
    const snapshot = await this.#load(id);
    if (snapshot?.result) return snapshot.result;
    const replay = snapshot?.tool ? this.#toolReplay.get(snapshot.tool) ?? "unsafe" : "unsafe";
    let state;
    try { state = await this.#client.job_state(id); } catch (error) {
      if (isMissingJob(error)) throw new Error(`operation ${id} cannot resume: runtime state is unavailable`);
      throw error;
    }
    if (state === "dead_letter") {
      if (replay !== "safe" && options.allowUnsafeReplay !== true) {
        throw new Error(`operation ${id} uses replay-unsafe tool ${snapshot?.tool ?? "unknown"}; pass allowUnsafeReplay: true to override`);
      }
      await this.#client.replay_dead_letter(id);
    }
    return this.wait(id, options);
  }

  async prompt(input, options = {}) {
    const receipt = await this.submit(input, options);
    return this.wait(receipt.operationId, options);
  }
}

function createStasisWith(client, options = {}) {
  return new Stasis(client, options);
}

module.exports = { Stasis, StasisSession, createStasisWith, extension, memoryLifecycleStore, tool, webStorageLifecycleStore };
