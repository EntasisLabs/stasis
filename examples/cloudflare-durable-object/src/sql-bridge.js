// Host bridge between Stasis wasm and a synchronous SQLite session.
//
// Durable Object hosts pass ctx.storage.sql. Tests pass node:sqlite.
// Integer parameters are decimal strings. SQLite INTEGER affinity stores them
// as 64-bit integers, and the kernel reads those columns with CAST(col AS TEXT)
// so JavaScript never rounds a nanosecond timestamp.

export function decodeParam(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "object" && Object.prototype.hasOwnProperty.call(value, "i")) {
    return String(value.i);
  }
  return value;
}

export function encodeCell(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "bigint") return value.toString();
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new Error("SQLite returned a non-finite number");
    }
    if (Number.isInteger(value) && !Number.isSafeInteger(value)) {
      throw new Error(
        "SQLite integer exceeded Number.MAX_SAFE_INTEGER; CAST the column to TEXT",
      );
    }
    return String(value);
  }
  return String(value);
}

function returnsRows(sql) {
  const head = sql.trim().replace(/^\(+/, "");
  return /^(select|with|explain)\b/i.test(head) || /\breturning\b/i.test(sql);
}

function cursorToResponse(cursor) {
  if (cursor && typeof cursor.raw === "function") {
    const raw = cursor.raw();
    const rows = typeof raw.toArray === "function" ? raw.toArray() : Array.from(raw);
    return {
      rowsWritten: Number(cursor.rowsWritten ?? 0),
      rows: rows.map((row) => Array.from(row, encodeCell)),
    };
  }
  return { rowsWritten: Number(cursor?.rowsWritten ?? 0), rows: [] };
}

export function createSqlBridge(storage) {
  function exec(sql, paramsJson) {
    const params = JSON.parse(paramsJson).map(decodeParam);
    try {
      const cursor = storage.exec(sql, params);
      return JSON.stringify(cursorToResponse(cursor));
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      throw new Error(`${detail} :: ${sql}`);
    }
  }

  function transaction(callback) {
    storage.transaction(() => {
      const ok = callback();
      if (ok === false) {
        throw new Error("stasis sql transaction aborted");
      }
    });
  }

  return { exec, transaction };
}

// node:sqlite binds `?1` as the named key "1", not as a positional argument.
// Durable Object `sql.exec(sql, ...bindings)` binds the same placeholders in order.
function nodeBindings(sql, params) {
  if (!/[?]\d+/.test(sql)) return params;
  const bound = {};
  for (let index = 0; index < params.length; index += 1) {
    bound[index + 1] = params[index];
  }
  return bound;
}

function callStatement(statement, method, sql, params) {
  const bound = nodeBindings(sql, params);
  return Array.isArray(bound) ? statement[method](...bound) : statement[method](bound);
}

export function createNodeSqliteStorage(db) {
  return {
    exec(sql, params) {
      const statement = db.prepare(sql);
      if (returnsRows(sql)) {
        const objects = callStatement(statement, "all", sql, params);
        return {
          rowsWritten: objects.length,
          raw() {
            return {
              toArray() {
                return objects.map((row) => Object.values(row));
              },
            };
          },
        };
      }
      const info = callStatement(statement, "run", sql, params);
      return {
        rowsWritten: Number(info.changes ?? 0),
        raw() {
          return { toArray: () => [] };
        },
      };
    },
    transaction(fn) {
      db.exec("BEGIN IMMEDIATE");
      try {
        fn();
        db.exec("COMMIT");
      } catch (error) {
        try {
          db.exec("ROLLBACK");
        } catch {
          // The transaction is already closed.
        }
        throw error;
      }
    },
  };
}

export function createDurableObjectStorage(ctx) {
  return {
    exec(sql, params) {
      return ctx.storage.sql.exec(sql, ...params);
    },
    transaction(fn) {
      ctx.storage.transactionSync(fn);
    },
  };
}
