import type {
  StasisCreateOptions,
  StasisToolContext,
  StasisWasmClient,
} from "../pkg/stasis_wasm";

export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };
export type JsonSchema = Record<string, unknown>;

export interface ToolDefinition<TInput = any, TResult = JsonValue> {
  name: string;
  description: string;
  parameters: JsonSchema & { readonly static?: TInput };
  timeoutMs?: number;
  /** Safe tools may be executed again when explicitly resuming a dead-lettered operation. */
  replay?: "safe" | "unsafe";
  execute(input: TInput, context: StasisToolContext): TResult | Promise<TResult>;
}

export interface PromptSectionContext {
  input: string;
  session: string;
}

export interface PromptSection {
  key: string;
  tag?: boolean;
  render(context: PromptSectionContext): string | undefined | Promise<string | undefined>;
}

export interface StasisExtension {
  name: string;
  tools?: readonly ToolDefinition<any, any>[];
  sections?: readonly PromptSection[];
}

export interface StasisDefaults {
  tool?: string;
  session?: string;
}

export interface LifecycleEvent {
  id: number;
  operationId: string;
  type: "accepted" | "state" | "completed" | "failed" | "cancelled" | "unavailable";
  timestamp: string;
  state?: string;
  session?: string;
  reason?: string;
  result?: PromptResult;
}

export interface LifecycleSnapshot {
  version: 1;
  operationId: string;
  session: string;
  tool?: string;
  status: string;
  updatedAt?: string;
  events: LifecycleEvent[];
  result?: PromptResult;
}

export interface WebStorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem?(key: string): void;
}

export interface LifecycleStore {
  load(operationId: string): LifecycleSnapshot | undefined | Promise<LifecycleSnapshot | undefined>;
  save(snapshot: LifecycleSnapshot): void | Promise<void>;
  remove?(operationId: string): void | Promise<void>;
}

export interface CreateStasisOptions {
  llm?: StasisCreateOptions;
  instructions?: string | ((context: PromptSectionContext) => string | Promise<string>);
  tools?: readonly ToolDefinition<any, any>[];
  extensions?: readonly StasisExtension[];
  defaults?: StasisDefaults;
  lifecycleStore?: LifecycleStore;
}

export interface PromptOptions {
  operationId?: string;
  session?: string;
  tool?: string;
  toolInput?: JsonValue;
  instructions?: string;
  signal?: AbortSignal;
  queue?: string;
  worker?: string;
  processLimit?: number;
  /** Resume event delivery after this durable event id. */
  after?: number;
  /** Defaults to true: aborting also transitions the underlying job to cancelled. */
  cancelOnAbort?: boolean;
  /** Required to retry a dead letter that selected a replay-unsafe tool. */
  allowUnsafeReplay?: boolean;
}

export interface SubmissionReceipt {
  operationId: string;
  accepted: boolean;
  session: string;
}

export interface PromptResult {
  operationId: string;
  status: "done" | "failed" | "cancelled";
  text?: string;
  reason?: string;
  diagnostics: Record<string, unknown> | null;
  history: Record<string, unknown>;
}

export class StasisSession {
  readonly id: string;
  submit(input: string, options?: Omit<PromptOptions, "session">): Promise<SubmissionReceipt>;
  prompt(input: string, options?: Omit<PromptOptions, "session">): Promise<PromptResult>;
  wait(operationId: string, options?: PromptOptions): Promise<PromptResult>;
  stream(input: string, options?: Omit<PromptOptions, "session">): AsyncIterable<LifecycleEvent>;
  events(operationId: string, options?: PromptOptions): AsyncIterable<LifecycleEvent>;
  cancel(operationId: string): Promise<boolean>;
  resume(operationId: string, options?: PromptOptions): Promise<PromptResult>;
}

export class Stasis {
  readonly raw: StasisWasmClient;
  session(id?: string): StasisSession;
  submit(input: string, options?: PromptOptions): Promise<SubmissionReceipt>;
  wait(operationId: string, options?: PromptOptions): Promise<PromptResult>;
  prompt(input: string, options?: PromptOptions): Promise<PromptResult>;
  stream(input: string, options?: PromptOptions): AsyncIterable<LifecycleEvent>;
  events(operationId: string, options?: PromptOptions): AsyncIterable<LifecycleEvent>;
  cancel(operationId: string): Promise<boolean>;
  resume(operationId: string, options?: PromptOptions): Promise<PromptResult>;
}

export function tool<TInput = any, TResult = JsonValue>(
  definition: ToolDefinition<TInput, TResult>,
): ToolDefinition<TInput, TResult>;
export function extension(definition: StasisExtension): StasisExtension;
export function memoryLifecycleStore(): LifecycleStore;
export function webStorageLifecycleStore(
  storage: WebStorageLike,
  options?: { prefix?: string },
): LifecycleStore;
export function createStasis(options?: CreateStasisOptions): Promise<Stasis>;
