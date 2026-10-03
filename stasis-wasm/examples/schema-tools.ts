import { Type } from "@sinclair/typebox";
import { Value } from "@sinclair/typebox/value";
import { z } from "zod";
import * as v from "valibot";
import { toJsonSchema } from "@valibot/to-json-schema";
import { tool, typeboxSchema, valibotSchema, zodSchema } from "stasis-wasm/sdk";

const TypeBoxParameters = Type.Object({ resourceId: Type.String() });
export const typeboxTool = tool({
  name: "typebox_lookup",
  description: "Look up a resource with TypeBox",
  parameters: typeboxSchema(TypeBoxParameters, {
    parse: input => Value.Parse(TypeBoxParameters, input),
  }),
  execute({ resourceId }) {
    return { resourceId, validator: "typebox" };
  },
});

const ZodParameters = z.object({ resourceId: z.string() });
export const zodTool = tool({
  name: "zod_lookup",
  description: "Look up a resource with Zod",
  parameters: zodSchema(ZodParameters, { toJSONSchema: z.toJSONSchema }),
  execute({ resourceId }) {
    return { resourceId, validator: "zod" };
  },
});

const ValibotParameters = v.object({ resourceId: v.string() });
export const valibotTool = tool({
  name: "valibot_lookup",
  description: "Look up a resource with Valibot",
  parameters: valibotSchema(ValibotParameters, {
    toJsonSchema,
    parse: input => v.parse(ValibotParameters, input),
  }),
  execute({ resourceId }) {
    return { resourceId, validator: "valibot" };
  },
});
