// Transform for {{NAME}}
//
// Reads JSONL from stdin, writes one JSON line per batch to stdout. The
// process stays alive across batches.
//
// Each output action (one of):
//   { type: "upsert", id: number | string, document: object, vector?: number[], distance_metric?: string, schema?: object }
//   { type: "delete", id: number | string }
//   { type: "skip" }
//
// distance_metric is required when vector is provided. Values: "cosine_distance" | "euclidean_squared"
//
// schema defines attribute types for the namespace.
// See https://turbopuffer.com/docs/write#schema for all options.

import "../../utils/load-env";
import { createInterface } from "readline";
import { embedBatchBaseten } from "../../utils/embed-baseten";
import { columns, parseRow } from "./schema";

interface Event {
  operation: "insert" | "update" | "delete";
  id: number | string;
  columns: (string | null)[];
}

type Action =
  | { type: "upsert"; id: number | string; document: Record<string, unknown>; vector?: number[]; distance_metric?: string; schema?: Record<string, unknown> }
  | { type: "delete"; id: number | string }
  | { type: "skip" };

const rl = createInterface({ input: process.stdin });

void (async () => {
  for await (const line of rl) {
    const input: Event[] = JSON.parse(line);

    const upsertEvents = input.filter((e) => e.operation !== "delete");
    const texts = upsertEvents.map((e) => {
      const row = parseRow(e.columns);
      // TODO: return the text you want embedded for this row
      // e.g. return row.name ?? "";
      return "";
    });
    const vectors = await embedBatchBaseten(texts);
    const vectorById = new Map(upsertEvents.map((e, i) => [e.id, vectors[i]]));

    const output: Action[] = input.map((event) => {
      if (event.operation === "delete") {
        return { type: "delete", id: event.id };
      }

      const row = parseRow(event.columns);

      return {
        type: "upsert",
        id: event.id,
        document: {
          // TODO: map row fields to document fields
        },
        vector: vectorById.get(event.id),
        distance_metric: "cosine_distance",
      };
    });

    process.stdout.write(JSON.stringify(output) + "\n");
  }
})();
