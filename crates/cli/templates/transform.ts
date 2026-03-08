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

import { createInterface } from "readline";
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

for await (const line of rl) {
  const input: Event[] = JSON.parse(line);

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
        // e.g. name: row.name,
      },
      // Build schema from columns for just the fields in your document.
      // Each column has .name and .type (PrimitiveType). Add overrides as needed.
      // schema: {
      //   name: { type: columns.find(c => c.name === "name")!.type, full_text_search: true },
      // },
    };
  });

  process.stdout.write(JSON.stringify(output) + "\n");
}
