// Transform for {{NAME}}
//
// Reads a JSON array of events from stdin, writes a JSON array of actions to stdout.
//
// Each input event:
//   { operation: "insert" | "update" | "delete", id: number | string, columns: (string | null)[] }
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

import { readFileSync } from "fs";

interface Event {
  operation: "insert" | "update" | "delete";
  id: number | string;
  columns: (string | null)[];
}

type Action =
  | { type: "upsert"; id: number | string; document: Record<string, unknown>; vector?: number[]; distance_metric?: string; schema?: Record<string, unknown> }
  | { type: "delete"; id: number | string }
  | { type: "skip" };

const input: Event[] = JSON.parse(readFileSync("/dev/stdin", "utf-8"));

const output: Action[] = input.map((event) => {
  if (event.operation === "delete") {
    return { type: "delete", id: event.id };
  }

  return {
    type: "upsert",
    id: event.id,
    document: {
      // TODO: map columns to document fields
    },
    // Define a schema entry for each attribute in your document.
    // For all config options, see https://turbopuffer.com/docs/write#schema
    // schema: {
    //   name: { type: "string", full_text_search: true },
    //   title: { type: "string" },
    // },
  };
});

process.stdout.write(JSON.stringify(output));
