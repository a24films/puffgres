// Transform for buyer_name
//
// Reads newline-delimited JSON (NDJSON) from stdin. Each line is a JSON array
// of events. For each line, write a JSON array of actions to stdout followed
// by a newline.
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

import "../../utils/load-env.ts";
import { createInterface } from "readline";
import { embedBatchZeroEntropy } from "../../utils/embed-zeroentropy.ts";
import { parseRow } from "./schema.ts";

interface Event {
  operation: "insert" | "update" | "delete";
  id: number | string;
  columns: (string | null)[];
}

type Action =
  | {
      type: "upsert";
      id: number | string;
      document: Record<string, unknown>;
      vector?: number[];
      distance_metric?: string;
      schema?: Record<string, unknown>;
    }
  | { type: "delete"; id: number | string }
  | { type: "skip" };

const rl = createInterface({ input: process.stdin });

for await (const line of rl) {
  const input: Event[] = JSON.parse(line);

  const upsertEvents = input.filter((e) => e.operation !== "delete");
  const buyerNames = upsertEvents.map(
    (e) => parseRow(e.columns).buyer_name ?? "",
  );
  const vectors = await embedBatchZeroEntropy(buyerNames);
  const vectorMap = new Map(upsertEvents.map((e, i) => [e.id, vectors[i]]));

  const output: Action[] = input.map((event) => {
    if (event.operation === "delete") {
      return { type: "delete", id: event.id };
    }

    const row = parseRow(event.columns);

    return {
      type: "upsert",
      id: event.id,
      document: {
        buyer_name: row.buyer_name,
      },
      vector: vectorMap.get(event.id)!,
      distance_metric: "cosine_distance",
      schema: {
        buyer_name: { type: "string" },
      },
    };
  });

  process.stdout.write(JSON.stringify(output) + "\n");
}
