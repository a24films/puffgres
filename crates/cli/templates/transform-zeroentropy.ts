// Transform for {{NAME}} (ZeroEntropy embeddings)
//
// Reads JSONL from stdin, writes one JSON line per batch to stdout. The
// process stays alive across batches.
//
// Requires ZEROENTROPY_API_KEY environment variable.

import { createInterface } from "readline";
import { columns, parseRow } from "./schema";
import { embedBatchZeroEntropy } from "../utils/embed-zeroentropy";

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

  // Collect texts to embed from non-delete events
  const textsToEmbed: string[] = [];

  input.forEach((event) => {
    if (event.operation !== "delete") {
      const row = parseRow(event.columns);
      // TODO: replace with the field(s) you want to embed
      const text = JSON.stringify(row);
      textsToEmbed.push(text);
    }
  });

  // Embed all texts in one batch
  const vectors = textsToEmbed.length > 0
    ? await embedBatchZeroEntropy(textsToEmbed)
    : [];

  let vectorIdx = 0;
  const output: Action[] = input.map((event) => {
    if (event.operation === "delete") {
      return { type: "delete", id: event.id };
    }

    const row = parseRow(event.columns);
    const vector = vectors[vectorIdx++];

    return {
      type: "upsert",
      id: event.id,
      document: {
        // TODO: map row fields to document fields
        // e.g. name: row.name,
      },
      vector,
      distance_metric: "cosine_distance",
    };
  });

  process.stdout.write(JSON.stringify(output) + "\n");
}
