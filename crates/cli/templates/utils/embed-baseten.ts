import { requireEnv } from "./load-env";

/**
 * Generate vector embeddings for an array of texts using a Baseten-deployed model.
 * Deduplicates inputs so each unique text is only embedded once,
 * then maps results back to the original positions.
 *
 * Requires BASETEN_API_KEY and BASETEN_MODEL_ID environment variables.
 */
export async function embedBatchBaseten(
  texts: string[],
): Promise<number[][]> {
  if (texts.length === 0) return [];

  const apiKey = requireEnv("BASETEN_API_KEY");
  const modelId = requireEnv("BASETEN_MODEL_ID");

  const uniqueTexts = [...new Set(texts)];

  const response = await fetch(
    `https://model-${modelId}.api.baseten.co/production/predict`,
    {
      method: "POST",
      headers: {
        Authorization: `Api-Key ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ input: uniqueTexts }),
    },
  );

  if (!response.ok) {
    throw new Error(`Baseten API error: ${response.status} ${await response.text()}`);
  }

  const result = await response.json() as { data: { embedding: number[] }[] };

  const uniqueResults = new Map(
    uniqueTexts.map((text, i) => [text, result.data[i].embedding]),
  );

  return texts.map((text) => uniqueResults.get(text)!);
}
