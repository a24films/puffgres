import OpenAI from "openai";

/**
 * Generate vector embeddings for an array of texts using ZeroEntropy.
 * Deduplicates inputs so each unique text is only embedded once,
 * then maps results back to the original positions.
 */
export async function embedBatchZeroEntropy(
  texts: string[],
): Promise<number[][]> {
  if (texts.length === 0) return [];

  const uniqueTexts = [...new Set(texts)];

  const client = new OpenAI({
    baseURL: "https://api.zeroentropy.dev/v1/models/openai",
    apiKey: process.env.ZEROENTROPY_API_KEY,
  });

  const response = await client.embeddings.create({
    model: "zembed-1",
    input: uniqueTexts,
    // @ts-expect-error ZeroEntropy-specific parameter not in OpenAI types
    input_type: "query",
  });

  const uniqueResults = new Map(
    uniqueTexts.map((text, i) => [text, response.data[i].embedding]),
  );

  return texts.map((text) => uniqueResults.get(text)!);
}
