import Together from "together-ai";

/**
 * Generate vector embeddings for an array of texts using Together AI.
 * Deduplicates inputs so each unique text is only embedded once,
 * then maps results back to the original positions.
 */
export async function embedBatch(
  texts: string[],
  model: string,
): Promise<number[][]> {
  if (texts.length === 0) return [];

  const uniqueTexts = [...new Set(texts)];

  const client = new Together({ apiKey: process.env.TOGETHER_API_KEY });
  const response = await client.embeddings.create({
    model,
    input: uniqueTexts,
  });

  const uniqueResults = new Map(
    uniqueTexts.map((text, i) => [text, response.data[i].embedding]),
  );

  return texts.map((text) => uniqueResults.get(text)!);
}
