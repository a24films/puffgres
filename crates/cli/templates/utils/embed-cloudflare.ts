/**
 * Generate vector embeddings for an array of texts using Cloudflare Workers AI.
 * Deduplicates inputs so each unique text is only embedded once,
 * then maps results back to the original positions.
 */
export async function embedBatchCloudflare(
  texts: string[],
  model = "@cf/baai/bge-base-en-v1.5",
): Promise<number[][]> {
  if (texts.length === 0) return [];

  const uniqueTexts = [...new Set(texts)];

  const accountId = process.env.CLOUDFLARE_ACCOUNT_ID;
  const apiToken = process.env.CLOUDFLARE_API_TOKEN;

  const response = await fetch(
    `https://api.cloudflare.com/client/v4/accounts/${accountId}/ai/run/${model}`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiToken}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ text: uniqueTexts }),
    },
  );

  const json = (await response.json()) as {
    result: { data: number[][] };
    success: boolean;
    errors: unknown[];
  };

  if (!json.success) {
    throw new Error(
      `Cloudflare embedding request failed: ${JSON.stringify(json.errors)}`,
    );
  }

  const uniqueResults = new Map(
    uniqueTexts.map((text, i) => [text, json.result.data[i]]),
  );

  return texts.map((text) => uniqueResults.get(text)!);
}
