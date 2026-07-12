import { requireEnv } from "./load-env";
import { tokenizeBatch } from "./tokenize";

const TOKENIZER = "Xenova/bge-base-en-v1.5";
const MAX_INPUT_TOKENS = 512;

/**
 * Generate vector embeddings for an array of texts using Cloudflare Workers AI.
 * Each text is trimmed to the model's input limit, which is also what keeps a
 * batch inside the model's context window. Deduplicates inputs so each unique
 * text is only embedded once, then maps results back to the original positions.
 *
 * Requires CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN environment variables.
 */
export async function embedBatchCloudflare(
  texts: string[],
  model = "@cf/baai/bge-base-en-v1.5",
): Promise<number[][]> {
  if (texts.length === 0) return [];

  const trimmed = await tokenizeBatch(texts, TOKENIZER, MAX_INPUT_TOKENS);
  const uniqueTexts = [...new Set(trimmed)];

  const accountId = requireEnv("CLOUDFLARE_ACCOUNT_ID");
  const apiToken = requireEnv("CLOUDFLARE_API_TOKEN");

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

  return trimmed.map((text) => uniqueResults.get(text)!);
}
