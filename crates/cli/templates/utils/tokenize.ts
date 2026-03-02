import {
  AutoTokenizer,
  type PreTrainedTokenizer,
} from "@huggingface/transformers";

const cachedTokenizers = new Map<string, PreTrainedTokenizer>();

async function getTokenizer(model: string): Promise<PreTrainedTokenizer> {
  let tokenizer = cachedTokenizers.get(model);
  if (!tokenizer) {
    tokenizer = await AutoTokenizer.from_pretrained(model);
    cachedTokenizers.set(model, tokenizer);
  }
  return tokenizer;
}

/**
 * Tokenize and truncate texts to the given max token length.
 * Deduplicates inputs so each unique text is only tokenized once,
 * then maps results back to the original positions.
 */
export async function tokenizeBatch(
  texts: string[],
  model: string,
  maxTokens: number,
): Promise<string[]> {
  if (texts.length === 0) return [];

  const uniqueTexts = [...new Set(texts)];
  const tokenizer = await getTokenizer(model);

  const uniqueResults = new Map(
    uniqueTexts.map((text) => {
      const encoded = tokenizer(text, {
        truncation: true,
        max_length: maxTokens,
      });
      return [
        text,
        tokenizer.decode(encoded.input_ids, { skip_special_tokens: true }),
      ];
    }),
  );

  return texts.map((text) => uniqueResults.get(text)!);
}
