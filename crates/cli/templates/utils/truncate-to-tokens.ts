import { AutoTokenizer } from "@huggingface/transformers";

export async function truncateToTokens({
  modelName,
  maxTokens,
  inputText,
}: {
  modelName: string;
  maxTokens: number;
  inputText: string;
}): Promise<string> {
  const tokenizer = await AutoTokenizer.from_pretrained(modelName);
  const tokenIds = tokenizer.encode(inputText, { add_special_tokens: false });

  if (tokenIds.length <= maxTokens) {
    return inputText;
  }

  const truncated = tokenIds.slice(0, maxTokens);
  return tokenizer.decode(truncated, { skip_special_tokens: true });
}
