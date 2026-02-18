import Together from "together-ai";

let client: Together | null = null;

export function getTogetherClient(): Together {
  if (!client) {
    client = new Together({ apiKey: process.env.TOGETHER_API_KEY });
  }
  return client;
}

export function resetClient(): void {
  client = null;
}

export async function embed(
  texts: string[],
  model: string = "BAAI/bge-base-en-v1.5",
): Promise<number[][]> {
  if (texts.length === 0) return [];

  const together = getTogetherClient();
  const response = await together.embeddings.create({ model, input: texts });
  return response.data.map((item) => item.embedding);
}
