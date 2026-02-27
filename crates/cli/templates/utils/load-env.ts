import { config } from "dotenv";
import { parse } from "smol-toml";
import { resolve, dirname } from "path";
import { readFileSync, existsSync } from "fs";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const root = resolve(__dirname, "..");

const tomlPath = resolve(root, "puffgres.toml");
const toml = parse(readFileSync(tomlPath, "utf-8"));
const envFiles = (toml.environment_files as string[]) ?? [];

for (const envFile of envFiles) {
  const envPath = resolve(root, envFile);
  if (existsSync(envPath)) {
    config({ path: envPath, quiet: true });
  }
}
