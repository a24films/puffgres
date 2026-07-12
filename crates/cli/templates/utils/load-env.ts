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

/** Absolute paths of the env files listed in puffgres.toml's `environment_files`. */
export const envFilePaths = envFiles.map((envFile) => resolve(root, envFile));

for (const envPath of envFilePaths) {
  if (existsSync(envPath)) {
    config({ path: envPath, quiet: true });
  }
}

/**
 * Read a required environment variable. If it is missing or empty, throw an
 * error that names the variable and points at the env files configured for
 * this project, with absolute paths that most terminals render as clickable
 * links.
 */
export function requireEnv(name: string): string {
  const value = process.env[name];
  if (value !== undefined && value !== "") return value;

  const hint =
    envFilePaths.length > 0
      ? `Add ${name}=... to one of the env files configured in ${tomlPath} (environment_files):\n` +
        envFilePaths.map((path) => `  ${path}`).join("\n")
      : `No environment_files are configured. Add one in ${tomlPath}, then set ${name} there.`;

  throw new Error(`Missing required environment variable ${name}.\n${hint}`);
}
