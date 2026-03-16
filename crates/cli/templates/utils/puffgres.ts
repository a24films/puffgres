export type PrimitiveType =
  | "string"
  | "uint"
  | "int"
  | "float"
  | "bool"
  | "uuid"
  | "datetime";

export type ArrayType =
  | "[]string"
  | "[]uint"
  | "[]int"
  | "[]float"
  | "[]bool"
  | "[]uuid"
  | "[]datetime";

export type ColumnType = PrimitiveType | ArrayType;

export type Column = { readonly name: string; readonly type: ColumnType };

function parseScalar(type: PrimitiveType, val: string): unknown {
  if (type === "uint" || type === "int" || type === "float") return Number(val);
  if (type === "bool") return val === "true";
  // datetime and uuid/string are passed through as-is (ISO 8601 strings)
  return val;
}

/**
 * Parse a PostgreSQL array literal like `{foo,bar,baz}` into string elements.
 * Handles quoted elements and escaped characters.
 */
function parsePgArray(val: string): string[] {
  if (!val.startsWith("{") || !val.endsWith("}")) return [val];
  const inner = val.slice(1, -1);
  if (inner === "") return [];

  const elements: string[] = [];
  let current = "";
  let inQuotes = false;
  let escaped = false;

  for (let i = 0; i < inner.length; i++) {
    const ch = inner[i];
    if (escaped) {
      current += ch;
      escaped = false;
    } else if (ch === "\\") {
      escaped = true;
    } else if (ch === '"') {
      inQuotes = !inQuotes;
    } else if (ch === "," && !inQuotes) {
      elements.push(current);
      current = "";
    } else {
      current += ch;
    }
  }
  elements.push(current);

  return elements.filter((e) => e !== "NULL");
}

function parseValue(type: ColumnType, val: string | null): unknown {
  if (val === null || val === undefined) return null;
  if (type.startsWith("[]")) {
    const elemType = type.slice(2) as PrimitiveType;
    return parsePgArray(val).map((elem) => parseScalar(elemType, elem));
  }
  return parseScalar(type as PrimitiveType, val);
}

export function parseRow(
  columns: readonly Column[],
  cols: (string | null)[],
): Record<string, unknown> {
  return Object.fromEntries(
    columns.map((col, i) => [col.name, parseValue(col.type, cols[i])])
  );
}

export function parseRows(
  columns: readonly Column[],
  rows: (string | null)[][],
): Record<string, unknown>[] {
  return rows.map((row) => parseRow(columns, row));
}
