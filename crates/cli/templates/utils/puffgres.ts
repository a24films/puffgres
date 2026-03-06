export type PrimitiveType =
  | "string"
  | "uint"
  | "int"
  | "float"
  | "bool"
  | "uuid"
  | "json";

export type Column = { readonly name: string; readonly type: PrimitiveType };

function parseValue(type: PrimitiveType, val: string | null): unknown {
  if (val === null || val === undefined) return null;
  if (type === "uint" || type === "int" || type === "float") return Number(val);
  if (type === "bool") return val === "true";
  if (type === "json") return JSON.parse(val);
  return val;
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
