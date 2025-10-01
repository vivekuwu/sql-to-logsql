import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table.tsx";
import { useMemo } from "react";

export interface QueryResultsTableProps {
  readonly data?: unknown;
}

export function QueryResultsTable({ data }: QueryResultsTableProps) {
  const rows = useMemo(() => extractRows(data), [data]);

  if (!rows || rows.length === 0) {
    return (
      <div className="table-wrapper">
        <div className="text-sm text-slate-400">
          No tabular rows to display.
        </div>
      </div>
    );
  }

  const columns = Array.from(
    rows.reduce((set, row) => {
      Object.keys(row || {}).forEach((key) => set.add(key));
      return set;
    }, new Set<string>()),
  );

  return (
    <Table>
      <TableHeader>
        <TableRow>
          {columns.map((col) => (
            <TableHead key={col}>{col}</TableHead>
          ))}
        </TableRow>
      </TableHeader>
      <TableBody>
        {(!rows || rows.length === 0) && (
          <div className="text-sm text-slate-400">
            No tabular rows to display.
          </div>
        )}
        {rows.map((row, idx) => (
          <TableRow key={idx}>
            {columns.map((col) => (
              <TableCell key={col}>{row[col] as any}</TableCell>
            ))}
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}

function extractRows(value: unknown): Array<Record<string, unknown>> {
  if (typeof value === "string") {
    value = JSON.parse(
      "[" + value.replaceAll("\n", ",").replace(/,$/, "") + "]",
    );
  } else {
    value = [value];
  }
  if (!value) return [];
  if (Array.isArray(value)) return value as Array<Record<string, unknown>>;
  if (typeof value === "object") {
    const asRecord = value as Record<string, unknown>;
    if (Array.isArray(asRecord.data))
      return asRecord.data as Array<Record<string, unknown>>;
    if (Array.isArray(asRecord.result))
      return asRecord.result as Array<Record<string, unknown>>;
  }
  return [];
}
