"""Plain-text summary of a pipeline run."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from pipeline import db
from pipeline.config import Settings


def _table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    cells = [[_cell(v) for v in row] for row in rows]
    widths = [max(len(h), *(len(r[i]) for r in cells)) if cells else len(h) for i, h in enumerate(headers)]
    numeric = [all(isinstance(row[i], (int, float)) or row[i] is None for row in rows) for i in range(len(headers))]

    def fmt(values: Sequence[str]) -> str:
        padded = (v.rjust(w) if n else v.ljust(w) for v, w, n in zip(values, widths, numeric, strict=True))
        return "  ".join(padded).rstrip()

    lines = [fmt(headers), "  ".join("-" * w for w in widths)]
    lines.extend(fmt(r) for r in cells)
    return "\n".join(lines)


def _cell(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "PASS" if value else "FAIL"
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        return f"{value:,.0f}" if value.is_integer() else f"{value:,.4f}"
    return str(value)


def render(settings: Settings, run_id: int | None = None) -> str:
    with db.connect(settings) as conn:
        if run_id is None:
            row = conn.execute("SELECT max(run_id) FROM etl.pipeline_runs").fetchone()
            run_id = row[0] if row else None
        if run_id is None:
            return "No pipeline runs found."

        run = conn.execute(
            """SELECT run_id, status, load_mode, data_dir, started_at, finished_at,
                      extract(epoch FROM finished_at - started_at), error
               FROM etl.pipeline_runs WHERE run_id = %s""", (run_id,)).fetchone()
        if run is None:
            return f"Run {run_id} not found."

        stats = conn.execute(
            """SELECT entity, records_read, records_rejected_validation, duplicates,
                      records_rejected_integrity, records_accepted, records_upserted,
                      (extract_seconds + merge_seconds)::float
               FROM etl.load_stats WHERE run_id = %s
               ORDER BY array_position(ARRAY['subreddits','authors','submissions','comments'], entity)""",
            (run_id,)).fetchall()

        reasons = conn.execute(
            """SELECT entity, reason, count(*) FROM etl.rejected_records WHERE run_id = %s
               GROUP BY 1, 2 ORDER BY 1, 3 DESC""", (run_id,)).fetchall()

        checks = conn.execute(
            """SELECT check_name, severity, passed, observed::float, threshold::float
               FROM etl.quality_results WHERE run_id = %s ORDER BY severity, check_name""",
            (run_id,)).fetchall()

    _, status, mode, data_dir, started, finished, seconds, error = run
    out = [
        f"Run {run_id}: {status} ({mode} load from {data_dir})",
        f"Started {started:%Y-%m-%d %H:%M:%S %Z}" + (f", took {float(seconds):,.1f}s" if seconds else ""),
    ]
    if error:
        out.append(f"Error: {error}")

    if stats:
        totals = [sum(r[i] for r in stats) for i in range(1, 7)]
        total_seconds = sum(r[7] for r in stats)
        rows = [list(r) for r in stats] + [["total", *totals, total_seconds]]
        out += ["", "Load statistics", _table(
            ["entity", "read", "invalid", "duplicates", "orphaned", "accepted", "upserted", "seconds"], rows)]
        if total_seconds:
            out.append(f"Throughput: {totals[0] / total_seconds:,.0f} lines/s")

    if reasons:
        out += ["", "Rejections by reason", _table(["entity", "reason", "count"], reasons)]

    if checks:
        out += ["", "Quality checks", _table(["check", "severity", "result", "observed", "threshold"], checks)]

    return "\n".join(out)
