from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from pipeline import db, generate, report, runner
from pipeline.config import load_settings

log = logging.getLogger("pipeline")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipeline", description="Reddit dump ingestion pipeline")
    parser.add_argument("-v", "--verbose", action="store_true", help="enable debug logging")
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate", help="write synthetic Pushshift-style dump files")
    gen.add_argument("--out", type=Path, default=Path("data"), help="output directory (default: data)")
    gen.add_argument("--scale", choices=sorted(generate.SCALES), default="small")
    gen.add_argument("--defect-rate", type=float, default=0.01, help="share of extra defective lines")
    gen.add_argument("--seed", type=int, default=42)
    gen.add_argument("--compress", choices=("none", "gz", "zst"), default="none")

    sub.add_parser("init-db", help="create schemas, tables, indexes and foreign keys")

    run = sub.add_parser("run", help="run the full extract-validate-load-check pipeline")
    run.add_argument("--data-dir", type=Path, default=Path("data"), help="directory with dump files")
    run.add_argument("--mode", choices=("auto", "bulk", "incremental"), default="auto",
                     help="bulk drops FKs/indexes during load and rebuilds them after; "
                          "auto picks bulk when the core tables are empty")

    sub.add_parser("check", help="re-run data quality checks for the latest run")

    rep = sub.add_parser("report", help="print load statistics and quality results")
    rep.add_argument("--run-id", type=int, help="run to report on (default: latest)")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger("psycopg").setLevel(logging.WARNING)

    if args.command == "generate":
        generate.generate(args.out, args.scale, args.defect_rate, args.seed, args.compress)
        return 0

    settings = load_settings()

    if args.command == "init-db":
        with db.connect(settings) as conn:
            db.apply_schema(conn)
        log.info("Database schema is ready")
        return 0

    if args.command == "run":
        try:
            outcome = runner.run(settings, args.data_dir, args.mode)
        except (runner.RunInProgressError, FileNotFoundError) as exc:
            log.error("%s", exc)
            return 2
        print()
        print(report.render(settings, outcome.run_id))
        return 0 if outcome.status == "succeeded" else 1

    if args.command == "check":
        results = runner.check_latest(settings)
        return 1 if any(not r.passed and r.check.severity == "error" for r in results) else 0

    print(report.render(settings, args.run_id))
    return 0


if __name__ == "__main__":
    sys.exit(main())
