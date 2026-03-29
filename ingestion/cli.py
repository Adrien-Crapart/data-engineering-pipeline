"""CLI entry point for the ingestion pipeline.

Supports multiple pipelines via --pipeline flag. Each pipeline module must
expose a ``run_pipeline(config)`` function.

Usage:
    python -m ingestion.cli --pipeline openweather
    python -m ingestion.cli --pipeline openweather --cities "Paris,Lyon"
"""

from __future__ import annotations

import argparse
import importlib
import logging
import os
import sys
import time

PIPELINES = {
    "openweather": "ingestion.pipelines.openweather_pipeline",
}

logger = logging.getLogger("ingestion")


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
        force=True,
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("dlt").setLevel(logging.INFO)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="DLT Ingestion Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--pipeline",
        default=os.getenv("INGESTION_PIPELINE", "openweather"),
        choices=list(PIPELINES.keys()),
        help="Pipeline to execute (default: openweather)",
    )
    parser.add_argument(
        "--cities",
        default=None,
        help="Comma-separated list of cities (overrides WEATHER_CITIES env var)",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Drop and recreate destination tables",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    _setup_logging()
    args = _parse_args(argv)

    logger.info("=== DLT Ingestion Pipeline ===")
    logger.info("  pipeline:  %s", args.pipeline)
    logger.info("  module:    %s", PIPELINES[args.pipeline])

    if args.cities:
        os.environ["WEATHER_CITIES"] = args.cities
        logger.info("  cities:    %s (CLI override)", args.cities)

    if args.full_refresh:
        os.environ["DLT_FULL_REFRESH"] = "1"
        logger.info("  mode:      full-refresh")

    module_path = PIPELINES[args.pipeline]
    try:
        mod = importlib.import_module(module_path)
    except ImportError as exc:
        logger.error("Failed to import pipeline module %s: %s", module_path, exc)
        return 1

    start = time.time()
    try:
        result = mod.run_pipeline()
        elapsed = time.time() - start
        logger.info("Pipeline completed in %.2fs — %s", elapsed, result)
        return 0
    except Exception:
        elapsed = time.time() - start
        logger.exception("Pipeline FAILED after %.2fs", elapsed)
        return 1


if __name__ == "__main__":
    sys.exit(main())
