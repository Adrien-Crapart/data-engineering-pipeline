"""Allow running as ``python -m ingestion``."""

from ingestion.cli import main

raise SystemExit(main())
