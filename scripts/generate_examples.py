"""Regenerate the deterministic offline evidence artifacts."""

from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path

from rich.console import Console


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from datahub_context import FixtureDataHubContext  # noqa: E402
from demo_dashboard import run_demo  # noqa: E402


FIXED_NOW = datetime(2026, 8, 4, 12, 0, 0, tzinfo=timezone.utc)


async def generate_examples() -> tuple[str, str]:
    """Return the synchronized console log and exact report JSON."""

    sink = StringIO()
    console = Console(
        file=sink,
        record=True,
        force_terminal=False,
        color_system=None,
        width=120,
        legacy_windows=False,
    )
    report = await run_demo(
        console=console,
        context=FixtureDataHubContext(),
        clock=lambda: FIXED_NOW,
        delay=0,
    )

    rendered = console.export_text(styles=False)
    lines = rendered.splitlines()
    execution_log = "\n".join(line.rstrip(" ") for line in lines) + "\n"
    remediation_report = (
        json.dumps(
            report.to_dict(),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
    return execution_log, remediation_report


if __name__ == "__main__":
    log_text, report_text = asyncio.run(generate_examples())
    (REPO_ROOT / "examples" / "execution_log.txt").write_bytes(
        log_text.encode("utf-8")
    )
    (REPO_ROOT / "examples" / "remediation_report.json").write_bytes(
        report_text.encode("utf-8")
    )
