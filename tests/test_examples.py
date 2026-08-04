from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path
from types import ModuleType


REPO_ROOT = Path(__file__).resolve().parents[1]
GENERATOR_PATH = REPO_ROOT / "scripts" / "generate_examples.py"


def _load_generator() -> ModuleType:
    assert GENERATOR_PATH.is_file(), (
        "scripts/generate_examples.py must generate the tracked offline examples"
    )
    spec = importlib.util.spec_from_file_location("generate_examples", GENERATOR_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_tracked_examples_equal_deterministic_generator_output() -> None:
    generator = _load_generator()

    execution_log, remediation_report = asyncio.run(generator.generate_examples())

    assert execution_log == (
        REPO_ROOT / "examples" / "execution_log.txt"
    ).read_text(encoding="utf-8")
    assert remediation_report == (
        REPO_ROOT / "examples" / "remediation_report.json"
    ).read_text(encoding="utf-8")
    assert execution_log.endswith("\n")
    assert remediation_report.endswith("\n")
    assert all(
        phase in execution_log for phase in ("OBSERVE", "DECIDE", "ACT", "PERSIST")
    )
    assert '"timestamp": "2026-08-04T12:00:00Z"' in remediation_report
