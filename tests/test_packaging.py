from __future__ import annotations

import shutil
import subprocess
import sys
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_wheel_contains_every_runtime_module(tmp_path: Path) -> None:
    project = tmp_path / "project"
    source = project / "src"
    wheelhouse = tmp_path / "wheelhouse"
    project.mkdir()
    shutil.copy2(ROOT / "pyproject.toml", project / "pyproject.toml")
    shutil.copy2(ROOT / "README.md", project / "README.md")
    shutil.copytree(
        ROOT / "src",
        source,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "wheel",
            "--no-deps",
            "--no-build-isolation",
            "--wheel-dir",
            str(wheelhouse),
            ".",
        ],
        cwd=project,
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    wheels = list(wheelhouse.glob("*.whl"))
    assert len(wheels) == 1
    expected_modules = {
        module.name for module in (ROOT / "src").glob("*.py")
    }
    assert expected_modules
    with zipfile.ZipFile(wheels[0]) as wheel:
        archived_files = set(wheel.namelist())
    missing = sorted(expected_modules - archived_files)
    assert not missing, f"wheel omitted runtime modules: {missing}"
