#!/usr/bin/env bash
set -euo pipefail

PYTHONPATH=. python - <<'PY'
import pathlib
import sys
import unittest

root = pathlib.Path(".")
exclude_parts = {".git", ".venv", "venv", "node_modules", ".mypy_cache", ".pytest_cache", "dist", "build"}

test_dirs = []
for p in root.rglob("tests"):
    if not p.is_dir():
        continue
    if any(part in exclude_parts for part in p.parts):
        continue
    test_dirs.append(p)

test_dirs = sorted(set(test_dirs))
if not test_dirs:
    print("No Python test directories found")
    sys.exit(1)

loader = unittest.TestLoader()
master_suite = unittest.TestSuite()

for test_dir in test_dirs:
    suite = loader.discover(start_dir=str(test_dir), pattern="test*.py")
    master_suite.addTests(suite)

count = master_suite.countTestCases()
print(f"Discovered {count} tests across {len(test_dirs)} test directories", flush=True)

runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
result = runner.run(master_suite)
if not result.wasSuccessful():
    sys.exit(1)
PY
