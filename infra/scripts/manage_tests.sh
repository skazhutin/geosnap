#!/usr/bin/env bash
set -euo pipefail

PYTHONPATH=. python - <<'PY'
import hashlib
import importlib.util
import pathlib
import sys
import unittest

root = pathlib.Path(".")
exclude_parts = {".git", ".venv", "venv", "node_modules", ".mypy_cache", ".pytest_cache", "dist", "build"}

test_files: list[pathlib.Path] = []
for test_file in root.rglob("test*.py"):
    if not test_file.is_file():
        continue
    if "tests" not in test_file.parts:
        continue
    if any(part in exclude_parts for part in test_file.parts):
        continue
    test_files.append(test_file)

test_files = sorted(set(test_files))
if not test_files:
    print("No Python test files found")
    sys.exit(1)

loader = unittest.TestLoader()
master_suite = unittest.TestSuite()

for file_path in test_files:
    module_name = f"_autotest_{hashlib.md5(str(file_path).encode(), usedforsecurity=False).hexdigest()}"
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load test file: {file_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    master_suite.addTests(loader.loadTestsFromModule(module))

count = master_suite.countTestCases()
print(f"Discovered {count} tests across {len(test_files)} test files", flush=True)

runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
result = runner.run(master_suite)
if not result.wasSuccessful():
    sys.exit(1)
PY
