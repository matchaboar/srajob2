#!/usr/bin/env python3
"""
Profile JSON and file I/O operations for test performance optimization.

Run with: uv run python scripts/profile_test_io.py
"""
from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any, Callable

# Add project root to path
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

# Test fixtures directory
FIXTURE_DIR = ROOT / "tests/job_scrape_application/workflows/fixtures/dbos_schedule"
ASSERTIONS_DIR = ROOT / "tests/job_scrape_application/workflows/assertions"
SCHEDULE_PATH = ROOT / "job_scrape_application/config/prod/site_schedules.yml"


def timeit(func: Callable, iterations: int = 100) -> tuple[float, Any]:
    """Time a function over multiple iterations, return (avg_ms, result)."""
    result = None
    start = time.perf_counter()
    for _ in range(iterations):
        result = func()
    elapsed = time.perf_counter() - start
    avg_ms = (elapsed / iterations) * 1000
    return avg_ms, result


def get_sample_fixtures() -> list[Path]:
    """Get a sample of fixture files for testing."""
    if not FIXTURE_DIR.exists():
        return []
    fixtures = list(FIXTURE_DIR.glob("*_detail.json"))
    return fixtures[:10]  # Sample of 10 fixtures


def get_sample_yaml_files() -> list[Path]:
    """Get a sample of YAML assertion files."""
    if not ASSERTIONS_DIR.exists():
        return []
    yamls = list(ASSERTIONS_DIR.glob("*.yml"))
    return yamls[:10]


# ============================================================================
# JSON Benchmarks
# ============================================================================

def benchmark_orjson_text(fixtures: list[Path]) -> dict[str, float]:
    """Benchmark orjson reading text inputs."""
    import orjson

    results = {}

    # Read benchmark
    def read_all():
        data = []
        for f in fixtures:
            content = f.read_text(encoding="utf-8")
            data.append(orjson.loads(content))
        return data

    avg_ms, parsed_data = timeit(read_all, iterations=20)
    results["orjson.loads (read text)"] = avg_ms

    # Write benchmark (to string, not file)
    def write_all():
        outputs = []
        for d in parsed_data:
            outputs.append(
                orjson.dumps(
                    d,
                    default=str,
                    option=orjson.OPT_INDENT_2,
                ).decode("utf-8")
            )
        return outputs

    avg_ms, _ = timeit(write_all, iterations=20)
    results["orjson.dumps (write text)"] = avg_ms

    return results


def benchmark_orjson_bytes(fixtures: list[Path]) -> dict[str, float]:
    """Benchmark orjson reading bytes inputs."""
    try:
        import orjson
    except ImportError:
        return {"error": "orjson not installed"}

    results = {}

    # Read benchmark
    def read_all():
        data = []
        for f in fixtures:
            content = f.read_bytes()
            data.append(orjson.loads(content))
        return data

    avg_ms, parsed_data = timeit(read_all, iterations=20)
    results["orjson.loads (read bytes)"] = avg_ms

    # Write benchmark
    def write_all():
        outputs = []
        for d in parsed_data:
            outputs.append(orjson.dumps(d, option=orjson.OPT_INDENT_2).decode())
        return outputs

    avg_ms, _ = timeit(write_all, iterations=20)
    results["orjson.dumps (write bytes)"] = avg_ms

    return results


def benchmark_json_ujson(fixtures: list[Path]) -> dict[str, float]:
    """Benchmark ujson."""
    try:
        import ujson
    except ImportError:
        return {"error": "ujson not installed"}

    results = {}

    # Read benchmark
    def read_all():
        data = []
        for f in fixtures:
            content = f.read_text(encoding="utf-8")
            data.append(ujson.loads(content))
        return data

    avg_ms, parsed_data = timeit(read_all, iterations=20)
    results["ujson.loads (read all)"] = avg_ms

    # Write benchmark
    def write_all():
        outputs = []
        for d in parsed_data:
            outputs.append(ujson.dumps(d, indent=2))
        return outputs

    avg_ms, _ = timeit(write_all, iterations=20)
    results["ujson.dumps (write all)"] = avg_ms

    return results


def benchmark_json_msgspec(fixtures: list[Path]) -> dict[str, float]:
    """Benchmark msgspec."""
    try:
        import msgspec.json
    except ImportError:
        return {"error": "msgspec not installed"}

    results = {}

    # Read benchmark
    def read_all():
        data = []
        for f in fixtures:
            content = f.read_bytes()
            data.append(msgspec.json.decode(content))
        return data

    avg_ms, parsed_data = timeit(read_all, iterations=20)
    results["msgspec.json.decode (read all)"] = avg_ms

    # Write benchmark (msgspec doesn't support indent directly)
    def write_all():
        outputs = []
        for d in parsed_data:
            outputs.append(msgspec.json.encode(d).decode())
        return outputs

    avg_ms, _ = timeit(write_all, iterations=20)
    results["msgspec.json.encode (write all)"] = avg_ms

    return results


# ============================================================================
# YAML Benchmarks
# ============================================================================

def benchmark_yaml_pyyaml(yamls: list[Path]) -> dict[str, float]:
    """Benchmark PyYAML."""
    import yaml

    results = {}

    def read_all():
        data = []
        for f in yamls:
            content = f.read_text(encoding="utf-8")
            data.append(yaml.safe_load(content))
        return data

    avg_ms, _ = timeit(read_all, iterations=50)
    results["yaml.safe_load (read all)"] = avg_ms

    return results


def benchmark_yaml_cyaml(yamls: list[Path]) -> dict[str, float]:
    """Benchmark PyYAML with C loader (if available)."""
    try:
        import yaml
        from yaml import CSafeLoader
    except ImportError:
        return {"error": "yaml C loader not available"}

    results = {}

    def read_all():
        data = []
        for f in yamls:
            content = f.read_text(encoding="utf-8")
            data.append(yaml.load(content, Loader=CSafeLoader))
        return data

    avg_ms, _ = timeit(read_all, iterations=50)
    results["yaml.load (CSafeLoader)"] = avg_ms

    return results


def benchmark_yaml_ruyaml(yamls: list[Path]) -> dict[str, float]:
    """Benchmark ruyaml (if available)."""
    try:
        from ruyaml import YAML
    except ImportError:
        return {"error": "ruyaml not installed"}

    results = {}
    yaml_parser = YAML(typ='safe')

    def read_all():
        data = []
        for f in yamls:
            with open(f, 'r') as fp:
                data.append(yaml_parser.load(fp))
        return data

    avg_ms, _ = timeit(read_all, iterations=50)
    results["ruyaml.load (safe)"] = avg_ms

    return results


# ============================================================================
# File I/O Benchmarks
# ============================================================================

def benchmark_file_pathlib(fixtures: list[Path]) -> dict[str, float]:
    """Benchmark pathlib file reading."""
    results = {}

    def read_all():
        data = []
        for f in fixtures:
            data.append(f.read_text(encoding="utf-8"))
        return data

    avg_ms, _ = timeit(read_all, iterations=50)
    results["Path.read_text()"] = avg_ms

    def read_bytes_all():
        data = []
        for f in fixtures:
            data.append(f.read_bytes())
        return data

    avg_ms, _ = timeit(read_bytes_all, iterations=50)
    results["Path.read_bytes()"] = avg_ms

    return results


def benchmark_file_builtin(fixtures: list[Path]) -> dict[str, float]:
    """Benchmark builtin open() file reading."""
    results = {}

    def read_all():
        data = []
        for f in fixtures:
            with open(f, 'r', encoding='utf-8') as fp:
                data.append(fp.read())
        return data

    avg_ms, _ = timeit(read_all, iterations=50)
    results["open().read()"] = avg_ms

    return results


def benchmark_file_mmap(fixtures: list[Path]) -> dict[str, float]:
    """Benchmark mmap file reading."""
    import mmap

    results = {}

    def read_all():
        data = []
        for f in fixtures:
            with open(f, 'rb') as fp:
                with mmap.mmap(fp.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                    data.append(mm.read())
        return data

    avg_ms, _ = timeit(read_all, iterations=50)
    results["mmap.read()"] = avg_ms

    return results


# ============================================================================
# Combined benchmark: realistic test scenario
# ============================================================================

def benchmark_realistic_load_fixture_orjson_text(fixtures: list[Path]) -> dict[str, float]:
    """Benchmark loading fixture with orjson text inputs - realistic scenario."""
    import orjson

    results = {}

    def load_fixture():
        data = []
        for f in fixtures:
            payload = orjson.loads(f.read_text(encoding="utf-8"))
            # Simulate what the test does - access response field
            response = payload.get("response", [])
            if isinstance(response, list) and response:
                for item in response:
                    if isinstance(item, str):
                        try:
                            orjson.loads(item)  # Parse JSONL items
                        except Exception:
                            pass
            data.append(payload)
        return data

    avg_ms, _ = timeit(load_fixture, iterations=20)
    results["orjson (text): load + parse JSONL"] = avg_ms

    return results


def benchmark_realistic_load_fixture_orjson(fixtures: list[Path]) -> dict[str, float]:
    """Benchmark loading fixture with orjson - realistic scenario."""
    try:
        import orjson
    except ImportError:
        return {"error": "orjson not installed"}

    results = {}

    def load_fixture():
        data = []
        for f in fixtures:
            payload = orjson.loads(f.read_bytes())
            response = payload.get("response", [])
            if isinstance(response, list) and response:
                for item in response:
                    if isinstance(item, str):
                        try:
                            orjson.loads(item.encode())
                        except Exception:
                            pass
            data.append(payload)
        return data

    avg_ms, _ = timeit(load_fixture, iterations=20)
    results["orjson: load + parse JSONL"] = avg_ms

    return results


def benchmark_realistic_load_fixture_msgspec(fixtures: list[Path]) -> dict[str, float]:
    """Benchmark loading fixture with msgspec - realistic scenario."""
    try:
        import msgspec.json
    except ImportError:
        return {"error": "msgspec not installed"}

    results = {}

    def load_fixture():
        data = []
        for f in fixtures:
            payload = msgspec.json.decode(f.read_bytes())
            response = payload.get("response", [])
            if isinstance(response, list) and response:
                for item in response:
                    if isinstance(item, str):
                        try:
                            msgspec.json.decode(item.encode())
                        except Exception:
                            pass
            data.append(payload)
        return data

    avg_ms, _ = timeit(load_fixture, iterations=20)
    results["msgspec: load + parse JSONL"] = avg_ms

    return results


def main():
    print("=" * 70)
    print("Test I/O Performance Profiler")
    print("=" * 70)

    fixtures = get_sample_fixtures()
    yamls = get_sample_yaml_files()

    print(f"\nUsing {len(fixtures)} fixture files and {len(yamls)} YAML files")

    if not fixtures:
        print("No fixture files found!")
        return

    # Show fixture sizes
    total_size = sum(f.stat().st_size for f in fixtures)
    print(f"Total fixture size: {total_size / 1024:.1f} KB")
    print()

    # JSON benchmarks
    print("-" * 70)
    print("JSON PARSING BENCHMARKS (lower is better)")
    print("-" * 70)

    json_benchmarks = [
        ("orjson (text)", benchmark_orjson_text),
        ("orjson (bytes)", benchmark_orjson_bytes),
        ("ujson", benchmark_json_ujson),
        ("msgspec", benchmark_json_msgspec),
    ]

    for name, bench_func in json_benchmarks:
        try:
            results = bench_func(fixtures)
            if "error" in results:
                print(f"\n{name}: {results['error']}")
            else:
                print(f"\n{name}:")
                for metric, ms in sorted(results.items()):
                    print(f"  {metric}: {ms:.3f} ms")
        except Exception as e:
            print(f"\n{name}: ERROR - {e}")

    # YAML benchmarks
    print("\n" + "-" * 70)
    print("YAML PARSING BENCHMARKS (lower is better)")
    print("-" * 70)

    yaml_benchmarks = [
        ("PyYAML (safe_load)", benchmark_yaml_pyyaml),
        ("PyYAML (CSafeLoader)", benchmark_yaml_cyaml),
        ("ruyaml", benchmark_yaml_ruyaml),
    ]

    for name, bench_func in yaml_benchmarks:
        try:
            results = bench_func(yamls)
            if "error" in results:
                print(f"\n{name}: {results['error']}")
            else:
                print(f"\n{name}:")
                for metric, ms in sorted(results.items()):
                    print(f"  {metric}: {ms:.3f} ms")
        except Exception as e:
            print(f"\n{name}: ERROR - {e}")

    # File I/O benchmarks
    print("\n" + "-" * 70)
    print("FILE I/O BENCHMARKS (lower is better)")
    print("-" * 70)

    file_benchmarks = [
        ("pathlib", benchmark_file_pathlib),
        ("builtin open()", benchmark_file_builtin),
        ("mmap", benchmark_file_mmap),
    ]

    for name, bench_func in file_benchmarks:
        try:
            results = bench_func(fixtures)
            print(f"\n{name}:")
            for metric, ms in sorted(results.items()):
                print(f"  {metric}: {ms:.3f} ms")
        except Exception as e:
            print(f"\n{name}: ERROR - {e}")

    # Realistic scenario benchmarks
    print("\n" + "-" * 70)
    print("REALISTIC SCENARIO: Load fixture + parse JSONL items")
    print("-" * 70)

    realistic_benchmarks = [
        ("orjson (text)", benchmark_realistic_load_fixture_orjson_text),
        ("orjson (bytes)", benchmark_realistic_load_fixture_orjson),
        ("msgspec", benchmark_realistic_load_fixture_msgspec),
    ]

    for name, bench_func in realistic_benchmarks:
        try:
            results = bench_func(fixtures)
            if "error" in results:
                print(f"\n{name}: {results['error']}")
            else:
                print(f"\n{name}:")
                for metric, ms in sorted(results.items()):
                    print(f"  {metric}: {ms:.3f} ms")
        except Exception as e:
            print(f"\n{name}: ERROR - {e}")

    print("\n" + "=" * 70)
    print("RECOMMENDATIONS")
    print("=" * 70)
    print("""
Based on typical results:
1. For JSON: orjson or msgspec are 3-10x faster than stdlib json
2. For YAML: CSafeLoader is ~2x faster than safe_load (if available)
3. For files: Path.read_bytes() is slightly faster than read_text()

To install faster JSON libraries:
  uv add orjson     # Fastest, Rust-based
  uv add msgspec    # Fast, also supports typed decoding

Note: The actual speedup depends on your fixture sizes and CPU.
""")


if __name__ == "__main__":
    main()
