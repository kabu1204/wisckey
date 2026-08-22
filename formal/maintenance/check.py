"""Command-line entry point for the checked formal scenarios."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .model import CHECKED_STEPS, Scenario, verify
from .policy import POLICY_NAME, infer_minimal_guard


def _trace_document(scenario: Scenario, result) -> dict:
    return {
        "scenario": scenario.value,
        "solver_result": result.solver_result,
        "violation": result.violation,
        "steps": [
            {"index": index, "action": action}
            for index, action in enumerate(result.trace)
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trace-dir", type=Path)
    args = parser.parse_args()

    inferred = infer_minimal_guard()
    if inferred != ("distinct_source",):
        raise AssertionError(f"unexpected inferred policy: {inferred}")

    results = {scenario: verify(scenario, CHECKED_STEPS) for scenario in Scenario}
    if not results[Scenario.SAFE].safe:
        raise AssertionError(f"safe model failed: {results[Scenario.SAFE]}")
    for scenario in Scenario:
        if scenario == Scenario.SAFE:
            continue
        if results[scenario].safe or results[scenario].solver_result != "sat":
            raise AssertionError(f"negative control did not produce SAT: {scenario}")

    if args.trace_dir is not None:
        args.trace_dir.mkdir(parents=True, exist_ok=True)
        for scenario, result in results.items():
            if scenario == Scenario.SAFE:
                continue
            output = args.trace_dir / f"{scenario.value}.json"
            output.write_text(
                json.dumps(_trace_document(scenario, result), indent=2) + "\n",
                encoding="utf-8",
            )

    print(f"policy={POLICY_NAME} guard={' + '.join(inferred)}")
    print(f"safe steps={CHECKED_STEPS} result=unsat")
    for scenario, result in results.items():
        if scenario != Scenario.SAFE:
            print(
                f"negative={scenario.value} result={result.solver_result} "
                f"violation={result.violation}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
