"""Runtime policy mirrored by CanRunVLogGCConcurrently in C++."""

from itertools import combinations

from z3 import And, BoolVal, Int, Solver, sat, unsat

POLICY_NAME = "distinct-source-v1"


def can_run_concurrently(lhs_source: int, rhs_source: int) -> bool:
    """Return the checked pairwise GC scheduling policy."""
    return lhs_source != 0 and rhs_source != 0 and lhs_source != rhs_source


def _candidate_is_safe(features: tuple[str, ...]) -> bool:
    lhs = Int("policy_lhs_source")
    rhs = Int("policy_rhs_source")
    clauses = [lhs >= 1, lhs <= 2, rhs >= 1, rhs <= 2]
    guards = {
        "distinct_source": lhs != rhs,
        "ordered_source": lhs < rhs,
    }
    guard = And(*(guards[name] for name in features)) if features else BoolVal(True)
    solver = Solver()
    solver.set(timeout=10_000)
    solver.add(*clauses, guard, lhs == rhs)
    return solver.check() == unsat


def infer_minimal_guard() -> tuple[str, ...]:
    """Infer the first cardinality-minimal guard in stable feature order."""
    feature_order = ("distinct_source", "ordered_source")
    for width in range(len(feature_order) + 1):
        for candidate in combinations(feature_order, width):
            if _candidate_is_safe(candidate):
                return candidate
    raise AssertionError("candidate vocabulary contains no safe GC guard")


def unsafe_policy_has_counterexample() -> bool:
    lhs = Int("unsafe_policy_lhs")
    rhs = Int("unsafe_policy_rhs")
    solver = Solver()
    solver.set(timeout=10_000)
    solver.add(lhs >= 1, lhs <= 2, rhs >= 1, rhs <= 2, lhs == rhs)
    return solver.check() == sat
