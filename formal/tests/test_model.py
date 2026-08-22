from maintenance.model import CHECKED_STEPS, Scenario, verify
from maintenance.policy import (
    can_run_concurrently,
    infer_minimal_guard,
    unsafe_policy_has_counterexample,
)


def test_safe_bounded_model_is_unsat() -> None:
    result = verify(Scenario.SAFE, CHECKED_STEPS)
    assert result.safe
    assert result.solver_result == "unsat"


def test_negative_controls_produce_counterexamples() -> None:
    for scenario in (
        Scenario.UNCONDITIONAL_UPDATE,
        Scenario.SAME_SOURCE,
        Scenario.EARLY_MANIFEST,
        Scenario.UNSAFE_RECLAIM,
    ):
        result = verify(scenario, CHECKED_STEPS)
        assert not result.safe, scenario
        assert result.solver_result == "sat", scenario
        assert result.violation is not None
        assert result.trace


def test_inferred_policy_matches_cpp_guard() -> None:
    assert infer_minimal_guard() == ("distinct_source",)
    assert unsafe_policy_has_counterexample()
    assert not can_run_concurrently(0, 3)
    assert not can_run_concurrently(3, 0)
    assert not can_run_concurrently(3, 3)
    assert can_run_concurrently(3, 4)
