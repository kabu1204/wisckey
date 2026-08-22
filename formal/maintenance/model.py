"""A bounded transition model for two concurrent value-log GC jobs.

The model is deliberately finite: two keys, two source files, two replacement
files, and at most 24 atomic transitions. It mirrors the durability ordering
and exact-handle callback used by db/blob_vlog_gc.cc.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum, StrEnum

from z3 import (
    And,
    Bool,
    BoolRef,
    BoolVal,
    If,
    Implies,
    Int,
    Or,
    PbLe,
    Solver,
    is_true,
    sat,
    unsat,
)

MAX_STEPS = 24
CHECKED_STEPS = 20
KEYS = 2
JOBS = 2
FILES = 5  # 0 is inline/deleted; 1-2 are sources; 3-4 are replacements.


class Scenario(StrEnum):
    SAFE = "safe"
    UNCONDITIONAL_UPDATE = "unconditional-update"
    SAME_SOURCE = "same-source"
    EARLY_MANIFEST = "early-manifest"
    UNSAFE_RECLAIM = "unsafe-reclaim"


class Action(IntEnum):
    NOOP = 0
    RESERVE_0 = 1
    COPY_0 = 2
    SYNC_REPLACEMENT_0 = 3
    UPDATE_LSM_0 = 4
    SYNC_LSM_0 = 5
    INSTALL_MANIFEST_0 = 6
    RECLAIM_0 = 7
    COMPLETE_0 = 8
    RESERVE_1 = 9
    COPY_1 = 10
    SYNC_REPLACEMENT_1 = 11
    UPDATE_LSM_1 = 12
    SYNC_LSM_1 = 13
    INSTALL_MANIFEST_1 = 14
    RECLAIM_1 = 15
    COMPLETE_1 = 16
    PUT_0 = 17
    PUT_1 = 18
    DELETE_0 = 19
    DELETE_1 = 20
    SNAPSHOT_0 = 21
    SNAPSHOT_1 = 22
    RELEASE_SNAPSHOT_0 = 23
    RELEASE_SNAPSHOT_1 = 24
    SYNC_FOREGROUND = 25
    CRASH_RECOVER = 26


@dataclass(frozen=True)
class ModelConfig:
    exact_handle_update: bool = True
    enforce_source_guard: bool = True
    ordered_manifest: bool = True
    snapshot_reclaim_guard: bool = True
    sources: tuple[int, int] = (1, 2)


@dataclass(frozen=True)
class VerificationResult:
    safe: bool
    solver_result: str
    violation: str | None = None
    trace: tuple[str, ...] = ()


@dataclass
class State:
    phase: list
    spec_present: list
    spec_version: list
    lsm_present: list
    lsm_version: list
    lsm_file: list
    lsm_offset: list
    lsm_size: list
    durable_present: list
    durable_version: list
    durable_file: list
    durable_offset: list
    durable_size: list
    snapshot_active: list
    snapshot_present: list
    snapshot_version: list
    snapshot_file: list
    file_durable: list
    obsolete: list
    tracked: list


def _scenario_config(scenario: Scenario) -> ModelConfig:
    if scenario == Scenario.SAFE:
        return ModelConfig()
    if scenario == Scenario.UNCONDITIONAL_UPDATE:
        return ModelConfig(exact_handle_update=False)
    if scenario == Scenario.SAME_SOURCE:
        return ModelConfig(enforce_source_guard=False, sources=(1, 1))
    if scenario == Scenario.EARLY_MANIFEST:
        return ModelConfig(ordered_manifest=False)
    if scenario == Scenario.UNSAFE_RECLAIM:
        return ModelConfig(snapshot_reclaim_guard=False)
    raise AssertionError(f"unknown scenario: {scenario}")


def _state(step: int) -> State:
    def bools(name: str, count: int) -> list[BoolRef]:
        return [Bool(f"{name}_{step}_{index}") for index in range(count)]

    def ints(name: str, count: int) -> list:
        return [Int(f"{name}_{step}_{index}") for index in range(count)]

    return State(
        phase=ints("phase", JOBS),
        spec_present=bools("spec_present", KEYS),
        spec_version=ints("spec_version", KEYS),
        lsm_present=bools("lsm_present", KEYS),
        lsm_version=ints("lsm_version", KEYS),
        lsm_file=ints("lsm_file", KEYS),
        lsm_offset=ints("lsm_offset", KEYS),
        lsm_size=ints("lsm_size", KEYS),
        durable_present=bools("durable_present", KEYS),
        durable_version=ints("durable_version", KEYS),
        durable_file=ints("durable_file", KEYS),
        durable_offset=ints("durable_offset", KEYS),
        durable_size=ints("durable_size", KEYS),
        snapshot_active=bools("snapshot_active", KEYS),
        snapshot_present=bools("snapshot_present", KEYS),
        snapshot_version=ints("snapshot_version", KEYS),
        snapshot_file=ints("snapshot_file", KEYS),
        file_durable=bools("file_durable", FILES),
        obsolete=bools("obsolete", FILES),
        tracked=bools("tracked", FILES),
    )


def _active(phase) -> BoolRef:
    return And(phase >= 1, phase <= 6)


def _job_action(job: int, offset: int) -> Action:
    return Action(1 + job * 8 + offset)


def _if_action(action, selected: Action, changed, unchanged):
    return If(action == int(selected), changed, unchanged)


def _initial_constraints(state: State) -> list:
    constraints = []
    constraints.extend(phase == 0 for phase in state.phase)
    for key in range(KEYS):
        source = key + 1
        offset = key + 1
        constraints.extend(
            [
                state.spec_present[key],
                state.spec_version[key] == 0,
                state.lsm_present[key],
                state.lsm_version[key] == 0,
                state.lsm_file[key] == source,
                state.lsm_offset[key] == offset,
                state.lsm_size[key] == 1,
                state.durable_present[key],
                state.durable_version[key] == 0,
                state.durable_file[key] == source,
                state.durable_offset[key] == offset,
                state.durable_size[key] == 1,
                state.snapshot_active[key] == BoolVal(False),
                state.snapshot_present[key] == BoolVal(False),
                state.snapshot_version[key] == 0,
                state.snapshot_file[key] == 0,
            ]
        )
    for file_number in range(FILES):
        initially_durable = file_number in (0, 1, 2)
        initially_tracked = file_number in (1, 2)
        constraints.extend(
            [
                state.file_durable[file_number] == initially_durable,
                state.obsolete[file_number] == BoolVal(False),
                state.tracked[file_number] == initially_tracked,
            ]
        )
    return constraints


def _transition_constraints(
    current: State, following: State, action, config: ModelConfig
) -> list:
    constraints = [action >= int(Action.NOOP), action <= int(Action.CRASH_RECOVER)]

    for job in range(JOBS):
        other = 1 - job
        source = config.sources[job]
        reserve = _job_action(job, 0)
        source_available = Or(
            BoolVal(not config.enforce_source_guard),
            config.sources[other] != source,
            ~_active(current.phase[other]),
        )
        constraints.append(
            Implies(
                action == int(reserve), And(current.phase[job] == 0, source_available)
            )
        )
        constraints.append(
            Implies(action == int(_job_action(job, 1)), current.phase[job] == 1)
        )
        constraints.append(
            Implies(action == int(_job_action(job, 2)), current.phase[job] == 2)
        )
        constraints.append(
            Implies(action == int(_job_action(job, 3)), current.phase[job] == 3)
        )
        constraints.append(
            Implies(action == int(_job_action(job, 4)), current.phase[job] == 4)
        )
        manifest_phase = (
            current.phase[job] == 5
            if config.ordered_manifest
            else Or(
                current.phase[job] == 2,
                current.phase[job] == 3,
                current.phase[job] == 5,
            )
        )
        constraints.append(Implies(action == int(_job_action(job, 5)), manifest_phase))
        snapshot_safe = And(
            *[
                Or(
                    ~current.snapshot_active[key],
                    ~current.snapshot_present[key],
                    current.snapshot_file[key] != source,
                )
                for key in range(KEYS)
            ]
        )
        reclaim_safe = snapshot_safe if config.snapshot_reclaim_guard else BoolVal(True)
        constraints.append(
            Implies(
                action == int(_job_action(job, 6)),
                And(current.phase[job] == 6, current.obsolete[source], reclaim_safe),
            )
        )
        constraints.append(
            Implies(action == int(_job_action(job, 7)), current.phase[job] == 7)
        )

    for key in range(KEYS):
        constraints.append(
            Implies(
                action == int(Action.SNAPSHOT_0) + key,
                ~current.snapshot_active[key],
            )
        )
        constraints.append(
            Implies(
                action == int(Action.RELEASE_SNAPSHOT_0) + key,
                current.snapshot_active[key],
            )
        )

    crash = action == int(Action.CRASH_RECOVER)

    for job in range(JOBS):
        phase_expr = current.phase[job]
        for offset in range(8):
            selected = _job_action(job, offset)
            next_phase = 6 if offset == 5 else offset + 1
            phase_expr = _if_action(action, selected, next_phase, phase_expr)
        phase_expr = If(crash, 8, phase_expr)
        constraints.append(following.phase[job] == phase_expr)

    for key in range(KEYS):
        put = action == int(Action.PUT_0) + key
        delete = action == int(Action.DELETE_0) + key
        acquire = action == int(Action.SNAPSHOT_0) + key
        release = action == int(Action.RELEASE_SNAPSHOT_0) + key

        spec_present = If(put, True, If(delete, False, current.spec_present[key]))
        spec_version = If(
            Or(put, delete), current.spec_version[key] + 1, current.spec_version[key]
        )

        present = If(put, True, If(delete, False, current.lsm_present[key]))
        version = If(
            Or(put, delete), current.spec_version[key] + 1, current.lsm_version[key]
        )
        file_number = If(Or(put, delete), 0, current.lsm_file[key])
        offset = If(Or(put, delete), 0, current.lsm_offset[key])
        size = If(Or(put, delete), 0, current.lsm_size[key])

        for job in range(JOBS):
            source = config.sources[job]
            replacement = job + 3
            expected_offset = job + 1
            exact_match = And(
                current.lsm_present[key],
                current.lsm_file[key] == source,
                current.lsm_offset[key] == expected_offset,
                current.lsm_size[key] == 1,
            )
            owns_record = key == job
            apply_update = And(
                BoolVal(owns_record),
                exact_match if config.exact_handle_update else BoolVal(True),
            )
            update = action == int(_job_action(job, 3))
            present = If(And(update, apply_update), True, present)
            version = If(And(update, apply_update), 0, version)
            file_number = If(And(update, apply_update), replacement, file_number)
            offset = If(And(update, apply_update), expected_offset, offset)
            size = If(And(update, apply_update), 1, size)

        sync = Or(
            action == int(Action.SYNC_FOREGROUND),
            action == int(Action.SYNC_LSM_0),
            action == int(Action.SYNC_LSM_1),
        )
        durable_present = If(sync, present, current.durable_present[key])
        durable_version = If(sync, version, current.durable_version[key])
        durable_file = If(sync, file_number, current.durable_file[key])
        durable_offset = If(sync, offset, current.durable_offset[key])
        durable_size = If(sync, size, current.durable_size[key])

        constraints.extend(
            [
                following.spec_present[key] == If(crash, durable_present, spec_present),
                following.spec_version[key] == If(crash, durable_version, spec_version),
                following.lsm_present[key] == If(crash, durable_present, present),
                following.lsm_version[key] == If(crash, durable_version, version),
                following.lsm_file[key] == If(crash, durable_file, file_number),
                following.lsm_offset[key] == If(crash, durable_offset, offset),
                following.lsm_size[key] == If(crash, durable_size, size),
                following.durable_present[key] == durable_present,
                following.durable_version[key] == durable_version,
                following.durable_file[key] == durable_file,
                following.durable_offset[key] == durable_offset,
                following.durable_size[key] == durable_size,
                following.snapshot_active[key]
                == If(
                    crash,
                    False,
                    If(acquire, True, If(release, False, current.snapshot_active[key])),
                ),
                following.snapshot_present[key]
                == If(acquire, current.lsm_present[key], current.snapshot_present[key]),
                following.snapshot_version[key]
                == If(acquire, current.lsm_version[key], current.snapshot_version[key]),
                following.snapshot_file[key]
                == If(acquire, current.lsm_file[key], current.snapshot_file[key]),
            ]
        )

    for file_number in range(FILES):
        durable = current.file_durable[file_number]
        obsolete = current.obsolete[file_number]
        tracked = current.tracked[file_number]
        for job in range(JOBS):
            source = config.sources[job]
            replacement = job + 3
            durable = (
                _if_action(
                    action,
                    _job_action(job, 2),
                    BoolVal(file_number == replacement),
                    durable,
                )
                if file_number == replacement
                else durable
            )
            durable = (
                _if_action(
                    action,
                    _job_action(job, 6),
                    BoolVal(False),
                    durable,
                )
                if file_number == source
                else durable
            )
            obsolete = (
                _if_action(
                    action,
                    _job_action(job, 5),
                    BoolVal(True),
                    obsolete,
                )
                if file_number == source
                else obsolete
            )
            tracked = (
                _if_action(
                    action,
                    _job_action(job, 5),
                    BoolVal(True),
                    tracked,
                )
                if file_number == replacement
                else tracked
            )
        constraints.extend(
            [
                following.file_durable[file_number] == durable,
                following.obsolete[file_number] == obsolete,
                following.tracked[file_number] == tracked,
            ]
        )

    return constraints


def _violations(state: State, config: ModelConfig) -> list[tuple[str, BoolRef]]:
    logical_mismatch = Or(
        *[
            Or(
                state.lsm_present[key] != state.spec_present[key],
                state.lsm_version[key] != state.spec_version[key],
            )
            for key in range(KEYS)
        ]
    )
    dangling_current = Or(
        *[
            And(
                state.lsm_present[key],
                state.lsm_file[key] > 0,
                ~_select_list(state.file_durable, state.lsm_file[key]),
            )
            for key in range(KEYS)
        ]
    )
    dangling_durable = Or(
        *[
            And(
                state.durable_present[key],
                state.durable_file[key] > 0,
                ~_select_list(state.file_durable, state.durable_file[key]),
            )
            for key in range(KEYS)
        ]
    )
    dangling_snapshot = Or(
        *[
            And(
                state.snapshot_active[key],
                state.snapshot_present[key],
                state.snapshot_file[key] > 0,
                ~_select_list(state.file_durable, state.snapshot_file[key]),
            )
            for key in range(KEYS)
        ]
    )
    duplicate_source = And(
        _active(state.phase[0]),
        _active(state.phase[1]),
        config.sources[0] == config.sources[1],
    )
    publication_order = Or(
        *[
            And(
                state.obsolete[config.sources[job]],
                Or(
                    ~state.file_durable[job + 3],
                    And(
                        state.durable_present[job],
                        state.durable_file[job] == config.sources[job],
                    ),
                ),
            )
            for job in range(JOBS)
        ]
    )
    return [
        ("logical-value-mismatch", logical_mismatch),
        ("dangling-current-handle", dangling_current),
        ("dangling-durable-handle", dangling_durable),
        ("reclaimed-snapshot-handle", dangling_snapshot),
        ("duplicate-active-source", duplicate_source),
        ("manifest-before-replacement-sync", publication_order),
    ]


def _select_list(items: list, index):
    expression = items[0]
    for item_index, item in enumerate(items[1:], start=1):
        expression = If(index == item_index, item, expression)
    return expression


def verify(scenario: Scenario, steps: int = CHECKED_STEPS) -> VerificationResult:
    if steps < 1 or steps > MAX_STEPS:
        raise ValueError(f"steps must be in [1, {MAX_STEPS}]")
    config = _scenario_config(scenario)
    states = [_state(step) for step in range(steps + 1)]
    actions = [Int(f"action_{step}") for step in range(steps)]
    solver = Solver()
    solver.set(timeout=10_000, random_seed=0)
    solver.add(*_initial_constraints(states[0]))
    for step in range(steps):
        solver.add(
            *_transition_constraints(
                states[step], states[step + 1], actions[step], config
            )
        )
        if step + 1 < steps:
            solver.add(
                Implies(
                    actions[step] == int(Action.NOOP),
                    actions[step + 1] == int(Action.NOOP),
                )
            )
            # Crash/recovery is a terminal observation point. Any operation
            # after recovery starts a fresh bounded prefix from that recovered
            # state and does not need to be duplicated in this trace.
            solver.add(
                Implies(
                    actions[step] == int(Action.CRASH_RECOVER),
                    actions[step + 1] == int(Action.NOOP),
                )
            )

    # A bounded trace contains at most one external mutation, snapshot event,
    # durability barrier, and crash of each kind. GC phase actions are already
    # single-use because their phase machine is monotonic.
    single_use = (
        Action.PUT_0,
        Action.PUT_1,
        Action.DELETE_0,
        Action.DELETE_1,
        Action.SNAPSHOT_0,
        Action.SNAPSHOT_1,
        Action.RELEASE_SNAPSHOT_0,
        Action.RELEASE_SNAPSHOT_1,
        Action.SYNC_FOREGROUND,
        Action.CRASH_RECOVER,
    )
    for selected in single_use:
        solver.add(PbLe([(action == int(selected), 1) for action in actions], 1))
    for key in range(KEYS):
        solver.add(
            PbLe(
                [(action == int(Action.PUT_0) + key, 1) for action in actions]
                + [(action == int(Action.DELETE_0) + key, 1) for action in actions],
                1,
            )
        )

    violations: list[tuple[int, str, BoolRef]] = []
    for step, state in enumerate(states):
        violations.extend(
            (step, name, expr) for name, expr in _violations(state, config)
        )
    solver.add(Or(*(expr for _, _, expr in violations)))
    result = solver.check()
    if result == unsat:
        return VerificationResult(safe=True, solver_result="unsat")
    if result != sat:
        return VerificationResult(
            safe=False, solver_result=str(result), violation="solver-timeout"
        )

    model = solver.model()
    first_step = 0
    first_violation = "unknown"
    for step, name, expr in violations:
        if is_true(model.eval(expr, model_completion=True)):
            first_step = step
            first_violation = name
            break
    trace = tuple(
        Action(model.eval(action, model_completion=True).as_long()).name
        for action in actions[:first_step]
    )
    return VerificationResult(
        safe=False,
        solver_result="sat",
        violation=first_violation,
        trace=trace,
    )
