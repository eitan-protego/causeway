"""Migration execution engine."""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from causeway.loader import ResolvedStep, discover
from causeway.state import MigrationHistoryEntry, StateStore

log = logging.getLogger(__name__)


@dataclass
class MigrationStatus:
    """Current migration state and pending steps."""

    current_version: int
    current_step: int
    pending: list[ResolvedStep]
    history: list[MigrationHistoryEntry] = field(default_factory=list)


async def migrate(
    store: StateStore[Any],
    migrations_path: Path,
    target_version: int | None = None,
    dry_run: bool = False,
) -> None:
    """Apply pending migrations up to target_version (default: all available)."""
    steps = discover(migrations_path)
    state = await store.read_state()

    pending = _pending_steps(steps, state.version, state.step, target_version)

    if not pending:
        log.info("No pending migrations")
        return

    for resolved in pending:
        label = f"v{resolved.version} step {resolved.step}: {resolved.name}"
        if dry_run:
            log.info(f"[dry run] Would apply migration {label}")
            continue

        log.info(f"Applying migration {label}")
        instance = resolved.cls()
        await instance.up(store.db)
        await store.update_state(resolved.version, resolved.step, resolved.name, "up")
        log.info(f"Applied migration {label}")


async def rollback(
    store: StateStore[Any],
    migrations_path: Path,
    target_version: int,
    dry_run: bool = False,
) -> None:
    """Roll back to target_version (inclusive — steps in target_version remain applied).

    Pre-validates that all steps to be rolled back have down() implementations.
    """
    steps = discover(migrations_path)
    state = await store.read_state()

    to_rollback = _rollback_steps(steps, state.version, state.step, target_version)

    if not to_rollback:
        log.info("Nothing to roll back")
        return

    # Pre-validate all steps are reversible before executing any
    irreversible = [r for r in to_rollback if not r.cls().has_down()]
    if irreversible:
        names = ", ".join(f"v{r.version} step {r.step}: {r.name}" for r in irreversible)
        raise NotImplementedError(f"Cannot roll back — irreversible steps: {names}")

    for resolved in to_rollback:
        label = f"v{resolved.version} step {resolved.step}: {resolved.name}"
        if dry_run:
            log.info(f"[dry run] Would roll back migration {label}")
            continue

        log.info(f"Rolling back migration {label}")
        instance = resolved.cls()
        await instance.down(store.db)

        # After rolling back, state = the step before this one
        prev = _step_before(steps, resolved)
        await store.update_state(prev[0], prev[1], resolved.name, "down")
        log.info(f"Rolled back migration {label}")


async def status(
    store: StateStore[Any],
    migrations_path: Path,
) -> MigrationStatus:
    """Return current migration state and list of pending steps."""
    steps = discover(migrations_path)
    state = await store.read_state()
    pending = _pending_steps(steps, state.version, state.step)
    return MigrationStatus(
        current_version=state.version,
        current_step=state.step,
        pending=pending,
        history=state.history,
    )


async def stamp(
    store: StateStore[Any],
    migrations_path: Path,
    version: int,
    step: int | None = None,
) -> None:
    """Forcibly set migration state without running any steps.

    If step is None, sets to the last step of the given version.
    version=0 resets to "no migrations applied".
    """
    if version == 0:
        await store.stamp_state(0, 0)
        log.info("Stamped migration state to v0 (no migrations)")
        return

    steps = discover(migrations_path)
    version_steps = [s for s in steps if s.version == version]
    if not version_steps:
        raise ValueError(f"No migration found for version {version}")

    if step is None:
        target = version_steps[-1]
    else:
        matching = [s for s in version_steps if s.step == step]
        if not matching:
            available = [s.step for s in version_steps]
            raise ValueError(
                f"No step {step} in version {version}. Available: {available}"
            )
        target = matching[0]

    await store.stamp_state(target.version, target.step)
    log.info(
        f"Stamped migration state to v{target.version} step {target.step}: "
        f"{target.name}"
    )


# --- Internal helpers ---


def _pending_steps(
    all_steps: list[ResolvedStep],
    current_version: int,
    current_step: int,
    target_version: int | None = None,
) -> list[ResolvedStep]:
    """Filter steps that haven't been applied yet."""
    pending = [
        s for s in all_steps if (s.version, s.step) > (current_version, current_step)
    ]
    if target_version is not None:
        pending = [s for s in pending if s.version <= target_version]
    return pending


def _rollback_steps(
    all_steps: list[ResolvedStep],
    current_version: int,
    current_step: int,
    target_version: int,
) -> list[ResolvedStep]:
    """Get steps to roll back (in reverse order) to reach target_version.

    Rolls back all steps with version > target_version.
    """
    to_rollback = [
        s
        for s in all_steps
        if (s.version, s.step) <= (current_version, current_step)
        and s.version > target_version
    ]
    return list(reversed(to_rollback))


def _step_before(
    all_steps: list[ResolvedStep], current: ResolvedStep
) -> tuple[int, int]:
    """Return (version, step) of the step before current, or (0, 0) if first."""
    for i, s in enumerate(all_steps):
        if s.version == current.version and s.step == current.step:
            if i == 0:
                return (0, 0)
            prev = all_steps[i - 1]
            return (prev.version, prev.step)
    return (0, 0)
