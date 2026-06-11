"""Migration execution engine."""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, runtime_checkable


from causeway.loader import ResolvedStep, discover
from causeway.state import MigrationHistoryEntry, StateStore


@runtime_checkable
class Logger(Protocol):
    """Minimal logger interface compatible with stdlib logging and loguru."""

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> Any: ...
    def info(self, msg: str, *args: Any, **kwargs: Any) -> Any: ...
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> Any: ...
    def error(self, msg: str, *args: Any, **kwargs: Any) -> Any: ...


_logger: Logger = logging.getLogger(__name__)


def configure(*, logger: Logger) -> None:
    """Set the logger used by causeway's migration runner.

    Accepts any object with debug/info/warning/error methods (stdlib Logger,
    loguru, structlog, etc.).
    """
    global _logger
    _logger = logger


@dataclass
class MigrationStatus:
    """Current migration state and pending steps."""

    current_migration_id: str | None
    current_step: int
    pending: list[ResolvedStep]
    history: list[MigrationHistoryEntry] = field(default_factory=list)


async def migrate(
    store: StateStore[Any],
    migrations_path: Path,
    target: str | None = None,
    dry_run: bool = False,
) -> None:
    """Apply pending migrations up to target migration ID (default: all available)."""
    steps = discover(migrations_path)
    state = await store.read_state()

    pending = _pending_steps(steps, state.migration_id, state.step, target)

    if not pending:
        _logger.info("No pending migrations")
        return

    for resolved in pending:
        label = f"{resolved.migration_id} step {resolved.step}: {resolved.name}"
        if dry_run:
            _logger.info(f"[dry run] Would apply migration {label}")
            continue

        _logger.info(f"Applying migration {label}")
        instance = resolved.cls()
        await instance.up(store.db)
        await store.update_state(
            resolved.migration_id, resolved.step, resolved.name, "up"
        )
        _logger.info(f"Applied migration {label}")


async def rollback(
    store: StateStore[Any],
    migrations_path: Path,
    target: str | None,
    dry_run: bool = False,
) -> None:
    """Roll back to target migration ID (inclusive — steps in target remain applied).

    Pass target=None to roll back all migrations.
    Pre-validates that all steps to be rolled back have down() implementations.
    """
    steps = discover(migrations_path)
    state = await store.read_state()

    to_rollback = _rollback_steps(steps, state.migration_id, state.step, target)

    if not to_rollback:
        _logger.info("Nothing to roll back")
        return

    # Pre-validate all steps are reversible before executing any
    irreversible = [r for r in to_rollback if not r.cls().has_down()]
    if irreversible:
        names = ", ".join(
            f"{r.migration_id} step {r.step}: {r.name}" for r in irreversible
        )
        raise NotImplementedError(f"Cannot roll back — irreversible steps: {names}")

    for resolved in to_rollback:
        label = f"{resolved.migration_id} step {resolved.step}: {resolved.name}"
        if dry_run:
            _logger.info(f"[dry run] Would roll back migration {label}")
            continue

        _logger.info(f"Rolling back migration {label}")
        instance = resolved.cls()
        await instance.down(store.db)

        # After rolling back, state = the step before this one
        prev_id, prev_step = _step_before(steps, resolved)
        await store.update_state(prev_id, prev_step, resolved.name, "down")
        _logger.info(f"Rolled back migration {label}")


async def status(
    store: StateStore[Any],
    migrations_path: Path,
) -> MigrationStatus:
    """Return current migration state and list of pending steps."""
    steps = discover(migrations_path)
    state = await store.read_state()
    pending = _pending_steps(steps, state.migration_id, state.step)
    return MigrationStatus(
        current_migration_id=state.migration_id,
        current_step=state.step,
        pending=pending,
        history=state.history,
    )


async def stamp(
    store: StateStore[Any],
    migrations_path: Path,
    migration_id: str | None,
    step: int | None = None,
) -> None:
    """Forcibly set migration state without running any steps.

    If step is None, sets to the last step of the given migration.
    migration_id=None resets to "no migrations applied".
    """
    if migration_id is None:
        await store.stamp_state(None, 0)
        _logger.info("Stamped migration state to None (no migrations)")
        return

    steps = discover(migrations_path)
    migration_steps = [s for s in steps if s.migration_id == migration_id]
    if not migration_steps:
        raise ValueError(f"No migration found with id {migration_id!r}")

    if step is None:
        target = migration_steps[-1]
    else:
        matching = [s for s in migration_steps if s.step == step]
        if not matching:
            available = [s.step for s in migration_steps]
            raise ValueError(
                f"No step {step} in migration {migration_id!r}. Available: {available}"
            )
        target = matching[0]

    await store.stamp_state(target.migration_id, target.step)
    _logger.info(
        f"Stamped migration state to {target.migration_id} step {target.step}: "
        f"{target.name}"
    )


# --- Internal helpers ---


def _find_current_index(
    all_steps: list[ResolvedStep],
    migration_id: str | None,
    step: int,
) -> int:
    """Return the index of the current position in all_steps, or -1 if no migrations applied."""
    if migration_id is None:
        return -1
    for i, s in enumerate(all_steps):
        if s.migration_id == migration_id and s.step == step:
            return i
    return -1


def _find_target_index(
    all_steps: list[ResolvedStep],
    target: str | None,
) -> int:
    """Return the index of the last step of the target migration, or -1 for None."""
    if target is None:
        return -1
    for i in range(len(all_steps) - 1, -1, -1):
        if all_steps[i].migration_id == target:
            return i
    raise ValueError(f"No migration found with id {target!r}")


def _pending_steps(
    all_steps: list[ResolvedStep],
    current_migration_id: str | None,
    current_step: int,
    target: str | None = None,
) -> list[ResolvedStep]:
    """Filter steps that haven't been applied yet."""
    current_idx = _find_current_index(all_steps, current_migration_id, current_step)
    pending = all_steps[current_idx + 1 :]
    if target is not None:
        target_idx = _find_target_index(all_steps, target)
        # target_idx is absolute, convert to relative to pending start
        max_count = target_idx - current_idx
        pending = pending[:max_count]
    return pending


def _rollback_steps(
    all_steps: list[ResolvedStep],
    current_migration_id: str | None,
    current_step: int,
    target: str | None,
) -> list[ResolvedStep]:
    """Get steps to roll back (in reverse order) to reach target.

    Rolls back all applied steps that come after the target migration.
    """
    current_idx = _find_current_index(all_steps, current_migration_id, current_step)
    if target is None:
        target_idx = -1
    else:
        target_idx = _find_target_index(all_steps, target)

    to_rollback = all_steps[target_idx + 1 : current_idx + 1]
    return list(reversed(to_rollback))


def _step_before(
    all_steps: list[ResolvedStep], current: ResolvedStep
) -> tuple[str | None, int]:
    """Return (migration_id, step) of the step before current, or (None, 0) if first."""
    for i, s in enumerate(all_steps):
        if s.migration_id == current.migration_id and s.step == current.step:
            if i == 0:
                return (None, 0)
            prev = all_steps[i - 1]
            return (prev.migration_id, prev.step)
    return (None, 0)
