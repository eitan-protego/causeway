"""Migration discovery and execution engine."""

import importlib.util
import logging
import types
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from causeway.base import MigrationStep
from causeway.state import MigrationHistoryEntry, StateStore

log = logging.getLogger(__name__)


@dataclass
class ResolvedStep:
    """A migration step with its assigned version and step number."""

    version: int
    step: int
    cls: type[MigrationStep[Any]]

    @property
    def name(self) -> str:
        return self.cls.name


@dataclass
class MigrationStatus:
    """Current migration state and pending steps."""

    current_version: int
    current_step: int
    pending: list[ResolvedStep]
    history: list[MigrationHistoryEntry] = field(default_factory=list)


def discover(migrations_path: Path) -> list[ResolvedStep]:
    """Discover and load migration files, returning resolved steps in order.

    Files must match the pattern NNN_description.py where NNN is the version number.
    Steps within each file are ordered by class definition order.
    """
    migration_files = sorted(
        migrations_path.glob("[0-9]*_*.py"),
        key=lambda f: _extract_version(f),
    )

    _validate_versions(migration_files)

    steps: list[ResolvedStep] = []

    for file_path in migration_files:
        version = _extract_version(file_path)
        module = _load_migration_module(file_path)
        file_steps = _collect_steps(module)

        for step_num, step_cls in enumerate(file_steps, start=1):
            step_cls.version = version
            step_cls.step = step_num
            steps.append(ResolvedStep(version=version, step=step_num, cls=step_cls))

    return steps


def load_version(migrations_path: Path, version: int) -> list[type[MigrationStep[Any]]]:
    """Load and return step classes for a specific migration version.

    Convenient for testing individual migrations::

        steps = load_version(MIGRATIONS_DIR, 1)
        await steps[0]().up(db)
    """
    all_steps = discover(migrations_path)
    return [s.cls for s in all_steps if s.version == version]


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


def _extract_version(file_path: Path) -> int:
    """Extract version number from filename prefix: 001_description.py -> 1."""
    stem = file_path.stem
    prefix = stem.split("_", maxsplit=1)[0]
    return int(prefix)


def _validate_versions(files: list[Path]) -> None:
    """Validate version numbers are unique and sequential starting from 1."""
    versions = [_extract_version(f) for f in files]
    if not versions:
        return

    seen: set[int] = set()
    for v in versions:
        if v in seen:
            raise ValueError(f"Duplicate migration version: {v}")
        seen.add(v)

    expected = list(range(1, max(versions) + 1))
    if versions != expected:
        missing = set(expected) - set(versions)
        raise ValueError(
            f"Non-sequential migration versions. Missing: {sorted(missing)}"
        )


def _load_migration_module(file_path: Path) -> types.ModuleType:
    """Dynamically import a migration file and return the module."""
    module_name = f"_causeway_migration_{file_path.stem}"
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load migration file: {file_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _collect_steps(module: types.ModuleType) -> list[type[MigrationStep[Any]]]:
    """Collect MigrationStep subclasses defined in a module, in definition order."""
    return [
        obj
        for obj in vars(module).values()
        if isinstance(obj, type)
        and issubclass(obj, MigrationStep)
        and obj.__module__ == module.__name__
    ]


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
