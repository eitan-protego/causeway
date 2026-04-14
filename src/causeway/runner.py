"""Migration discovery and execution engine."""

import logging
import random
import string
import subprocess
import types
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from causeway.base import MigrationStep
from causeway.registration import MigrationMetadata, migration_loading_context
from causeway.state import MigrationHistoryEntry, StateStore

log = logging.getLogger(__name__)


@dataclass
class LoadedMigration:
    """A migration loaded from a file, before ordering."""

    metadata: MigrationMetadata
    steps: list[type[MigrationStep[Any]]]
    file_path: Path


@dataclass
class ResolvedStep:
    """A migration step with its assigned version and step number."""

    version: int
    step: int
    migration_id: str
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

    Each migration file must call ``register_migration(...)`` exactly once.
    The ``previous`` and ``next`` fields form a doubly-linked list that
    determines execution order.
    """
    migration_files = sorted(
        f for f in migrations_path.glob("*.py") if f.name != "__init__.py"
    )

    loaded = [_load_migration(f) for f in migration_files]
    ordered = _assemble_migration_order(loaded)

    steps: list[ResolvedStep] = []
    for version, migration in enumerate(ordered, start=1):
        for step_num, step_cls in enumerate(migration.steps, start=1):
            step_cls.version = version
            step_cls.step = step_num
            steps.append(
                ResolvedStep(
                    version=version,
                    step=step_num,
                    migration_id=migration.metadata.id,
                    cls=step_cls,
                )
            )

    return steps


def load_version(migrations_path: Path, version: int) -> list[type[MigrationStep[Any]]]:
    """Load and return step classes for a specific migration version.

    Version numbers are assigned based on position in the linked list
    (1-indexed).

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


def create(
    migrations_path: Path,
    *,
    id: str | None = None,
    name: str | None = None,
    description: str | None = None,
) -> Path:
    """Create a new migration file at the end of the migration chain.

    Generates a timestamped filename, writes a skeleton migration file with
    a docstring, register_migration() call, and a MigrationStep subclass.
    If existing migrations are found, the previous tail migration's ``next``
    field is updated via ast-grep.

    Args:
        migrations_path: Directory to create the migration file in.
        id: Migration ID (alphanumeric and dashes). Auto-generated if omitted.
        name: Human-readable name. Written to the docstring if provided.
        description: Optional description. Written to the docstring if provided.

    Returns:
        Path to the newly created migration file.
    """
    if id is None:
        id = _random_id()

    # Validate id format
    MigrationMetadata(id=id, previous=None, next=None)

    # Find the current tail migration (if any)
    tail = _find_tail_migration(migrations_path)

    # Generate filename
    now = datetime.now(UTC)
    timestamp = now.strftime("%Y-%m-%d_%H%M%S") + "Z"
    filename = f"{timestamp}-{id}.py"
    file_path = migrations_path / filename

    # Build file content
    previous_id = tail.metadata.id if tail else None
    content = _build_migration_file(id, name, description, previous_id)
    file_path.write_text(content)

    # Update the previous tail's next= field
    if tail is not None:
        _update_tail_next(tail.file_path, id)

    return file_path


# --- Internal helpers ---


def _random_id() -> str:
    """Generate a random 10-character lowercase alphanumeric ID."""
    chars = string.ascii_lowercase + string.digits
    return "".join(random.choice(chars) for _ in range(10))


def _find_tail_migration(migrations_path: Path) -> LoadedMigration | None:
    """Find the tail (last) migration in the chain, or None if empty."""
    migration_files = sorted(
        f for f in migrations_path.glob("*.py") if f.name != "__init__.py"
    )
    if not migration_files:
        return None

    loaded = [_load_migration(f) for f in migration_files]
    ordered = _assemble_migration_order(loaded)
    return ordered[-1]


def _build_migration_file(
    migration_id: str,
    name: str | None,
    description: str | None,
    previous_id: str | None,
) -> str:
    """Build the content of a new migration file."""
    parts: list[str] = []

    # Docstring
    if name or description:
        docstring_lines: list[str] = []
        if name:
            docstring_lines.append(name)
        if description:
            if docstring_lines:
                docstring_lines.append("")
            docstring_lines.append(description)
        docstring = "\n".join(docstring_lines)
        parts.append(f'"""{docstring}\n"""')
    else:
        parts.append(f'"""TODO: Add migration description."""')

    parts.append(
        "from causeway import MigrationStep, register_migration\n"
        "from typing import Any"
    )

    # register_migration call
    prev_str = f'"{previous_id}"' if previous_id else "None"
    parts.append(
        f'register_migration(id="{migration_id}", previous={prev_str}, next=None)'
    )

    # Step class
    parts.append(
        "class Step(MigrationStep[Any]):\n"
        "    async def up(self, db: Any) -> None:\n"
        "        raise NotImplementedError\n"
    )

    return "\n\n".join(parts)


def _update_tail_next(file_path: Path, new_id: str) -> None:
    """Update the previous tail migration's next=None to next=new_id via ast-grep."""
    rule = (
        f"id: update-next\n"
        f"language: python\n"
        f"rule:\n"
        f"  pattern: register_migration($$$BEFORE, next=None)\n"
        f'fix: register_migration($$$BEFORE, next="{new_id}")'
    )
    result = subprocess.run(
        ["ast-grep", "scan", "--update-all", "--inline-rules", rule, str(file_path)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"ast-grep failed to update {file_path.name}: {result.stderr}"
        )


def _load_migration(file_path: Path) -> LoadedMigration:
    """Load a single migration file, capturing its registration and step classes."""
    with migration_loading_context() as registered:
        module = _load_migration_module(file_path)

    if len(registered) == 0:
        raise ValueError(
            f"Migration file {file_path.name} did not call register_migration(). "
            f"Each migration file must call causeway.register_migration(...) exactly once."
        )
    if len(registered) > 1:
        raise ValueError(
            f"Migration file {file_path.name} called register_migration() "
            f"{len(registered)} times. Each migration file must call it exactly once."
        )

    metadata = registered[0]
    metadata = _auto_fill_metadata(metadata, module, file_path)
    steps = _collect_steps(module)

    return LoadedMigration(metadata=metadata, steps=steps, file_path=file_path)


def _auto_fill_metadata(
    metadata: MigrationMetadata, module: types.ModuleType, file_path: Path
) -> MigrationMetadata:
    """Auto-fill name and description from module docstring if not provided."""
    updates: dict[str, str] = {}

    if metadata.name is None:
        docstring = module.__doc__
        if not docstring:
            raise ValueError(
                f"Migration {metadata.id!r} ({file_path.name}) has no name and no "
                f"module docstring. Either provide name= to register_migration() or "
                f"add a module docstring."
            )

        # Strip leading newlines but preserve structure: a docstring whose
        # first physical line (after the opening quotes) is blank counts as
        # having an empty first line.
        lines = docstring.split("\n")
        # Find the first non-empty line
        first_line = ""
        first_line_idx = 0
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped:
                first_line = stripped
                first_line_idx = i
                break

        if not first_line:
            raise ValueError(
                f"Migration {metadata.id!r} ({file_path.name}) has no name and the "
                f"module docstring has an empty first line. Either provide name= or "
                f"ensure the docstring starts with a non-empty line."
            )

        # Only use the first non-empty line if it's the actual first line
        # (possibly after one leading newline from triple-quote style)
        if first_line_idx > 1:
            raise ValueError(
                f"Migration {metadata.id!r} ({file_path.name}) has no name and the "
                f"module docstring has an empty first line. Either provide name= or "
                f"ensure the docstring starts with a non-empty line."
            )

        updates["name"] = first_line

        if metadata.description is None:
            rest = "\n".join(lines[first_line_idx + 1 :]).strip()
            if rest:
                updates["description"] = rest

    if updates:
        return metadata.model_copy(update=updates)
    return metadata


def _assemble_migration_order(
    loaded: list[LoadedMigration],
) -> list[LoadedMigration]:
    """Assemble loaded migrations into order using previous/next links.

    Validates that they form a single valid doubly-linked list.
    """
    if not loaded:
        return []

    by_id: dict[str, LoadedMigration] = {}
    for m in loaded:
        if m.metadata.id in by_id:
            existing = by_id[m.metadata.id]
            raise ValueError(
                f"Duplicate migration id {m.metadata.id!r}: "
                f"found in {existing.file_path.name} and {m.file_path.name}"
            )
        by_id[m.metadata.id] = m

    # Validate all referenced IDs exist
    for m in loaded:
        if m.metadata.previous is not None and m.metadata.previous not in by_id:
            raise ValueError(
                f"Migration {m.metadata.id!r} ({m.file_path.name}) references "
                f"previous={m.metadata.previous!r} which does not exist"
            )
        if m.metadata.next is not None and m.metadata.next not in by_id:
            raise ValueError(
                f"Migration {m.metadata.id!r} ({m.file_path.name}) references "
                f"next={m.metadata.next!r} which does not exist"
            )

    # Validate prev/next consistency
    for m in loaded:
        if m.metadata.next is not None:
            next_m = by_id[m.metadata.next]
            if next_m.metadata.previous != m.metadata.id:
                raise ValueError(
                    f"Inconsistent links: migration {m.metadata.id!r} "
                    f"({m.file_path.name}) has next={m.metadata.next!r}, but "
                    f"migration {next_m.metadata.id!r} ({next_m.file_path.name}) "
                    f"has previous={next_m.metadata.previous!r}"
                )
        if m.metadata.previous is not None:
            prev_m = by_id[m.metadata.previous]
            if prev_m.metadata.next != m.metadata.id:
                raise ValueError(
                    f"Inconsistent links: migration {m.metadata.id!r} "
                    f"({m.file_path.name}) has previous={m.metadata.previous!r}, but "
                    f"migration {prev_m.metadata.id!r} ({prev_m.file_path.name}) "
                    f"has next={prev_m.metadata.next!r}"
                )

    # Find head(s) — migrations with previous=None
    heads = [m for m in loaded if m.metadata.previous is None]
    if len(heads) == 0:
        ids = [f"{m.metadata.id!r} ({m.file_path.name})" for m in loaded]
        raise ValueError(
            f"No initial migration found (all migrations have a previous link). "
            f"This indicates a cycle among: {', '.join(ids)}"
        )
    if len(heads) > 1:
        head_names = [f"{m.metadata.id!r} ({m.file_path.name})" for m in heads]
        raise ValueError(
            f"Multiple initial migrations (previous=None): "
            f"{', '.join(head_names)}. "
            f"There must be exactly one migration with previous=None."
        )

    # Walk the list from head to tail
    ordered: list[LoadedMigration] = []
    current: LoadedMigration | None = heads[0]
    seen: set[str] = set()

    while current is not None:
        if current.metadata.id in seen:
            raise ValueError(
                f"Cycle detected: migration {current.metadata.id!r} "
                f"({current.file_path.name}) was already visited"
            )
        seen.add(current.metadata.id)
        ordered.append(current)

        if current.metadata.next is not None:
            current = by_id[current.metadata.next]
        else:
            current = None

    # Check all migrations are reachable
    if len(ordered) != len(loaded):
        unreachable = [m for m in loaded if m.metadata.id not in seen]
        unreachable_names = [
            f"{m.metadata.id!r} ({m.file_path.name})" for m in unreachable
        ]
        raise ValueError(
            f"Not all migrations are reachable from the initial migration. "
            f"Unreachable: {', '.join(unreachable_names)}. "
            f"These migrations form a separate chain."
        )

    return ordered


_module_load_counter = 0


def _load_migration_module(file_path: Path) -> types.ModuleType:
    """Dynamically import a migration file and return the module.

    Uses compile+exec instead of importlib loaders to bypass the bytecode
    cache (.pyc), which can serve stale content when a file is modified
    within the same second (e.g. by ``create()`` updating the tail migration).
    """
    global _module_load_counter
    _module_load_counter += 1
    module_name = f"_causeway_migration_{file_path.stem}_{_module_load_counter}"
    source = file_path.read_text(encoding="utf-8")
    code = compile(source, str(file_path), "exec")
    module = types.ModuleType(module_name)
    module.__file__ = str(file_path)
    exec(code, module.__dict__)  # noqa: S102
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
