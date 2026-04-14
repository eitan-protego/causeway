"""Migration discovery and loading from the filesystem."""

import types
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from causeway.base import MigrationStep
from causeway.registration import MigrationMetadata, migration_loading_context


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


def discover(migrations_path: Path) -> list[ResolvedStep]:
    """Discover and load migration files, returning resolved steps in order.

    Each migration file must call ``register_migration(...)`` exactly once.
    The ``previous`` and ``next`` fields form a doubly-linked list that
    determines execution order.
    """
    loaded = load_all(migrations_path)
    ordered = assemble_migration_order(loaded)

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


def load_all(migrations_path: Path) -> list[LoadedMigration]:
    """Load all migration files from a directory without ordering them."""
    migration_files = sorted(
        f for f in migrations_path.glob("*.py") if f.name != "__init__.py"
    )
    return [load_migration(f) for f in migration_files]


def load_migration(file_path: Path) -> LoadedMigration:
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


def assemble_migration_order(
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


# --- Internal helpers ---

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
