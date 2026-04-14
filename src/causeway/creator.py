"""Scaffolding for new migration files."""

import random
import string
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from causeway.loader import LoadedMigration, assemble_migration_order, load_all
from causeway.registration import MigrationMetadata


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


def _random_id() -> str:
    """Generate a random 10-character lowercase alphanumeric ID."""
    chars = string.ascii_lowercase + string.digits
    return "".join(random.choice(chars) for _ in range(10))


def _find_tail_migration(migrations_path: Path) -> LoadedMigration | None:
    """Find the tail (last) migration in the chain, or None if empty."""
    loaded = load_all(migrations_path)
    if not loaded:
        return None

    ordered = assemble_migration_order(loaded)
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
        parts.append('"""TODO: Add migration description."""')

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
