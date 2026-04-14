"""Migration registration via MigrationMetadata and ContextVar-based loading."""

import re
from collections.abc import Callable, Generator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

from pydantic import BaseModel, field_validator


class MigrationMetadata(BaseModel):
    """Metadata for a migration, registered by calling register_migration()."""

    id: str
    name: str | None = None
    description: str | None = None
    previous: str | None
    next: str | None

    @field_validator("id")
    @classmethod
    def validate_id(cls, v: str) -> str:
        if not re.fullmatch(r"[a-zA-Z0-9-]+", v):
            raise ValueError(
                f"Migration id must consist of alphanumeric and dash characters, got: {v!r}"
            )
        return v


_registration_callback: ContextVar[Callable[[MigrationMetadata], None] | None] = (
    ContextVar("_registration_callback", default=None)
)


def register_migration(**kwargs: Any) -> None:
    """Register a migration. Must be called exactly once per migration file.

    Args:
        id: Unique identifier (alphanumeric and dashes only).
        name: Human-readable name (auto-filled from module docstring if omitted).
        description: Optional description (auto-filled from docstring body if omitted).
        previous: ID of the preceding migration, or None for the first migration.
        next: ID of the following migration, or None for the last migration.
    """
    callback = _registration_callback.get()
    if callback is None:
        raise RuntimeError(
            "register_migration() called outside of migration loading context"
        )
    metadata = MigrationMetadata(**kwargs)
    callback(metadata)


@contextmanager
def migration_loading_context() -> Generator[list[MigrationMetadata], None, None]:
    """Context manager that captures register_migration() calls."""
    registered: list[MigrationMetadata] = []

    def callback(metadata: MigrationMetadata) -> None:
        registered.append(metadata)

    token = _registration_callback.set(callback)
    try:
        yield registered
    finally:
        _registration_callback.reset(token)
