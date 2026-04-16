"""Migration state models and StateStore protocol."""

from datetime import UTC, datetime
from typing import Literal, Protocol

from pydantic import BaseModel, Field


class MigrationHistoryEntry(BaseModel):
    """Record of a single migration step execution."""

    migration_id: str | None
    step: int
    name: str
    direction: Literal["up", "down"]
    applied_at: datetime


class MigrationState(BaseModel):
    """Tracks the current migration position and execution history.

    migration_id=None, step=0 means no migrations have been applied.
    """

    migration_id: str | None = None
    step: int = 0
    history: list[MigrationHistoryEntry] = Field(default_factory=list)

    @staticmethod
    def make_history_entry(
        migration_id: str | None, step: int, name: str, direction: Literal["up", "down"]
    ) -> MigrationHistoryEntry:
        return MigrationHistoryEntry(
            migration_id=migration_id,
            step=step,
            name=name,
            direction=direction,
            applied_at=datetime.now(UTC),
        )


class StateStore[T](Protocol):
    """Abstraction over migration state persistence.

    Implementations hold the database instance (passed at construction)
    and provide read/write access to migration state.
    """

    @property
    def db(self) -> T:
        """The underlying database instance."""
        ...

    async def read_state(self) -> MigrationState:
        """Read current migration state. Returns default if none exists."""
        ...

    async def update_state(
        self,
        migration_id: str | None,
        step: int,
        name: str,
        direction: Literal["up", "down"],
    ) -> None:
        """Record a migration step execution and update current position."""
        ...

    async def stamp_state(self, migration_id: str | None, step: int) -> None:
        """Forcibly set the migration position without recording history."""
        ...
