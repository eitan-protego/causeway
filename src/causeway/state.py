"""Migration state models and StateStore protocol."""

from datetime import UTC, datetime
from typing import Literal, Protocol, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T", covariant=True)


class MigrationHistoryEntry(BaseModel):
    """Record of a single migration step execution."""

    version: int
    step: int
    name: str
    direction: Literal["up", "down"]
    applied_at: datetime


class MigrationState(BaseModel):
    """Tracks the current migration position and execution history.

    version=0, step=0 means no migrations have been applied.
    """

    version: int = 0
    step: int = 0
    history: list[MigrationHistoryEntry] = Field(default_factory=list)

    @staticmethod
    def make_history_entry(
        version: int, step: int, name: str, direction: Literal["up", "down"]
    ) -> MigrationHistoryEntry:
        return MigrationHistoryEntry(
            version=version,
            step=step,
            name=name,
            direction=direction,
            applied_at=datetime.now(UTC),
        )


class StateStore(Protocol[T]):
    """Abstraction over migration state persistence.

    Implementations hold the database instance (passed at construction)
    and provide read/write access to migration state.
    """

    @property
    def db(self) -> T:
        """The underlying database instance."""
        ...

    async def read_state(self) -> MigrationState:
        """Read current migration state. Returns default (v0/s0) if none exists."""
        ...

    async def update_state(
        self, version: int, step: int, name: str, direction: Literal["up", "down"]
    ) -> None:
        """Record a migration step execution and update current position."""
        ...

    async def stamp_state(self, version: int, step: int) -> None:
        """Forcibly set the migration position without recording history."""
        ...
