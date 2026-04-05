"""Migration state tracking model stored in the _migrations collection."""

from datetime import UTC, datetime
from typing import ClassVar, Literal

import typed_mongo
from pydantic import BaseModel, Field


class MigrationHistoryEntry(BaseModel):
    """Record of a single migration step execution."""

    version: int
    step: int
    name: str
    direction: Literal["up", "down"]
    applied_at: datetime


class MigrationState(typed_mongo.MongoCollectionModel):
    """Tracks the current migration position and execution history.

    A single document per database with _id="state".
    version=0, step=0 means no migrations have been applied.
    """

    __collection_name__: ClassVar[str] = "_migrations"

    id: Literal["state"] = Field(alias="_id", default="state")
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
