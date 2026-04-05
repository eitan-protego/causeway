"""MongoDB implementation of StateStore."""

from typing import Any, Literal

from pymongo.asynchronous.database import AsyncDatabase

from causeway.base import MigrationStep
from causeway.state import MigrationState

_COLLECTION_NAME = "_migrations"

type MongoDb = AsyncDatabase[dict[str, Any]]

MongoMigrationStep = MigrationStep[MongoDb]


class MongoStateStore:
    """MongoDB implementation of the causeway StateStore protocol."""

    _db: MongoDb

    def __init__(self, db: MongoDb) -> None:
        self._db = db

    @property
    def db(self) -> MongoDb:
        return self._db

    async def read_state(self) -> MigrationState:
        collection = self._db.get_collection(_COLLECTION_NAME)
        doc = await collection.find_one({"_id": "state"})
        if doc is None:
            return MigrationState()
        return MigrationState.model_validate(doc)

    async def update_state(
        self, version: int, step: int, name: str, direction: Literal["up", "down"]
    ) -> None:
        entry = MigrationState.make_history_entry(
            version=version, step=step, name=name, direction=direction
        )
        collection = self._db.get_collection(_COLLECTION_NAME)
        await collection.update_one(
            {"_id": "state"},
            {
                "$set": {"version": version, "step": step},
                "$push": {"history": entry.model_dump()},
            },
            upsert=True,
        )

    async def stamp_state(self, version: int, step: int) -> None:
        collection = self._db.get_collection(_COLLECTION_NAME)
        await collection.update_one(
            {"_id": "state"},
            {"$set": {"version": version, "step": step}},
            upsert=True,
        )
