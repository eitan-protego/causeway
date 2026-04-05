"""MongoDB-specific migration step helpers."""

from abc import ABC, abstractmethod
from typing import Any, ClassVar, override

from pymongo.asynchronous.database import AsyncDatabase

from causeway.base import MigrationStep

type MongoDb = AsyncDatabase[dict[str, Any]]


class DocumentMigrationStep(MigrationStep[MongoDb], ABC):
    """Iterate matching documents and apply a sync transform to each.

    Define collection_name, optionally query,
    and implement transform() to modify each document.

    Example:
        class BackfillStatus(DocumentMigrationStep):
            collection_name: ClassVar[str] = "cases"
            query: ClassVar[dict[str, Any]] = {"status": {"$exists": False}}

            def transform(self, doc: dict[str, Any]) -> dict[str, Any]:
                doc["status"] = "pending"
                return doc
    """

    collection_name: ClassVar[str]
    query: ClassVar[dict[str, Any]] = {}

    @abstractmethod
    def transform(self, doc: dict[str, Any]) -> dict[str, Any]:
        """Transform a single document. Return the modified document."""

    @override
    async def up(self, db: MongoDb) -> None:
        collection = db.get_collection(self.collection_name)
        async for doc in collection.find(self.query):
            transformed = self.transform(doc)
            await collection.replace_one({"_id": doc["_id"]}, transformed)


class IndexMigrationStep(MigrationStep[MongoDb]):
    """Create an index on up(), drop it on down(). Both auto-implemented.

    Example:
        class CaseStatusIndex(IndexMigrationStep):
            collection_name: ClassVar[str] = "cases"
            index: ClassVar[list[tuple[str, int]]] = [("status", 1), ("created_at", -1)]
            unique: ClassVar[bool] = True
    """

    collection_name: ClassVar[str]
    index: ClassVar[list[tuple[str, int]]]
    unique: ClassVar[bool] = False
    sparse: ClassVar[bool] = False
    index_name: ClassVar[str | None] = None

    @override
    async def up(self, db: MongoDb) -> None:
        collection = db.get_collection(self.collection_name)
        kwargs: dict[str, Any] = {}
        if self.unique:
            kwargs["unique"] = True
        if self.sparse:
            kwargs["sparse"] = True
        if self.index_name:
            kwargs["name"] = self.index_name
        await collection.create_index(self.index, **kwargs)

    @override
    async def down(self, db: MongoDb) -> None:
        collection = db.get_collection(self.collection_name)
        await collection.drop_index(self.index)
