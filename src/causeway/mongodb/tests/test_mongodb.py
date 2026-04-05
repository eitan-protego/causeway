"""Integration tests for the causeway MongoDB backend."""

import textwrap
from pathlib import Path
from typing import Any

import pytest
from pymongo.asynchronous.database import AsyncDatabase

from causeway import migrate, rollback, stamp, status
from causeway.mongodb import MongoStateStore
from causeway.state import MigrationState

_STEP_IMPORTS = textwrap.dedent("""\
    from causeway import MigrationStep
    from typing import Any
    from pymongo.asynchronous.database import AsyncDatabase
""")

_NOOP_STEP = _STEP_IMPORTS + textwrap.dedent("""\

    class Step(MigrationStep):
        async def up(self, db: AsyncDatabase[dict[str, Any]]) -> None:
            pass
""")


@pytest.fixture
def migrations_dir(tmp_path: Path) -> Path:
    return tmp_path


def _write_migration(migrations_dir: Path, filename: str, content: str) -> Path:
    file_path = migrations_dir / filename
    file_path.write_text(content)
    return file_path


async def _get_state(db: AsyncDatabase[dict[str, Any]]) -> MigrationState:
    collection = db.get_collection("_migrations")
    doc = await collection.find_one({"_id": "state"})
    assert doc is not None, "Migration state document not found"
    return MigrationState.model_validate(doc)


def _step(body: str, cls_name: str = "Step") -> str:
    return (
        _STEP_IMPORTS
        + f"""
class {cls_name}(MigrationStep):
    async def up(self, db: AsyncDatabase[dict[str, Any]]) -> None:
        {body}
"""
    )


def _reversible_step(up_body: str, down_body: str, cls_name: str = "Step") -> str:
    return (
        _STEP_IMPORTS
        + f"""
class {cls_name}(MigrationStep):
    async def up(self, db: AsyncDatabase[dict[str, Any]]) -> None:
        {up_body}

    async def down(self, db: AsyncDatabase[dict[str, Any]]) -> None:
        {down_body}
"""
    )


class TestMongoMigrate:
    async def test_creates_state_on_first_run(
        self,
        db: AsyncDatabase[dict[str, Any]],
        store: MongoStateStore,
        migrations_dir: Path,
    ) -> None:
        _write_migration(
            migrations_dir,
            "001_init.py",
            _step('await db.get_collection("data").insert_one({"_id": "v1"})'),
        )

        await migrate(store, migrations_dir)

        state = await _get_state(db)
        assert state.version == 1
        assert state.step == 1
        assert len(state.history) == 1
        assert state.history[0].direction == "up"
        assert await db.get_collection("data").count_documents({}) == 1

    async def test_applies_all_pending(
        self,
        db: AsyncDatabase[dict[str, Any]],
        store: MongoStateStore,
        migrations_dir: Path,
    ) -> None:
        for v in [1, 2]:
            _write_migration(
                migrations_dir,
                f"00{v}_migration.py",
                _step(
                    f'await db.get_collection("data").insert_one({{"_id": "v{v}"}})',
                    cls_name=f"Step{v}",
                ),
            )

        await migrate(store, migrations_dir)

        assert await db.get_collection("data").count_documents({}) == 2
        state = await _get_state(db)
        assert state.version == 2

    async def test_stops_at_target_version(
        self,
        db: AsyncDatabase[dict[str, Any]],
        store: MongoStateStore,
        migrations_dir: Path,
    ) -> None:
        for v in range(1, 4):
            _write_migration(
                migrations_dir,
                f"00{v}_migration.py",
                _step(
                    f'await db.get_collection("data").insert_one({{"_id": "v{v}"}})',
                    cls_name=f"Step{v}",
                ),
            )

        await migrate(store, migrations_dir, target_version=2)

        state = await _get_state(db)
        assert state.version == 2
        assert await db.get_collection("data").count_documents({}) == 2

    async def test_dry_run(
        self,
        db: AsyncDatabase[dict[str, Any]],
        store: MongoStateStore,
        migrations_dir: Path,
    ) -> None:
        _write_migration(
            migrations_dir,
            "001_init.py",
            _step('await db.get_collection("data").insert_one({"_id": "x"})'),
        )

        await migrate(store, migrations_dir, dry_run=True)

        doc = await db.get_collection("_migrations").find_one({"_id": "state"})
        assert doc is None
        assert await db.get_collection("data").count_documents({}) == 0


class TestMongoRollback:
    async def test_rolls_back(
        self,
        db: AsyncDatabase[dict[str, Any]],
        store: MongoStateStore,
        migrations_dir: Path,
    ) -> None:
        for v in [1, 2]:
            _write_migration(
                migrations_dir,
                f"00{v}_migration.py",
                _reversible_step(
                    f'await db.get_collection("data").insert_one({{"_id": "v{v}"}})',
                    f'await db.get_collection("data").delete_one({{"_id": "v{v}"}})',
                    cls_name=f"Step{v}",
                ),
            )

        await migrate(store, migrations_dir)
        assert await db.get_collection("data").count_documents({}) == 2

        await rollback(store, migrations_dir, target_version=1)

        assert await db.get_collection("data").count_documents({}) == 1
        state = await _get_state(db)
        assert state.version == 1


class TestMongoStamp:
    async def test_stamp_sets_state(
        self,
        db: AsyncDatabase[dict[str, Any]],
        store: MongoStateStore,
        migrations_dir: Path,
    ) -> None:
        _write_migration(migrations_dir, "001_init.py", _NOOP_STEP)

        await stamp(store, migrations_dir, version=1)

        state = await _get_state(db)
        assert state.version == 1

    async def test_stamp_zero_resets(
        self,
        db: AsyncDatabase[dict[str, Any]],
        store: MongoStateStore,
        migrations_dir: Path,
    ) -> None:
        _write_migration(migrations_dir, "001_init.py", _NOOP_STEP)
        await migrate(store, migrations_dir)

        await stamp(store, migrations_dir, version=0)

        state = await _get_state(db)
        assert state.version == 0


class TestMongoStatus:
    async def test_returns_pending(
        self, store: MongoStateStore, migrations_dir: Path
    ) -> None:
        _write_migration(migrations_dir, "001_init.py", _NOOP_STEP)

        result = await status(store, migrations_dir)

        assert result.current_version == 0
        assert len(result.pending) == 1


class TestDocumentMigrationStep:
    async def test_transforms_matching_documents(
        self,
        db: AsyncDatabase[dict[str, Any]],
        store: MongoStateStore,
        migrations_dir: Path,
    ) -> None:
        collection = db.get_collection("items")
        await collection.insert_many(
            [
                {"_id": "1", "name": "alice"},
                {"_id": "2", "name": "bob"},
                {"_id": "3", "name": "charlie", "status": "active"},
            ]
        )

        content = textwrap.dedent("""\
            from causeway.mongodb.helpers import DocumentMigrationStep
            from typing import Any, ClassVar

            class BackfillStatus(DocumentMigrationStep):
                collection_name: ClassVar[str] = "items"
                query: ClassVar[dict[str, Any]] = {"status": {"$exists": False}}

                def transform(self, doc: dict[str, Any]) -> dict[str, Any]:
                    doc["status"] = "pending"
                    return doc
        """)
        _write_migration(migrations_dir, "001_backfill.py", content)

        await migrate(store, migrations_dir)

        alice = await collection.find_one({"_id": "1"})
        assert alice is not None
        assert alice["status"] == "pending"

        charlie = await collection.find_one({"_id": "3"})
        assert charlie is not None
        assert charlie["status"] == "active"


class TestIndexMigrationStep:
    _INDEX_MIGRATION: str = textwrap.dedent("""\
        from causeway.mongodb.helpers import IndexMigrationStep
        from typing import ClassVar

        class StatusIndex(IndexMigrationStep):
            collection_name: ClassVar[str] = "items"
            index: ClassVar[list[tuple[str, int]]] = [("status", 1)]
    """)

    async def test_creates_index(
        self,
        db: AsyncDatabase[dict[str, Any]],
        store: MongoStateStore,
        migrations_dir: Path,
    ) -> None:
        _write_migration(migrations_dir, "001_indexes.py", self._INDEX_MIGRATION)

        await migrate(store, migrations_dir)

        indexes = await db.get_collection("items").index_information()
        index_keys = [v["key"] for v in indexes.values()]
        assert [("status", 1)] in index_keys

    async def test_drops_index_on_rollback(
        self,
        db: AsyncDatabase[dict[str, Any]],
        store: MongoStateStore,
        migrations_dir: Path,
    ) -> None:
        _write_migration(migrations_dir, "001_indexes.py", self._INDEX_MIGRATION)

        await migrate(store, migrations_dir)
        await rollback(store, migrations_dir, target_version=0)

        indexes = await db.get_collection("items").index_information()
        index_keys = [v["key"] for v in indexes.values()]
        assert [("status", 1)] not in index_keys
