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
    from causeway import MigrationStep, register_migration
    from typing import Any
    from pymongo.asynchronous.database import AsyncDatabase
""")


def _quote_or_none(val: str | None) -> str:
    return f'"{val}"' if val is not None else "None"


def _noop_migration(
    migration_id: str,
    previous: str | None = None,
    next: str | None = None,
    name: str | None = None,
) -> str:
    parts = [f'id="{migration_id}"']
    if name is not None:
        parts.append(f'name="{name}"')
    else:
        parts.append(f'name="{migration_id}"')
    parts.append(f"previous={_quote_or_none(previous)}")
    parts.append(f"next={_quote_or_none(next)}")
    reg_call = f"register_migration({', '.join(parts)})"
    return (
        _STEP_IMPORTS
        + f"\n{reg_call}\n"
        + textwrap.dedent("""
    class Step(MigrationStep):
        async def up(self, db: AsyncDatabase[dict[str, Any]]) -> None:
            pass
    """)
    )


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


def _step(
    body: str,
    cls_name: str = "Step",
    migration_id: str = "m1",
    previous: str | None = None,
    next: str | None = None,
    name: str = "test",
) -> str:
    return (
        _STEP_IMPORTS
        + f'\nregister_migration(id="{migration_id}", name="{name}", '
        + f"previous={_quote_or_none(previous)}, next={_quote_or_none(next)})\n"
        + f"""
class {cls_name}(MigrationStep):
    async def up(self, db: AsyncDatabase[dict[str, Any]]) -> None:
        {body}
"""
    )


def _reversible_step(
    up_body: str,
    down_body: str,
    cls_name: str = "Step",
    migration_id: str = "m1",
    previous: str | None = None,
    next: str | None = None,
    name: str = "test",
) -> str:
    return (
        _STEP_IMPORTS
        + f'\nregister_migration(id="{migration_id}", name="{name}", '
        + f"previous={_quote_or_none(previous)}, next={_quote_or_none(next)})\n"
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
            "init.py",
            _step(
                'await db.get_collection("data").insert_one({"_id": "v1"})',
                migration_id="init",
                previous=None,
                next=None,
                name="init",
            ),
        )

        await migrate(store, migrations_dir)

        state = await _get_state(db)
        assert state.migration_id == "init"
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
        _write_migration(
            migrations_dir,
            "first.py",
            _step(
                'await db.get_collection("data").insert_one({"_id": "v1"})',
                cls_name="Step1",
                migration_id="m1",
                previous=None,
                next="m2",
                name="first",
            ),
        )
        _write_migration(
            migrations_dir,
            "second.py",
            _step(
                'await db.get_collection("data").insert_one({"_id": "v2"})',
                cls_name="Step2",
                migration_id="m2",
                previous="m1",
                next=None,
                name="second",
            ),
        )

        await migrate(store, migrations_dir)

        assert await db.get_collection("data").count_documents({}) == 2
        state = await _get_state(db)
        assert state.migration_id == "m2"

    async def test_stops_at_target(
        self,
        db: AsyncDatabase[dict[str, Any]],
        store: MongoStateStore,
        migrations_dir: Path,
    ) -> None:
        _write_migration(
            migrations_dir,
            "first.py",
            _step(
                'await db.get_collection("data").insert_one({"_id": "v1"})',
                cls_name="Step1",
                migration_id="m1",
                previous=None,
                next="m2",
                name="first",
            ),
        )
        _write_migration(
            migrations_dir,
            "second.py",
            _step(
                'await db.get_collection("data").insert_one({"_id": "v2"})',
                cls_name="Step2",
                migration_id="m2",
                previous="m1",
                next="m3",
                name="second",
            ),
        )
        _write_migration(
            migrations_dir,
            "third.py",
            _step(
                'await db.get_collection("data").insert_one({"_id": "v3"})',
                cls_name="Step3",
                migration_id="m3",
                previous="m2",
                next=None,
                name="third",
            ),
        )

        await migrate(store, migrations_dir, target="m2")

        state = await _get_state(db)
        assert state.migration_id == "m2"
        assert await db.get_collection("data").count_documents({}) == 2

    async def test_dry_run(
        self,
        db: AsyncDatabase[dict[str, Any]],
        store: MongoStateStore,
        migrations_dir: Path,
    ) -> None:
        _write_migration(
            migrations_dir,
            "init.py",
            _step(
                'await db.get_collection("data").insert_one({"_id": "x"})',
                migration_id="init",
                previous=None,
                next=None,
                name="init",
            ),
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
        _write_migration(
            migrations_dir,
            "first.py",
            _reversible_step(
                'await db.get_collection("data").insert_one({"_id": "v1"})',
                'await db.get_collection("data").delete_one({"_id": "v1"})',
                cls_name="Step1",
                migration_id="m1",
                previous=None,
                next="m2",
                name="first",
            ),
        )
        _write_migration(
            migrations_dir,
            "second.py",
            _reversible_step(
                'await db.get_collection("data").insert_one({"_id": "v2"})',
                'await db.get_collection("data").delete_one({"_id": "v2"})',
                cls_name="Step2",
                migration_id="m2",
                previous="m1",
                next=None,
                name="second",
            ),
        )

        await migrate(store, migrations_dir)
        assert await db.get_collection("data").count_documents({}) == 2

        await rollback(store, migrations_dir, target="m1")

        assert await db.get_collection("data").count_documents({}) == 1
        state = await _get_state(db)
        assert state.migration_id == "m1"


class TestMongoStamp:
    async def test_stamp_sets_state(
        self,
        db: AsyncDatabase[dict[str, Any]],
        store: MongoStateStore,
        migrations_dir: Path,
    ) -> None:
        _write_migration(
            migrations_dir,
            "init.py",
            _noop_migration("init", previous=None, next=None),
        )

        await stamp(store, migrations_dir, migration_id="init")

        state = await _get_state(db)
        assert state.migration_id == "init"

    async def test_stamp_none_resets(
        self,
        db: AsyncDatabase[dict[str, Any]],
        store: MongoStateStore,
        migrations_dir: Path,
    ) -> None:
        _write_migration(
            migrations_dir,
            "init.py",
            _noop_migration("init", previous=None, next=None),
        )
        await migrate(store, migrations_dir)

        await stamp(store, migrations_dir, migration_id=None)

        state = await _get_state(db)
        assert state.migration_id is None


class TestMongoStatus:
    async def test_returns_pending(
        self, store: MongoStateStore, migrations_dir: Path
    ) -> None:
        _write_migration(
            migrations_dir,
            "init.py",
            _noop_migration("init", previous=None, next=None),
        )

        result = await status(store, migrations_dir)

        assert result.current_migration_id is None
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
            from causeway import register_migration
            from causeway.mongodb.helpers import DocumentMigrationStep
            from typing import Any, ClassVar

            register_migration(id="backfill", name="backfill status", previous=None, next=None)

            class BackfillStatus(DocumentMigrationStep):
                collection_name: ClassVar[str] = "items"
                query: ClassVar[dict[str, Any]] = {"status": {"$exists": False}}

                def transform(self, doc: dict[str, Any]) -> dict[str, Any]:
                    doc["status"] = "pending"
                    return doc
        """)
        _write_migration(migrations_dir, "backfill.py", content)

        await migrate(store, migrations_dir)

        alice = await collection.find_one({"_id": "1"})
        assert alice is not None
        assert alice["status"] == "pending"

        charlie = await collection.find_one({"_id": "3"})
        assert charlie is not None
        assert charlie["status"] == "active"


class TestIndexMigrationStep:
    _INDEX_MIGRATION: str = textwrap.dedent("""\
        from causeway import register_migration
        from causeway.mongodb.helpers import IndexMigrationStep
        from typing import ClassVar

        register_migration(id="indexes", name="status index", previous=None, next=None)

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
        _write_migration(migrations_dir, "indexes.py", self._INDEX_MIGRATION)

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
        _write_migration(migrations_dir, "indexes.py", self._INDEX_MIGRATION)

        await migrate(store, migrations_dir)
        await rollback(store, migrations_dir, target=None)

        indexes = await db.get_collection("items").index_information()
        index_keys = [v["key"] for v in indexes.values()]
        assert [("status", 1)] not in index_keys
