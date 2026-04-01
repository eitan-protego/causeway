"""Tests for the MongoDB migration system."""

import textwrap
from pathlib import Path
from typing import Any, override

import pytest
from pymongo.asynchronous.database import AsyncDatabase

from mongo_migrations import (
    MigrationStep,
    migrate,
    rollback,
    status,
)
from mongo_migrations.runner import discover
from mongo_migrations.state import MigrationState

_STEP_IMPORTS = textwrap.dedent("""\
    from mongo_migrations import MigrationStep
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


async def _get_state(
    db: AsyncDatabase[dict[str, Any]],
) -> MigrationState:
    collection = MigrationState.get_collection(db)
    doc = await collection.find_one({"_id": "state"})
    assert doc is not None, "Migration state document not found"
    return MigrationState.model_validate(doc)


def _step(body: str, cls_name: str = "Step") -> str:
    """Build a single-step migration file with the given up() body."""
    return (
        _STEP_IMPORTS
        + f"""
class {cls_name}(MigrationStep):
    async def up(self, db: AsyncDatabase[dict[str, Any]]) -> None:
        {body}
"""
    )


def _reversible_step(up_body: str, down_body: str, cls_name: str = "Step") -> str:
    """Build a single reversible migration step."""
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


class TestDiscovery:
    def test_returns_empty_list_for_empty_directory(self, migrations_dir: Path) -> None:
        assert discover(migrations_dir) == []

    def test_extracts_version_step_and_name_from_single_file(
        self, migrations_dir: Path
    ) -> None:
        content = (
            _STEP_IMPORTS
            + """
class CreateUsersCollection(MigrationStep):
    async def up(self, db: AsyncDatabase[dict[str, Any]]) -> None:
        pass
"""
        )
        _write_migration(migrations_dir, "001_init.py", content)

        steps = discover(migrations_dir)

        assert len(steps) == 1
        assert steps[0].version == 1
        assert steps[0].step == 1
        assert steps[0].name == "create users collection"

    def test_preserves_class_definition_order_within_file(
        self, migrations_dir: Path
    ) -> None:
        content = (
            _STEP_IMPORTS
            + """
class StepOne(MigrationStep):
    async def up(self, db: AsyncDatabase[dict[str, Any]]) -> None:
        pass

class StepTwo(MigrationStep):
    async def up(self, db: AsyncDatabase[dict[str, Any]]) -> None:
        pass
"""
        )
        _write_migration(migrations_dir, "001_init.py", content)

        steps = discover(migrations_dir)

        assert len(steps) == 2
        assert steps[0].step == 1
        assert steps[0].name == "step one"
        assert steps[1].step == 2
        assert steps[1].name == "step two"

    def test_orders_steps_across_multiple_files_by_version(
        self, migrations_dir: Path
    ) -> None:
        for i in range(1, 4):
            _write_migration(
                migrations_dir,
                f"00{i}_migration.py",
                _step("pass", cls_name=f"Step{i}"),
            )

        steps = discover(migrations_dir)

        assert [s.version for s in steps] == [1, 2, 3]

    def test_raises_on_duplicate_version_numbers(self, migrations_dir: Path) -> None:
        for suffix in ["a", "b"]:
            _write_migration(migrations_dir, f"001_{suffix}.py", _NOOP_STEP)

        with pytest.raises(ValueError, match="Duplicate migration version"):
            discover(migrations_dir)

    def test_raises_on_non_sequential_version_numbers(
        self, migrations_dir: Path
    ) -> None:
        for v in [1, 3]:
            _write_migration(migrations_dir, f"00{v}_migration.py", _NOOP_STEP)

        with pytest.raises(ValueError, match="Non-sequential"):
            discover(migrations_dir)


class TestMigrate:
    async def test_creates_state_on_first_run_against_empty_database(
        self, db: AsyncDatabase[dict[str, Any]], migrations_dir: Path
    ) -> None:
        existing = await db.list_collection_names()
        assert "_migrations" not in existing

        _write_migration(
            migrations_dir,
            "001_init.py",
            _step('await db.get_collection("data").insert_one({"_id": "v1"})'),
        )

        await migrate(db, migrations_dir)

        state = await _get_state(db)
        assert state.version == 1
        assert state.step == 1
        assert len(state.history) == 1
        assert state.history[0].direction == "up"
        assert await db.get_collection("data").count_documents({}) == 1

    async def test_applies_all_pending_versions_in_order(
        self, db: AsyncDatabase[dict[str, Any]], migrations_dir: Path
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

        await migrate(db, migrations_dir)

        assert await db.get_collection("data").count_documents({}) == 2
        state = await _get_state(db)
        assert state.version == 2
        assert len(state.history) == 2

    async def test_stops_at_target_version(
        self, db: AsyncDatabase[dict[str, Any]], migrations_dir: Path
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

        await migrate(db, migrations_dir, target_version=2)

        state = await _get_state(db)
        assert state.version == 2
        assert await db.get_collection("data").count_documents({}) == 2

    async def test_is_noop_when_already_at_latest(
        self, db: AsyncDatabase[dict[str, Any]], migrations_dir: Path
    ) -> None:
        _write_migration(migrations_dir, "001_init.py", _NOOP_STEP)

        await migrate(db, migrations_dir)
        state_before = await _get_state(db)

        await migrate(db, migrations_dir)
        state_after = await _get_state(db)

        assert len(state_before.history) == len(state_after.history)

    async def test_dry_run_does_not_apply_changes(
        self, db: AsyncDatabase[dict[str, Any]], migrations_dir: Path
    ) -> None:
        _write_migration(
            migrations_dir,
            "001_init.py",
            _step('await db.get_collection("data").insert_one({"_id": "x"})'),
        )

        await migrate(db, migrations_dir, dry_run=True)

        collection = MigrationState.get_collection(db)
        assert await collection.find_one({"_id": "state"}) is None
        assert await db.get_collection("data").count_documents({}) == 0

    async def test_stops_on_step_error_and_records_last_success(
        self, db: AsyncDatabase[dict[str, Any]], migrations_dir: Path
    ) -> None:
        content = (
            _STEP_IMPORTS
            + """
class SuccessStep(MigrationStep):
    async def up(self, db: AsyncDatabase[dict[str, Any]]) -> None:
        await db.get_collection("data").insert_one({"_id": "ok"})

class FailStep(MigrationStep):
    async def up(self, db: AsyncDatabase[dict[str, Any]]) -> None:
        raise RuntimeError("intentional failure")
"""
        )
        _write_migration(migrations_dir, "001_init.py", content)

        with pytest.raises(RuntimeError, match="intentional failure"):
            await migrate(db, migrations_dir)

        state = await _get_state(db)
        assert state.version == 1
        assert state.step == 1
        assert len(state.history) == 1


class TestRollback:
    async def test_rolls_back_to_target_version(
        self, db: AsyncDatabase[dict[str, Any]], migrations_dir: Path
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

        await migrate(db, migrations_dir)
        assert await db.get_collection("data").count_documents({}) == 2

        await rollback(db, migrations_dir, target_version=1)

        assert await db.get_collection("data").count_documents({}) == 1
        state = await _get_state(db)
        assert state.version == 1
        assert state.step == 1

    async def test_raises_before_executing_when_steps_are_irreversible(
        self, db: AsyncDatabase[dict[str, Any]], migrations_dir: Path
    ) -> None:
        for v in [1, 2]:
            _write_migration(
                migrations_dir,
                f"00{v}_migration.py",
                _NOOP_STEP,
            )

        await migrate(db, migrations_dir)

        with pytest.raises(NotImplementedError, match="irreversible"):
            await rollback(db, migrations_dir, target_version=0)

    async def test_dry_run_does_not_roll_back(
        self, db: AsyncDatabase[dict[str, Any]], migrations_dir: Path
    ) -> None:
        _write_migration(
            migrations_dir,
            "001_init.py",
            _reversible_step(
                'await db.get_collection("data").insert_one({"_id": "x"})',
                'await db.get_collection("data").delete_one({"_id": "x"})',
            ),
        )

        await migrate(db, migrations_dir)

        await rollback(db, migrations_dir, target_version=0, dry_run=True)

        state = await _get_state(db)
        assert state.version == 1
        assert await db.get_collection("data").count_documents({}) == 1


class TestDocumentMigrationStep:
    async def test_transforms_matching_documents_and_skips_non_matching(
        self, db: AsyncDatabase[dict[str, Any]], migrations_dir: Path
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
            from mongo_migrations.helpers import DocumentMigrationStep
            from typing import Any, ClassVar

            class BackfillStatus(DocumentMigrationStep):
                collection_name: ClassVar[str] = "items"
                query: ClassVar[dict[str, Any]] = {"status": {"$exists": False}}

                def transform(self, doc: dict[str, Any]) -> dict[str, Any]:
                    doc["status"] = "pending"
                    return doc
        """)
        _write_migration(migrations_dir, "001_backfill.py", content)

        await migrate(db, migrations_dir)

        alice = await collection.find_one({"_id": "1"})
        assert alice is not None
        assert alice["status"] == "pending"

        charlie = await collection.find_one({"_id": "3"})
        assert charlie is not None
        assert charlie["status"] == "active"


class TestIndexMigrationStep:
    _INDEX_MIGRATION: str = textwrap.dedent("""\
        from mongo_migrations.helpers import IndexMigrationStep
        from typing import ClassVar

        class StatusIndex(IndexMigrationStep):
            collection_name: ClassVar[str] = "items"
            index: ClassVar[list[tuple[str, int]]] = [("status", 1)]
    """)

    async def test_creates_index_on_upgrade(
        self, db: AsyncDatabase[dict[str, Any]], migrations_dir: Path
    ) -> None:
        _write_migration(migrations_dir, "001_indexes.py", self._INDEX_MIGRATION)

        await migrate(db, migrations_dir)

        indexes = await db.get_collection("items").index_information()
        index_keys = [v["key"] for v in indexes.values()]
        assert [("status", 1)] in index_keys

    async def test_drops_index_on_rollback(
        self, db: AsyncDatabase[dict[str, Any]], migrations_dir: Path
    ) -> None:
        _write_migration(migrations_dir, "001_indexes.py", self._INDEX_MIGRATION)

        await migrate(db, migrations_dir)

        await rollback(db, migrations_dir, target_version=0)

        indexes = await db.get_collection("items").index_information()
        index_keys = [v["key"] for v in indexes.values()]
        assert [("status", 1)] not in index_keys


class TestStatus:
    async def test_returns_zero_state_and_pending_on_fresh_database(
        self, db: AsyncDatabase[dict[str, Any]], migrations_dir: Path
    ) -> None:
        _write_migration(migrations_dir, "001_init.py", _NOOP_STEP)

        result = await status(db, migrations_dir)

        assert result.current_version == 0
        assert result.current_step == 0
        assert len(result.pending) == 1
        assert result.pending[0].version == 1
        assert result.history == []


class TestStepRegistration:
    def test_has_down_returns_true_when_down_is_overridden(self) -> None:
        class WithDown(MigrationStep):
            @override
            async def up(self, db: AsyncDatabase[dict[str, Any]]) -> None:
                pass

            @override
            async def down(self, db: AsyncDatabase[dict[str, Any]]) -> None:
                pass

        assert WithDown().has_down() is True

    def test_has_down_returns_false_when_down_is_not_overridden(
        self,
    ) -> None:
        class WithoutDown(MigrationStep):
            @override
            async def up(self, db: AsyncDatabase[dict[str, Any]]) -> None:
                pass

        assert WithoutDown().has_down() is False
