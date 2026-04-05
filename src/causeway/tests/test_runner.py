"""Tests for the causeway runner using an in-memory StateStore."""

import textwrap
from pathlib import Path
from typing import Any, Literal, override

import pytest

from causeway import MigrationStep, migrate, rollback, stamp, status
from causeway.runner import discover
from causeway.state import MigrationState

_STEP_IMPORTS = textwrap.dedent("""\
    from causeway import MigrationStep
    from typing import Any
""")

_NOOP_STEP = _STEP_IMPORTS + textwrap.dedent("""\

    class Step(MigrationStep):
        async def up(self, db: Any) -> None:
            pass
""")


class InMemoryStateStore:
    """Fake StateStore for testing the runner without any database."""

    _state: MigrationState
    _db: dict[str, list[dict[str, Any]]]

    def __init__(self) -> None:
        self._state = MigrationState()
        self._db = {}

    @property
    def db(self) -> dict[str, list[dict[str, Any]]]:
        return self._db

    async def read_state(self) -> MigrationState:
        return self._state.model_copy()

    async def update_state(
        self, version: int, step: int, name: str, direction: Literal["up", "down"]
    ) -> None:
        entry = MigrationState.make_history_entry(
            version=version, step=step, name=name, direction=direction
        )
        self._state.version = version
        self._state.step = step
        self._state.history.append(entry)

    async def stamp_state(self, version: int, step: int) -> None:
        self._state.version = version
        self._state.step = step


@pytest.fixture
def store() -> InMemoryStateStore:
    return InMemoryStateStore()


@pytest.fixture
def migrations_dir(tmp_path: Path) -> Path:
    return tmp_path


def _write_migration(migrations_dir: Path, filename: str, content: str) -> Path:
    file_path = migrations_dir / filename
    file_path.write_text(content)
    return file_path


def _step(body: str, cls_name: str = "Step") -> str:
    return (
        _STEP_IMPORTS
        + f"""
class {cls_name}(MigrationStep):
    async def up(self, db: Any) -> None:
        {body}
"""
    )


def _reversible_step(up_body: str, down_body: str, cls_name: str = "Step") -> str:
    return (
        _STEP_IMPORTS
        + f"""
class {cls_name}(MigrationStep):
    async def up(self, db: Any) -> None:
        {up_body}

    async def down(self, db: Any) -> None:
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
    async def up(self, db: Any) -> None:
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
    async def up(self, db: Any) -> None:
        pass

class StepTwo(MigrationStep):
    async def up(self, db: Any) -> None:
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
    async def test_applies_all_pending(
        self, store: InMemoryStateStore, migrations_dir: Path
    ) -> None:
        for v in [1, 2]:
            _write_migration(
                migrations_dir,
                f"00{v}_migration.py",
                _step(
                    f'db.setdefault("data", []).append("{v}")',
                    cls_name=f"Step{v}",
                ),
            )

        await migrate(store, migrations_dir)

        assert len(store.db.get("data", [])) == 2
        state = await store.read_state()
        assert state.version == 2
        assert len(state.history) == 2

    async def test_stops_at_target_version(
        self, store: InMemoryStateStore, migrations_dir: Path
    ) -> None:
        for v in range(1, 4):
            _write_migration(
                migrations_dir,
                f"00{v}_migration.py",
                _step(
                    f'db.setdefault("data", []).append("{v}")',
                    cls_name=f"Step{v}",
                ),
            )

        await migrate(store, migrations_dir, target_version=2)

        state = await store.read_state()
        assert state.version == 2
        assert len(store.db.get("data", [])) == 2

    async def test_is_noop_when_already_at_latest(
        self, store: InMemoryStateStore, migrations_dir: Path
    ) -> None:
        _write_migration(migrations_dir, "001_init.py", _NOOP_STEP)

        await migrate(store, migrations_dir)
        state_before = await store.read_state()

        await migrate(store, migrations_dir)
        state_after = await store.read_state()

        assert len(state_before.history) == len(state_after.history)

    async def test_dry_run_does_not_apply(
        self, store: InMemoryStateStore, migrations_dir: Path
    ) -> None:
        _write_migration(
            migrations_dir,
            "001_init.py",
            _step('db.setdefault("data", []).append("x")'),
        )

        await migrate(store, migrations_dir, dry_run=True)

        state = await store.read_state()
        assert state.version == 0
        assert store.db == {}

    async def test_stops_on_error_and_records_last_success(
        self, store: InMemoryStateStore, migrations_dir: Path
    ) -> None:
        content = (
            _STEP_IMPORTS
            + """
class SuccessStep(MigrationStep):
    async def up(self, db: Any) -> None:
        db.setdefault("data", []).append("ok")

class FailStep(MigrationStep):
    async def up(self, db: Any) -> None:
        raise RuntimeError("intentional failure")
"""
        )
        _write_migration(migrations_dir, "001_init.py", content)

        with pytest.raises(RuntimeError, match="intentional failure"):
            await migrate(store, migrations_dir)

        state = await store.read_state()
        assert state.version == 1
        assert state.step == 1
        assert len(state.history) == 1


class TestRollback:
    async def test_rolls_back_to_target(
        self, store: InMemoryStateStore, migrations_dir: Path
    ) -> None:
        for v in [1, 2]:
            _write_migration(
                migrations_dir,
                f"00{v}_migration.py",
                _reversible_step(
                    f'db.setdefault("data", []).append("{v}")',
                    f'db["data"].remove("{v}")',
                    cls_name=f"Step{v}",
                ),
            )

        await migrate(store, migrations_dir)
        assert len(store.db["data"]) == 2

        await rollback(store, migrations_dir, target_version=1)

        assert len(store.db["data"]) == 1
        state = await store.read_state()
        assert state.version == 1

    async def test_raises_on_irreversible(
        self, store: InMemoryStateStore, migrations_dir: Path
    ) -> None:
        for v in [1, 2]:
            _write_migration(migrations_dir, f"00{v}_migration.py", _NOOP_STEP)

        await migrate(store, migrations_dir)

        with pytest.raises(NotImplementedError, match="irreversible"):
            await rollback(store, migrations_dir, target_version=0)

    async def test_dry_run_does_not_roll_back(
        self, store: InMemoryStateStore, migrations_dir: Path
    ) -> None:
        _write_migration(
            migrations_dir,
            "001_init.py",
            _reversible_step(
                'db.setdefault("data", []).append("x")',
                'db["data"].remove("x")',
            ),
        )

        await migrate(store, migrations_dir)

        await rollback(store, migrations_dir, target_version=0, dry_run=True)

        state = await store.read_state()
        assert state.version == 1
        assert len(store.db["data"]) == 1


class TestStatus:
    async def test_returns_zero_state_and_pending(
        self, store: InMemoryStateStore, migrations_dir: Path
    ) -> None:
        _write_migration(migrations_dir, "001_init.py", _NOOP_STEP)

        result = await status(store, migrations_dir)

        assert result.current_version == 0
        assert result.current_step == 0
        assert len(result.pending) == 1
        assert result.pending[0].version == 1
        assert result.history == []


class TestStamp:
    async def test_stamp_sets_version(
        self, store: InMemoryStateStore, migrations_dir: Path
    ) -> None:
        _write_migration(migrations_dir, "001_init.py", _NOOP_STEP)

        await stamp(store, migrations_dir, version=1)

        state = await store.read_state()
        assert state.version == 1

    async def test_stamp_zero_resets(
        self, store: InMemoryStateStore, migrations_dir: Path
    ) -> None:
        _write_migration(migrations_dir, "001_init.py", _NOOP_STEP)
        await migrate(store, migrations_dir)

        await stamp(store, migrations_dir, version=0)

        state = await store.read_state()
        assert state.version == 0


class TestStepRegistration:
    def test_has_down_returns_true_when_overridden(self) -> None:
        class WithDown(MigrationStep[Any]):
            @override
            async def up(self, db: Any) -> None:
                pass

            @override
            async def down(self, db: Any) -> None:
                pass

        assert WithDown().has_down() is True

    def test_has_down_returns_false_when_not_overridden(self) -> None:
        class WithoutDown(MigrationStep[Any]):
            @override
            async def up(self, db: Any) -> None:
                pass

        assert WithoutDown().has_down() is False
