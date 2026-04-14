"""Tests for the causeway runner using an in-memory StateStore."""

import textwrap
from pathlib import Path
from typing import Any, Literal, override

import pytest

from causeway import MigrationStep, migrate, rollback, stamp, status
from causeway.runner import discover
from causeway.state import MigrationState

_STEP_IMPORTS = textwrap.dedent("""\
    from causeway import MigrationStep, register_migration
    from typing import Any
""")


def _noop_migration(
    migration_id: str,
    previous: str | None = None,
    next: str | None = None,
    name: str | None = None,
    docstring: str | None = None,
) -> str:
    """Generate a simple no-op migration file."""
    parts = [f'id="{migration_id}"']
    if name is not None:
        parts.append(f'name="{name}"')
    parts.append(f"previous={_quote_or_none(previous)}")
    parts.append(f"next={_quote_or_none(next)}")
    reg_call = f"register_migration({', '.join(parts)})"

    doc = ""
    if docstring is not None:
        doc = f'"""{docstring}"""\n'

    return (
        doc
        + _STEP_IMPORTS
        + f"\n{reg_call}\n"
        + textwrap.dedent("""
    class Step(MigrationStep):
        async def up(self, db: Any) -> None:
            pass
    """)
    )


def _quote_or_none(val: str | None) -> str:
    return f'"{val}"' if val is not None else "None"


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
    async def up(self, db: Any) -> None:
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
    async def up(self, db: Any) -> None:
        {up_body}

    async def down(self, db: Any) -> None:
        {down_body}
"""
    )


class TestDiscovery:
    def test_returns_empty_list_for_empty_directory(self, migrations_dir: Path) -> None:
        assert discover(migrations_dir) == []

    def test_extracts_name_from_registration(self, migrations_dir: Path) -> None:
        content = (
            _STEP_IMPORTS
            + '\nregister_migration(id="init", name="initialize", previous=None, next=None)\n'
            + """
class CreateUsersCollection(MigrationStep):
    async def up(self, db: Any) -> None:
        pass
"""
        )
        _write_migration(migrations_dir, "init.py", content)

        steps = discover(migrations_dir)

        assert len(steps) == 1
        assert steps[0].version == 1
        assert steps[0].step == 1
        assert steps[0].migration_id == "init"
        assert steps[0].name == "create users collection"

    def test_preserves_class_definition_order_within_file(
        self, migrations_dir: Path
    ) -> None:
        content = (
            _STEP_IMPORTS
            + '\nregister_migration(id="init", name="init", previous=None, next=None)\n'
            + """
class StepOne(MigrationStep):
    async def up(self, db: Any) -> None:
        pass

class StepTwo(MigrationStep):
    async def up(self, db: Any) -> None:
        pass
"""
        )
        _write_migration(migrations_dir, "init.py", content)

        steps = discover(migrations_dir)

        assert len(steps) == 2
        assert steps[0].step == 1
        assert steps[0].name == "step one"
        assert steps[1].step == 2
        assert steps[1].name == "step two"

    def test_orders_steps_across_files_by_linked_list(
        self, migrations_dir: Path
    ) -> None:
        _write_migration(
            migrations_dir,
            "first.py",
            _step(
                "pass",
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
                "pass",
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
                "pass",
                cls_name="Step3",
                migration_id="m3",
                previous="m2",
                next=None,
                name="third",
            ),
        )

        steps = discover(migrations_dir)

        assert [s.version for s in steps] == [1, 2, 3]
        assert [s.migration_id for s in steps] == ["m1", "m2", "m3"]

    def test_raises_on_duplicate_migration_id(self, migrations_dir: Path) -> None:
        _write_migration(
            migrations_dir,
            "a.py",
            _step("pass", migration_id="dup", previous=None, next=None),
        )
        _write_migration(
            migrations_dir,
            "b.py",
            _step("pass", migration_id="dup", previous=None, next=None),
        )

        with pytest.raises(ValueError, match="Duplicate migration id"):
            discover(migrations_dir)

    def test_raises_on_no_registration(self, migrations_dir: Path) -> None:
        content = (
            _STEP_IMPORTS
            + """
class Step(MigrationStep):
    async def up(self, db: Any) -> None:
        pass
"""
        )
        _write_migration(migrations_dir, "bad.py", content)

        with pytest.raises(ValueError, match="did not call register_migration"):
            discover(migrations_dir)

    def test_raises_on_multiple_registrations(self, migrations_dir: Path) -> None:
        content = (
            _STEP_IMPORTS
            + """
register_migration(id="a", name="a", previous=None, next=None)
register_migration(id="b", name="b", previous=None, next=None)

class Step(MigrationStep):
    async def up(self, db: Any) -> None:
        pass
"""
        )
        _write_migration(migrations_dir, "bad.py", content)

        with pytest.raises(ValueError, match="called register_migration\\(\\) 2 times"):
            discover(migrations_dir)

    def test_raises_on_multiple_heads(self, migrations_dir: Path) -> None:
        _write_migration(
            migrations_dir,
            "a.py",
            _step("pass", migration_id="a", previous=None, next=None, name="a"),
        )
        _write_migration(
            migrations_dir,
            "b.py",
            _step("pass", migration_id="b", previous=None, next=None, name="b"),
        )

        with pytest.raises(ValueError, match="Multiple initial migrations"):
            discover(migrations_dir)

    def test_raises_on_inconsistent_links(self, migrations_dir: Path) -> None:
        _write_migration(
            migrations_dir,
            "a.py",
            _step("pass", migration_id="a", previous=None, next="b", name="a"),
        )
        _write_migration(
            migrations_dir,
            "b.py",
            _step("pass", migration_id="b", previous=None, next=None, name="b"),
        )

        with pytest.raises(ValueError, match="Inconsistent links"):
            discover(migrations_dir)

    def test_raises_on_missing_reference(self, migrations_dir: Path) -> None:
        _write_migration(
            migrations_dir,
            "a.py",
            _step(
                "pass", migration_id="a", previous=None, next="nonexistent", name="a"
            ),
        )

        with pytest.raises(ValueError, match="does not exist"):
            discover(migrations_dir)

    def test_raises_on_unreachable_migrations(self, migrations_dir: Path) -> None:
        # Two separate chains: a->b and c->d
        _write_migration(
            migrations_dir,
            "a.py",
            _step("pass", migration_id="a", previous=None, next="b", name="a"),
        )
        _write_migration(
            migrations_dir,
            "b.py",
            _step("pass", migration_id="b", previous="a", next=None, name="b"),
        )
        _write_migration(
            migrations_dir,
            "c.py",
            _step("pass", migration_id="c", previous="b", next="d", name="c"),
        )
        _write_migration(
            migrations_dir,
            "d.py",
            _step("pass", migration_id="d", previous="c", next=None, name="d"),
        )

        # b.next=None but c.previous=b => inconsistent
        with pytest.raises(ValueError, match="Inconsistent links"):
            discover(migrations_dir)

    def test_skips_init_py(self, migrations_dir: Path) -> None:
        (migrations_dir / "__init__.py").write_text("")
        _write_migration(
            migrations_dir,
            "init.py",
            _step("pass", migration_id="init", previous=None, next=None, name="init"),
        )

        steps = discover(migrations_dir)
        assert len(steps) == 1

    def test_auto_fills_name_from_docstring(self, migrations_dir: Path) -> None:
        content = '"""Initialize the database."""\n' + (
            _STEP_IMPORTS
            + '\nregister_migration(id="init", previous=None, next=None)\n'
            + """
class Step(MigrationStep):
    async def up(self, db: Any) -> None:
        pass
"""
        )
        _write_migration(migrations_dir, "init.py", content)

        steps = discover(migrations_dir)
        assert len(steps) == 1

    def test_auto_fills_description_from_docstring(self, migrations_dir: Path) -> None:
        content = (
            '"""Initialize the database.\n\nSets up initial collections and indexes.\n"""\n'
            + (
                _STEP_IMPORTS
                + '\nregister_migration(id="init", previous=None, next=None)\n'
                + """
class Step(MigrationStep):
    async def up(self, db: Any) -> None:
        pass
"""
            )
        )
        _write_migration(migrations_dir, "init.py", content)

        # Should not raise — name and description auto-filled
        steps = discover(migrations_dir)
        assert len(steps) == 1

    def test_raises_on_missing_name_and_no_docstring(
        self, migrations_dir: Path
    ) -> None:
        content = (
            _STEP_IMPORTS
            + '\nregister_migration(id="init", previous=None, next=None)\n'
            + """
class Step(MigrationStep):
    async def up(self, db: Any) -> None:
        pass
"""
        )
        _write_migration(migrations_dir, "init.py", content)

        with pytest.raises(ValueError, match="no name and no module docstring"):
            discover(migrations_dir)

    def test_raises_on_missing_name_and_empty_docstring_first_line(
        self, migrations_dir: Path
    ) -> None:
        content = '"""\n\nSome description.\n"""\n' + (
            _STEP_IMPORTS
            + '\nregister_migration(id="init", previous=None, next=None)\n'
            + """
class Step(MigrationStep):
    async def up(self, db: Any) -> None:
        pass
"""
        )
        _write_migration(migrations_dir, "init.py", content)

        with pytest.raises(ValueError, match="empty first line"):
            discover(migrations_dir)

    def test_validates_migration_id_format(self, migrations_dir: Path) -> None:
        content = (
            _STEP_IMPORTS
            + '\nregister_migration(id="bad id!", name="bad", previous=None, next=None)\n'
            + """
class Step(MigrationStep):
    async def up(self, db: Any) -> None:
        pass
"""
        )
        _write_migration(migrations_dir, "bad.py", content)

        with pytest.raises(ValueError, match="alphanumeric and dash"):
            discover(migrations_dir)


class TestMigrate:
    async def test_applies_all_pending(
        self, store: InMemoryStateStore, migrations_dir: Path
    ) -> None:
        _write_migration(
            migrations_dir,
            "first.py",
            _step(
                'db.setdefault("data", []).append("1")',
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
                'db.setdefault("data", []).append("2")',
                cls_name="Step2",
                migration_id="m2",
                previous="m1",
                next=None,
                name="second",
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
        _write_migration(
            migrations_dir,
            "first.py",
            _step(
                'db.setdefault("data", []).append("1")',
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
                'db.setdefault("data", []).append("2")',
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
                'db.setdefault("data", []).append("3")',
                cls_name="Step3",
                migration_id="m3",
                previous="m2",
                next=None,
                name="third",
            ),
        )

        await migrate(store, migrations_dir, target_version=2)

        state = await store.read_state()
        assert state.version == 2
        assert len(store.db.get("data", [])) == 2

    async def test_is_noop_when_already_at_latest(
        self, store: InMemoryStateStore, migrations_dir: Path
    ) -> None:
        _write_migration(
            migrations_dir,
            "init.py",
            _noop_migration("init", previous=None, next=None, name="init"),
        )

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
            "init.py",
            _step(
                'db.setdefault("data", []).append("x")',
                migration_id="init",
                previous=None,
                next=None,
                name="init",
            ),
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
            + '\nregister_migration(id="init", name="init", previous=None, next=None)\n'
            + """
class SuccessStep(MigrationStep):
    async def up(self, db: Any) -> None:
        db.setdefault("data", []).append("ok")

class FailStep(MigrationStep):
    async def up(self, db: Any) -> None:
        raise RuntimeError("intentional failure")
"""
        )
        _write_migration(migrations_dir, "init.py", content)

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
        _write_migration(
            migrations_dir,
            "first.py",
            _reversible_step(
                'db.setdefault("data", []).append("1")',
                'db["data"].remove("1")',
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
                'db.setdefault("data", []).append("2")',
                'db["data"].remove("2")',
                cls_name="Step2",
                migration_id="m2",
                previous="m1",
                next=None,
                name="second",
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
        _write_migration(
            migrations_dir,
            "first.py",
            _noop_migration("m1", previous=None, next="m2", name="first"),
        )
        _write_migration(
            migrations_dir,
            "second.py",
            _noop_migration("m2", previous="m1", next=None, name="second"),
        )

        await migrate(store, migrations_dir)

        with pytest.raises(NotImplementedError, match="irreversible"):
            await rollback(store, migrations_dir, target_version=0)

    async def test_dry_run_does_not_roll_back(
        self, store: InMemoryStateStore, migrations_dir: Path
    ) -> None:
        _write_migration(
            migrations_dir,
            "init.py",
            _reversible_step(
                'db.setdefault("data", []).append("x")',
                'db["data"].remove("x")',
                migration_id="init",
                previous=None,
                next=None,
                name="init",
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
        _write_migration(
            migrations_dir,
            "init.py",
            _noop_migration("init", previous=None, next=None, name="init"),
        )

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
        _write_migration(
            migrations_dir,
            "init.py",
            _noop_migration("init", previous=None, next=None, name="init"),
        )

        await stamp(store, migrations_dir, version=1)

        state = await store.read_state()
        assert state.version == 1

    async def test_stamp_zero_resets(
        self, store: InMemoryStateStore, migrations_dir: Path
    ) -> None:
        _write_migration(
            migrations_dir,
            "init.py",
            _noop_migration("init", previous=None, next=None, name="init"),
        )
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
