# causeway

A database-agnostic migration system for Python. Define migration steps as classes, track state via pluggable backends, and run forward/rollback migrations with version tracking.

Ships with a [MongoDB backend](#mongodb-backend) built on [pymongo](https://pymongo.readthedocs.io/) async and [typed-mongo](https://github.com/eitan-protego/typed-mongo).

## Installation

```bash
# Core only (bring your own StateStore)
uv add causeway@git+https://github.com/eitan-protego/causeway

# With MongoDB backend
uv add "causeway[mongodb]@git+https://github.com/eitan-protego/causeway"
```

## Quick start

### 1. Create a migrations directory

```
my_app/migrations/
├── 001_add_status_field.py
├── 002_create_indexes.py
└── 003_backfill_names.py
```

### 2. Write a migration

Each file is named `NNN_description.py` where `NNN` is a sequential version number starting from 1. Define one or more step subclasses per file — each class is a step within that version, ordered by definition order.

`MigrationStep` is generic over the database type `T`. For MongoDB, use `MongoMigrationStep` from `causeway.mongodb`:

```python
# 001_add_status_field.py
from typing import Any, override
from pymongo.asynchronous.database import AsyncDatabase
from causeway.mongodb import MongoMigrationStep

class AddStatusField(MongoMigrationStep):
    @override
    async def up(self, db: AsyncDatabase[dict[str, Any]]) -> None:
        await db.get_collection("cases").update_many(
            {"status": {"$exists": False}},
            {"$set": {"status": "pending"}},
        )

    @override
    async def down(self, db: AsyncDatabase[dict[str, Any]]) -> None:
        await db.get_collection("cases").update_many(
            {}, {"$unset": {"status": ""}}
        )
```

### 3. Run migrations

All runner functions take a `StateStore` instance (not a raw database object):

```python
from pathlib import Path
from causeway import migrate, rollback, status
from causeway.mongodb import MongoStateStore

MIGRATIONS_DIR = Path("my_app/migrations")
store = MongoStateStore(db)  # wraps an AsyncDatabase instance

# Apply all pending
await migrate(store, MIGRATIONS_DIR)

# Apply up to version 2
await migrate(store, MIGRATIONS_DIR, target_version=2)

# Preview without executing
await migrate(store, MIGRATIONS_DIR, dry_run=True)

# Roll back to version 1 (version 1 steps remain applied)
await rollback(store, MIGRATIONS_DIR, target_version=1)

# Check current state
result = await status(store, MIGRATIONS_DIR)
print(f"At v{result.current_version}, {len(result.pending)} pending")
```

## Architecture

### Core (`causeway`)

The core package is database-agnostic. It provides:

- **`MigrationStep[T]`** — Abstract base class for migration steps, generic over database type `T`. Subclass it and implement `up(db: T)` and optionally `down(db: T)`.
- **`StateStore[T]`** — Protocol that backends implement to persist migration state. Provides `db`, `read_state()`, `update_state()`, and `stamp_state()`.
- **Runner functions** — `migrate()`, `rollback()`, `status()`, `stamp()`, `discover()`, `load_version()` — all operate on a `StateStore` instance.

### Backends (`causeway.mongodb`, ...)

Backend packages provide concrete `StateStore` implementations and typed step base classes. The MongoDB backend provides:

- **`MongoStateStore`** — Stores state in a `_migrations` collection.
- **`MongoMigrationStep`** — Type alias for `MigrationStep[AsyncDatabase[dict[str, Any]]]`.
- **`DocumentMigrationStep`** — Iterates and transforms matching documents.
- **`IndexMigrationStep`** — Creates/drops indexes declaratively.

## API

### Runner functions

| Function | Description |
|---|---|
| `migrate(store, path, target_version=None, dry_run=False)` | Apply pending migrations up to `target_version` (default: all) |
| `rollback(store, path, target_version, dry_run=False)` | Roll back to `target_version` (that version's steps remain applied) |
| `status(store, path)` | Return current version, step, pending steps, and history |
| `stamp(store, path, version, step=None)` | Set migration state without running steps (use `version=0` to reset) |
| `discover(path)` | Discover and return all resolved steps from migration files |
| `load_version(path, version)` | Load step classes for a specific version (useful in tests) |

### StateStore protocol

Implement this protocol to add support for a new database:

```python
from causeway import StateStore, MigrationState

class MyStateStore:
    def __init__(self, db: MyDbType) -> None:
        self._db = db

    @property
    def db(self) -> MyDbType:
        return self._db

    async def read_state(self) -> MigrationState:
        """Read current state. Return default MigrationState() if none exists."""
        ...

    async def update_state(self, version: int, step: int, name: str, direction: Literal["up", "down"]) -> None:
        """Record a step execution and update current position."""
        ...

    async def stamp_state(self, version: int, step: int) -> None:
        """Set position without recording history."""
        ...
```

### MigrationStep[T]

The base class for all migrations. Implement `up()` (required) and optionally `down()` for rollback support. Steps without `down()` are marked irreversible and block rollback.

The class name is auto-converted to a human-readable name: `BackfillUserStates` becomes "backfill user states".

## MongoDB backend

Install with the `mongodb` extra: `causeway[mongodb]`.

### MongoMigrationStep

Base class for MongoDB migrations — equivalent to `MigrationStep[AsyncDatabase[dict[str, Any]]]`.

### DocumentMigrationStep

Iterates matching documents and applies a sync transform to each:

```python
from causeway.mongodb import DocumentMigrationStep

class NormalizeName(DocumentMigrationStep):
    collection_name: ClassVar[str] = "users"
    query: ClassVar[dict[str, Any]] = {"name": {"$exists": True}}

    def transform(self, doc: dict[str, Any]) -> dict[str, Any]:
        doc["name"] = doc["name"].strip().lower()
        return doc
```

### IndexMigrationStep

Creates an index on `up()`, drops it on `down()` — both auto-implemented:

```python
from causeway.mongodb import IndexMigrationStep

class CaseStatusIndex(IndexMigrationStep):
    collection_name: ClassVar[str] = "cases"
    index: ClassVar[list[tuple[str, int]]] = [("status", 1), ("created_at", -1)]
    unique: ClassVar[bool] = True   # optional, default False
    sparse: ClassVar[bool] = False  # optional
    index_name: ClassVar[str | None] = None  # optional, auto-generated if omitted
```

## How it works

- **Discovery**: Scans a directory for files matching `[0-9]*_*.py`, loads them dynamically, and collects `MigrationStep` subclasses in definition order.
- **Versioning**: Version numbers must be sequential starting from 1 with no gaps or duplicates. Each file is one version; each class in the file is a step within that version.
- **State tracking**: Delegated to the `StateStore` backend. The MongoDB backend stores state in a `_migrations` collection as a single document (`_id: "state"`) with current version, step, and full history.
- **Rollback safety**: Before executing any rollback, all steps to be rolled back are pre-validated for `down()` implementations. If any step is irreversible, the rollback is rejected before any changes are made.

## Testing migrations

Use `load_version()` to load step classes for unit testing:

```python
from pathlib import Path
from causeway import load_version

MIGRATIONS_DIR = Path(__file__).parent.parent
BackfillStep = load_version(MIGRATIONS_DIR, 1)[0]

async def test_backfill(db):
    await db.get_collection("items").insert_one({"_id": "1"})
    await BackfillStep().up(db)
    item = await db.get_collection("items").find_one({"_id": "1"})
    assert item["status"] == "pending"
```

## Development

```bash
uv sync --dev
uv run pytest
uv run ruff check
uv run ruff format --check
uv run basedpyright
```
