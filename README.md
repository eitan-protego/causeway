# mongo_migrations

A file-based migration system for MongoDB, built on [pymongo](https://pymongo.readthedocs.io/) async and [typed-mongo](https://github.com/eitan-protego/typed-mongo).

Inspired by Alembic's approach to SQL migrations, adapted for MongoDB's schema-free world — backfills, index changes, and document transforms with version tracking and rollback support.

## Installation

```bash
uv add mongo-migrations@git+https://github.com/eitan-protego/mongo_migrations
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

Each file is named `NNN_description.py` where `NNN` is a sequential version number starting from 1. Define one or more `MigrationStep` subclasses per file — each class is a step within that version, ordered by definition order.

```python
# 001_add_status_field.py
from typing import Any, override
from pymongo.asynchronous.database import AsyncDatabase
from mongo_migrations import MigrationStep

class AddStatusField(MigrationStep):
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

```python
from pathlib import Path
from mongo_migrations import migrate, rollback, status

MIGRATIONS_DIR = Path("my_app/migrations")

# Apply all pending
await migrate(db, MIGRATIONS_DIR)

# Apply up to version 2
await migrate(db, MIGRATIONS_DIR, target_version=2)

# Preview without executing
await migrate(db, MIGRATIONS_DIR, dry_run=True)

# Roll back to version 1 (version 1 steps remain applied)
await rollback(db, MIGRATIONS_DIR, target_version=1)

# Check current state
result = await status(db, MIGRATIONS_DIR)
print(f"At v{result.current_version}, {len(result.pending)} pending")
```

## API

### Core functions

| Function | Description |
|---|---|
| `migrate(db, path, target_version=None, dry_run=False)` | Apply pending migrations up to `target_version` (default: all) |
| `rollback(db, path, target_version, dry_run=False)` | Roll back to `target_version` (that version's steps remain applied) |
| `status(db, path)` | Return current version, step, pending steps, and history |
| `stamp(db, path, version, step=None)` | Set migration state without running steps (use `version=0` to reset) |
| `discover(path)` | Discover and return all resolved steps from migration files |
| `load_version(path, version)` | Load step classes for a specific version (useful in tests) |

### Base classes

#### `MigrationStep`

The base class for all migrations. Implement `up()` (required) and optionally `down()` for rollback support. Steps without `down()` are marked irreversible and block rollback.

The class name is auto-converted to a human-readable name: `BackfillUserStates` becomes "backfill user states".

#### `DocumentMigrationStep`

Iterates matching documents and applies a sync transform to each:

```python
from mongo_migrations import DocumentMigrationStep

class NormalizeName(DocumentMigrationStep):
    collection_name: ClassVar[str] = "users"
    query: ClassVar[dict[str, Any]] = {"name": {"$exists": True}}

    def transform(self, doc: dict[str, Any]) -> dict[str, Any]:
        doc["name"] = doc["name"].strip().lower()
        return doc
```

#### `IndexMigrationStep`

Creates an index on `up()`, drops it on `down()` — both auto-implemented:

```python
from mongo_migrations import IndexMigrationStep

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
- **State tracking**: Migration state is stored in a `_migrations` collection as a single document (`_id: "state"`) containing the current version, step, and a full history log of all applied/rolled-back steps.
- **Rollback safety**: Before executing any rollback, all steps to be rolled back are pre-validated for `down()` implementations. If any step is irreversible, the rollback is rejected before any changes are made.

## Testing migrations

Use `load_version()` to load step classes for unit testing:

```python
from pathlib import Path
from mongo_migrations import load_version

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
