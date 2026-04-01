"""MongoDB migration system.

Tracks migration state in a _migrations collection, discovers migration files
by filename convention, and provides base classes for defining migration steps.
"""

from mongo_migrations.base import MigrationStep
from mongo_migrations.helpers import DocumentMigrationStep, IndexMigrationStep
from mongo_migrations.runner import (
    MigrationStatus,
    discover,
    load_version,
    migrate,
    rollback,
    stamp,
    status,
)

__all__ = [
    "MigrationStep",
    "DocumentMigrationStep",
    "IndexMigrationStep",
    "discover",
    "load_version",
    "migrate",
    "rollback",
    "stamp",
    "status",
    "MigrationStatus",
]
