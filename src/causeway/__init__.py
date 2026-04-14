"""Database-agnostic migration system.

Tracks migration state via a StateStore backend, discovers migration files
via register_migration() calls, and provides base classes for defining
migration steps.
"""

from causeway.base import MigrationStep
from causeway.creator import create
from causeway.loader import ResolvedStep, discover, load_version
from causeway.registration import MigrationMetadata, register_migration
from causeway.runner import MigrationStatus, migrate, rollback, stamp, status
from causeway.state import MigrationHistoryEntry, MigrationState, StateStore

__all__ = [
    "MigrationMetadata",
    "MigrationStep",
    "MigrationHistoryEntry",
    "MigrationState",
    "StateStore",
    "MigrationStatus",
    "ResolvedStep",
    "create",
    "discover",
    "load_version",
    "migrate",
    "register_migration",
    "rollback",
    "stamp",
    "status",
]
