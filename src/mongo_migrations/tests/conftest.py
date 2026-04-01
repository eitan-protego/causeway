"""Test fixtures for the mongo_migrations module."""

import uuid
from collections.abc import AsyncGenerator
from typing import Any, cast

import pytest
from pymongo import AsyncMongoClient
from pymongo.asynchronous.database import AsyncDatabase


@pytest.fixture
async def db() -> AsyncGenerator[AsyncDatabase[dict[str, Any]], None]:
    """Provide a fresh mongomock database for each test."""
    from mongomock_motor import AsyncMongoMockClient

    client = cast(
        AsyncMongoClient[dict[str, Any]], cast(object, AsyncMongoMockClient())
    )
    database = client.get_database(f"test_db_{uuid.uuid4().hex[:12]}")
    yield database
    await client.drop_database(database.name)
