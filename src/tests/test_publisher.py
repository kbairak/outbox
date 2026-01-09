import json

import asyncpg
import pytest
from psycopg2.extensions import connection as Psycopg2Connection

from outbox import OutboxMessage, Publisher

from .utils import Person, PublishType


@pytest.mark.asyncio
async def test_publish(publish: PublishType, db_connection_async: asyncpg.Connection) -> None:
    # act
    await publish(db_connection_async, "test_routing_key", "test_body")

    # assert
    messages = await db_connection_async.fetch(
        "SELECT routing_key, body, created_at, sent_at FROM outbox_table"
    )
    assert len(messages) == 1
    assert messages[0]["routing_key"] == "test_routing_key"
    assert messages[0]["body"] == b'"test_body"'
    assert messages[0]["created_at"] is not None
    assert messages[0]["sent_at"] is None


def test_publish_sync(db_connection_sync: Psycopg2Connection) -> None:
    # setup
    publisher = Publisher()

    # act
    publisher.publish_sync(db_connection_sync, "test_routing_key", "test_body")

    # assert
    with db_connection_sync.cursor() as cursor:
        cursor.execute("SELECT routing_key, body, created_at, sent_at FROM outbox_table")
        messages = cursor.fetchall()
        assert len(messages) == 1
        assert messages[0][0] == "test_routing_key"
        assert bytes(messages[0][1]) == b'"test_body"'
        assert messages[0][2] is not None
        assert messages[0][3] is None


@pytest.mark.asyncio
async def test_bulk_publish(publisher: Publisher, db_connection_async: asyncpg.Connection) -> None:
    # act
    messages = [
        OutboxMessage(routing_key="test_key_1", body="test_body_1"),
        OutboxMessage(routing_key="test_key_2", body="test_body_2"),
    ]
    await publisher.bulk_publish(db_connection_async, messages)

    # assert
    rows = await db_connection_async.fetch(
        "SELECT routing_key, body, created_at, sent_at FROM outbox_table ORDER BY id"
    )
    assert len(rows) == 2
    assert rows[0]["routing_key"] == "test_key_1"
    assert rows[0]["body"] == b'"test_body_1"'
    assert rows[0]["created_at"] is not None
    assert rows[0]["sent_at"] is None
    assert rows[1]["routing_key"] == "test_key_2"
    assert rows[1]["body"] == b'"test_body_2"'
    assert rows[1]["created_at"] is not None
    assert rows[1]["sent_at"] is None


@pytest.mark.asyncio
async def test_publish_with_pydantic(
    publish: PublishType, db_connection_async: asyncpg.Connection
) -> None:
    # act
    await publish(db_connection_async, "my_routing_key", Person(name="MyName"))

    # assert
    messages = await db_connection_async.fetch(
        "SELECT routing_key, body, created_at, sent_at FROM outbox_table"
    )
    assert len(messages) == 1
    assert messages[0]["routing_key"] == "my_routing_key"
    assert json.loads(messages[0]["body"]) == {"name": "MyName"}
    assert messages[0]["created_at"] is not None
    assert messages[0]["sent_at"] is None


def test_publish_with_psycopg_connection(
    db_connection_sync: Psycopg2Connection, publish: PublishType
) -> None:
    publish(db_connection_sync, "test_routing_key", "test_body")
    with db_connection_sync.cursor() as cursor:
        cursor.execute("SELECT routing_key, body FROM outbox_table")
        result = cursor.fetchall()
        assert len(result) == 1
        assert result[0][0] == "test_routing_key"
        assert bytes(result[0][1]) == b'"test_body"'


def test_publish_with_psycopg_cursor(
    db_connection_sync: Psycopg2Connection, publish: PublishType
) -> None:
    with db_connection_sync.cursor() as cursor:
        publish(cursor, "test_routing_key", "test_body")
        cursor.execute("SELECT routing_key, body FROM outbox_table")
        result = cursor.fetchall()
        assert len(result) == 1
        assert result[0][0] == "test_routing_key"
        assert bytes(result[0][1]) == b'"test_body"'


@pytest.mark.asyncio
async def test_publish_with_asyncpg_connection(db_url_async: str, publish: PublishType) -> None:
    connection = await asyncpg.connect(
        db_url_async.replace("postgresql+asyncpg://", "postgresql://")
    )
    await publish(connection, "test_routing_key", "test_body")
    records = await connection.fetch("SELECT routing_key, body FROM outbox_table")
    assert len(records) == 1
    assert records[0]["routing_key"] == "test_routing_key"
    assert records[0]["body"] == b'"test_body"'
