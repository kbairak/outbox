import json

import pytest
from sqlalchemy import Engine, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from outbox import OutboxMessage, Publisher
from outbox.database import OutboxTable

from .utils import Person, PublishType


@pytest.mark.asyncio(loop_scope="session")
async def test_publish(publish: PublishType, session: AsyncSession) -> None:
    # act
    await publish(session, "test_routing_key", "test_body")

    # assert
    messages = (await session.execute(select(OutboxTable))).scalars().all()
    assert len(messages) == 1
    assert messages[0].routing_key == "test_routing_key"
    assert messages[0].body == b'"test_body"'
    assert messages[0].created_at is not None
    assert messages[0].sent_at is None


def test_publish_sync(db_engine_sync: Engine, session_sync: Session) -> None:
    # setup
    publisher = Publisher(db_engine=db_engine_sync, auto_create_table=True)

    # act
    publisher.publish_sync(session_sync, "test_routing_key", "test_body")

    # assert
    messages = (session_sync.execute(select(OutboxTable))).scalars().all()
    assert len(messages) == 1
    assert messages[0].routing_key == "test_routing_key"
    assert messages[0].body == b'"test_body"'
    assert messages[0].created_at is not None
    assert messages[0].sent_at is None


@pytest.mark.asyncio(loop_scope="session")
async def test_bulk_publish(publisher: Publisher, session: AsyncSession) -> None:
    # act
    messages = [
        OutboxMessage(routing_key="test_key_1", body="test_body_1"),
        OutboxMessage(routing_key="test_key_2", body="test_body_2"),
    ]
    await publisher.bulk_publish(session, messages)
    await session.commit()

    # assert
    rows = (await session.execute(select(OutboxTable))).scalars().all()
    assert len(rows) == 2
    assert rows[0].routing_key == "test_key_1"
    assert rows[0].body == b'"test_body_1"'
    assert rows[0].created_at is not None
    assert rows[0].sent_at is None
    assert rows[1].routing_key == "test_key_2"
    assert rows[1].body == b'"test_body_2"'
    assert rows[1].created_at is not None
    assert rows[1].sent_at is None


@pytest.mark.asyncio(loop_scope="session")
async def test_publish_with_pydantic(publish: PublishType, session: AsyncSession) -> None:
    # act
    await publish(session, "my_routing_key", Person(name="MyName"))

    # assert
    messages = (await session.execute(select(OutboxTable))).scalars().all()
    assert len(messages) == 1
    assert messages[0].routing_key == "my_routing_key"
    assert json.loads(messages[0].body) == {"name": "MyName"}
    assert messages[0].created_at is not None
    assert messages[0].sent_at is None
