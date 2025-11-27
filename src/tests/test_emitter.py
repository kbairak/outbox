import json

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from outbox import Emitter, OutboxMessage
from outbox.database import OutboxTable

from .utils import EmitType, Person


@pytest.mark.asyncio(loop_scope="session")
async def test_emit(emit: EmitType, session: AsyncSession) -> None:
    # act
    await emit(session, "test_routing_key", "test_body")

    # assert
    messages = (await session.execute(select(OutboxTable))).scalars().all()
    assert len(messages) == 1
    assert messages[0].routing_key == "test_routing_key"
    assert messages[0].body == b'"test_body"'
    assert messages[0].created_at is not None
    assert messages[0].sent_at is None


@pytest.mark.asyncio(loop_scope="session")
async def test_bulk_emit(emitter: Emitter, session: AsyncSession) -> None:
    # act
    messages = [
        OutboxMessage(routing_key="test_key_1", body="test_body_1"),
        OutboxMessage(routing_key="test_key_2", body="test_body_2"),
    ]
    await emitter.bulk_emit(session, messages)
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
async def test_emit_with_pydantic(emit: EmitType, session: AsyncSession) -> None:
    # act
    await emit(session, "my_routing_key", Person(name="MyName"))

    # assert
    messages = (await session.execute(select(OutboxTable))).scalars().all()
    assert len(messages) == 1
    assert messages[0].routing_key == "my_routing_key"
    assert json.loads(messages[0].body) == {"name": "MyName"}
    assert messages[0].created_at is not None
    assert messages[0].sent_at is None
