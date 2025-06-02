import asyncio
import json
from typing import AsyncGenerator, Protocol
from unittest.mock import AsyncMock

import aio_pika
import pytest
import pytest_asyncio
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

from outbox import Outbox, OutboxTable


class EmitType(Protocol):
    async def __call__(
        self, session: AsyncSession, routing_key: str, body: str, *, commit: bool = False
    ) -> None: ...


@pytest.fixture
def db_engine() -> AsyncEngine:
    return create_async_engine("sqlite+aiosqlite://")


@pytest_asyncio.fixture
async def session(db_engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSession(db_engine) as session:
        yield session


@pytest_asyncio.fixture
async def outbox(db_engine: AsyncEngine) -> Outbox:
    outbox = Outbox()
    await outbox.setup_async(db_engine=db_engine)
    return outbox


@pytest_asyncio.fixture
async def emit(outbox: Outbox) -> EmitType:
    return outbox.emit


@pytest.mark.asyncio
async def test_emit(session: AsyncSession, emit: EmitType):
    # test
    await emit(session, "test_routing_key", "test_body")

    # assert
    stmt = select(OutboxTable)
    messages = (await session.execute(stmt)).scalars().all()
    assert len(messages) == 1
    assert messages[0].routing_key == "test_routing_key"
    assert messages[0].body == '"test_body"'
    assert messages[0].created_at is not None
    assert messages[0].sent_at is None


@pytest.mark.asyncio
async def test_worker(
    monkeypatch: pytest.MonkeyPatch, outbox: Outbox, emit: EmitType, session: AsyncSession
):
    # arrange
    monkeypatch.setattr(
        outbox, "rmq_connection", (rmq_connection_mock := AsyncMock(name="rmq_connection"))
    )
    rmq_connection_mock.channel.return_value = (channel_mock := AsyncMock(name="channel"))
    channel_mock.declare_exchange.return_value = (exchange_mock := AsyncMock(name="exchange"))

    await emit(session, "test_routing_key", "test_body", commit=True)

    # test
    try:
        await asyncio.wait_for(outbox.message_relay(), timeout=0.1)
    except asyncio.TimeoutError:
        pass

    # assert
    rmq_connection_mock.channel.assert_called_once_with()
    channel_mock.declare_exchange.assert_called_once_with(
        "outbox_exchange", aio_pika.ExchangeType.TOPIC, durable=True
    )
    assert exchange_mock.publish.call_count == 1
    actual_message = exchange_mock.publish.mock_calls[0].args[0]
    assert actual_message.body == b'"test_body"'
    assert actual_message.body_size == 11
    assert actual_message.content_type == "application/json"


class MyModel(BaseModel):
    name: str


@pytest.mark.asyncio
async def test_emit_with_pydantic(emit, session):
    await emit(session, "my_routing_key", MyModel(name="MyName"))
    stmt = select(OutboxTable)
    message = (await session.execute(stmt)).scalars().one()
    assert message.routing_key == "my_routing_key"
    assert json.loads(message.body) == {"name": "MyName"}
    assert message.created_at is not None
    assert message.sent_at is None
