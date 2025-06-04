import asyncio
import json
from typing import AsyncGenerator
from unittest.mock import AsyncMock

import aio_pika
import pytest
import pytest_asyncio
from aio_pika.abc import AbstractConnection
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from testcontainers.rabbitmq import RabbitMqContainer

import outbox
from outbox import emit, listen, message_relay, setup, worker


class Person(BaseModel):
    name: str


@pytest.fixture
def db_engine() -> AsyncEngine:
    return create_async_engine("sqlite+aiosqlite://")


@pytest_asyncio.fixture(loop_scope="session")
async def session(db_engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSession(db_engine) as session:
        yield session


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def rmq_connection():
    with RabbitMqContainer("rabbitmq:4.1") as rabbitmq:
        connection = await aio_pika.connect(
            f"amqp://{rabbitmq.username}:{rabbitmq.password}@{rabbitmq.get_container_host_ip()}:"
            f"{rabbitmq.get_exposed_port(rabbitmq.port)}/"
        )
        async with connection:
            yield connection


@pytest_asyncio.fixture(loop_scope="session")
async def outbox_setup(db_engine: AsyncEngine, rmq_connection: AbstractConnection) -> None:
    outbox.outbox._reset_listeners()  # Reset listeners before setup
    await setup(db_engine=db_engine, rmq_connection=rmq_connection)


@pytest.mark.asyncio(loop_scope="session")
async def test_emit(outbox_setup: None, session: AsyncSession) -> None:
    # test
    emit(session, "test_routing_key", "test_body")

    # assert
    messages = (await session.execute(select(outbox.OutboxTable))).scalars().all()
    assert len(messages) == 1
    assert messages[0].routing_key == "test_routing_key"
    assert messages[0].body == '"test_body"'
    assert messages[0].created_at is not None
    assert messages[0].sent_at is None


@pytest.mark.asyncio(loop_scope="session")
async def test_emit_with_pydantic(outbox_setup: None, session: AsyncSession) -> None:
    # test
    emit(session, "my_routing_key", Person(name="MyName"))

    # assert
    messages = (await session.execute(select(outbox.OutboxTable))).scalars().all()
    assert len(messages) == 1
    assert messages[0].routing_key == "my_routing_key"
    assert json.loads(messages[0].body) == {"name": "MyName"}
    assert messages[0].created_at is not None
    assert messages[0].sent_at is None


@pytest.mark.asyncio(loop_scope="session")
async def test_message_relay(
    outbox_setup: None, monkeypatch: pytest.MonkeyPatch, session: AsyncSession
) -> None:
    # arrange
    monkeypatch.setattr(
        outbox.outbox, "rmq_connection", (rmq_connection_mock := AsyncMock(name="rmq_connection"))
    )
    rmq_connection_mock.channel.return_value = (channel_mock := AsyncMock(name="channel"))
    channel_mock.declare_exchange.return_value = (exchange_mock := AsyncMock(name="exchange"))

    emit(session, "test_routing_key", "test_body")
    await session.commit()

    # test
    try:
        await asyncio.wait_for(message_relay(), timeout=0.1)
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

    message = (await session.execute(select(outbox.OutboxTable))).scalars().one()
    assert message.sent_at is not None


@pytest.mark.asyncio(loop_scope="session")
async def test_regiter_listener(outbox_setup: None) -> None:
    # test
    @listen("routing_key")
    async def test_listener(person):
        pass

    # assert
    assert list(outbox.outbox.listeners.keys()) == ["routing_key"]
    assert len(list(outbox.outbox.listeners.values())[0]) == 1
    assert "test_listener" in list(outbox.outbox.listeners.values())[0][0][0]


@pytest.mark.asyncio(loop_scope="session")
async def test_worker(outbox_setup: None, session: AsyncSession) -> None:
    # setup
    callcount = 0
    retrieved_argument = None

    @listen("routing_key")
    async def test_listener(person):
        nonlocal callcount, retrieved_argument
        callcount += 1
        retrieved_argument = person

    emit(session, "routing_key", {"name": "MyName"})
    await session.commit()

    async def _message_relay():
        await asyncio.sleep(0.05)  # Give a small lead time for the worker to setup up the queue
        await message_relay()

    # test
    try:
        await asyncio.wait_for(asyncio.gather(_message_relay(), worker()), timeout=0.1)
    except asyncio.TimeoutError:
        pass

    # assert
    assert callcount == 1
    assert retrieved_argument == {"name": "MyName"}


@pytest.mark.asyncio(loop_scope="session")
async def test_worker_with_pydantic(outbox_setup: None, session: AsyncSession) -> None:
    # setup
    callcount = 0
    retrieved_argument = None

    @listen("routing_key")
    async def test_listener(person: Person):
        nonlocal callcount, retrieved_argument
        callcount += 1
        retrieved_argument = person

    emit(session, "routing_key", Person(name="MyName"))
    await session.commit()

    async def _message_relay():
        await asyncio.sleep(0.05)  # Give a small lead time for the worker to setup up the queue
        await message_relay()

    # test
    try:
        await asyncio.wait_for(asyncio.gather(_message_relay(), worker()), timeout=0.1)
    except asyncio.TimeoutError:
        pass

    # assert
    assert callcount == 1
    assert retrieved_argument == Person(name="MyName")


@pytest.mark.asyncio(loop_scope="session")
async def test_worker_with_wildcard(outbox_setup: None, session: AsyncSession) -> None:
    # setup
    callcount = 0
    retrieved_argument = None
    retrieved_routing_key = None

    @listen("routing_key.*")
    async def test_listener(routing_key: str, person):
        nonlocal callcount, retrieved_routing_key, retrieved_argument
        callcount += 1
        retrieved_routing_key = routing_key
        retrieved_argument = person

    emit(session, "routing_key.foo", {"name": "MyName"})
    await session.commit()

    async def _message_relay():
        await asyncio.sleep(0.05)  # Give a small lead time for the worker to setup up the queue
        await message_relay()

    # test
    try:
        await asyncio.wait_for(asyncio.gather(_message_relay(), worker()), timeout=0.1)
    except asyncio.TimeoutError:
        pass

    # assert
    assert callcount == 1
    assert retrieved_routing_key == "routing_key.foo"
    assert retrieved_argument == {"name": "MyName"}


@pytest.mark.asyncio(loop_scope="session")
async def test_retry(outbox_setup: None, session: AsyncSession) -> None:
    # setup
    callcount = 0
    retrieved_argument = None

    @listen("routing_key")
    async def test_listener(person):
        nonlocal callcount, retrieved_argument
        callcount += 1
        if callcount < 3:
            raise ValueError("Simulated failure")
        retrieved_argument = person

    emit(session, "routing_key", {"name": "MyName"})
    await session.commit()

    async def _message_relay():
        await asyncio.sleep(0.05)  # Give a small lead time for the worker to setup up the queue
        await message_relay()

    # test
    try:
        await asyncio.wait_for(asyncio.gather(_message_relay(), worker()), timeout=0.1)
    except asyncio.TimeoutError:
        pass

    # assert
    assert callcount == 3
    assert retrieved_argument == {"name": "MyName"}


@pytest.mark.asyncio(loop_scope="session")
async def test_no_retry_with_setup(outbox_setup: None, session: AsyncSession) -> None:
    # setup
    await setup(retry_on_error=False)
    callcount = 0
    retrieved_argument = None

    @listen("routing_key")
    async def test_listener(person):
        nonlocal callcount, retrieved_argument
        callcount += 1
        if callcount < 3:
            raise ValueError("Simulated failure")
        retrieved_argument = person

    emit(session, "routing_key", {"name": "MyName"})
    await session.commit()

    async def _message_relay():
        await asyncio.sleep(0.05)  # Give a small lead time for the worker to setup up the queue
        await message_relay()

    # test
    try:
        await asyncio.wait_for(asyncio.gather(_message_relay(), worker()), timeout=0.1)
    except asyncio.TimeoutError:
        pass

    # assert
    assert callcount == 1
    assert retrieved_argument is None


@pytest.mark.asyncio(loop_scope="session")
async def test_no_retry_with_listen(outbox_setup: None, session: AsyncSession) -> None:
    # setup
    callcount = 0
    retrieved_argument = None

    @listen("routing_key", retry_on_error=False)
    async def test_listener(person):
        nonlocal callcount, retrieved_argument
        callcount += 1
        if callcount < 3:
            raise ValueError("Simulated failure")
        retrieved_argument = person

    emit(session, "routing_key", {"name": "MyName"})
    await session.commit()

    async def _message_relay():
        await asyncio.sleep(0.05)  # Give a small lead time for the worker to setup up the queue
        await message_relay()

    # test
    try:
        await asyncio.wait_for(asyncio.gather(_message_relay(), worker()), timeout=0.1)
    except asyncio.TimeoutError:
        pass

    # assert
    assert callcount == 1
    assert retrieved_argument is None


@pytest.mark.asyncio(loop_scope="session")
async def test_force_retry_with_setup(outbox_setup: None, session: AsyncSession) -> None:
    # setup
    await setup(retry_on_error=False)
    callcount = 0
    retrieved_argument = None

    @listen("routing_key")
    async def test_listener(person):
        nonlocal callcount, retrieved_argument
        callcount += 1
        if callcount < 3:
            raise outbox.Retry("Simulated failure")
        retrieved_argument = person

    emit(session, "routing_key", {"name": "MyName"})
    await session.commit()

    async def _message_relay():
        await asyncio.sleep(0.05)  # Give a small lead time for the worker to setup up the queue
        await message_relay()

    # test
    try:
        await asyncio.wait_for(asyncio.gather(_message_relay(), worker()), timeout=0.1)
    except asyncio.TimeoutError:
        pass

    # assert
    assert callcount == 3
    assert retrieved_argument == {"name": "MyName"}


@pytest.mark.asyncio(loop_scope="session")
async def test_force_no_retry_with_setup(outbox_setup: None, session: AsyncSession) -> None:
    # setup
    callcount = 0
    retrieved_argument = None

    @listen("routing_key")
    async def test_listener(person):
        nonlocal callcount, retrieved_argument
        callcount += 1
        if callcount < 3:
            raise outbox.Abort("Simulated failure")
        retrieved_argument = person

    emit(session, "routing_key", {"name": "MyName"})
    await session.commit()

    async def _message_relay():
        await asyncio.sleep(0.05)  # Give a small lead time for the worker to setup up the queue
        await message_relay()

    # test
    try:
        await asyncio.wait_for(asyncio.gather(_message_relay(), worker()), timeout=0.1)
    except asyncio.TimeoutError:
        pass

    # assert
    assert callcount == 1
    assert retrieved_argument is None


@pytest.mark.asyncio(loop_scope="session")
async def test_force_retry_with_listen(outbox_setup: None, session: AsyncSession) -> None:
    # setup
    callcount = 0
    retrieved_argument = None

    @listen("routing_key", retry_on_error=False)
    async def test_listener(person):
        nonlocal callcount, retrieved_argument
        callcount += 1
        if callcount < 3:
            raise outbox.Retry("Simulated failure")
        retrieved_argument = person

    emit(session, "routing_key", {"name": "MyName"})
    await session.commit()

    async def _message_relay():
        await asyncio.sleep(0.05)  # Give a small lead time for the worker to setup up the queue
        await message_relay()

    # test
    try:
        await asyncio.wait_for(asyncio.gather(_message_relay(), worker()), timeout=0.1)
    except asyncio.TimeoutError:
        pass

    # assert
    assert callcount == 3
    assert retrieved_argument == {"name": "MyName"}


@pytest.mark.asyncio(loop_scope="session")
async def test_force_no_retry_with_listen(outbox_setup: None, session: AsyncSession) -> None:
    # setup
    callcount = 0
    retrieved_argument = None

    @listen("routing_key", retry_on_error=True)
    async def test_listener(person):
        nonlocal callcount, retrieved_argument
        callcount += 1
        if callcount < 3:
            raise outbox.Abort("Simulated failure")
        retrieved_argument = person

    emit(session, "routing_key", {"name": "MyName"})
    await session.commit()

    async def _message_relay():
        await asyncio.sleep(0.05)  # Give a small lead time for the worker to setup up the queue
        await message_relay()

    # test
    try:
        await asyncio.wait_for(asyncio.gather(_message_relay(), worker()), timeout=0.1)
    except asyncio.TimeoutError:
        pass

    # assert
    assert callcount == 1
    assert retrieved_argument is None
