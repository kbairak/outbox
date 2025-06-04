import asyncio
import json
from typing import Any, AsyncGenerator, Callable, Coroutine, Protocol
from unittest.mock import AsyncMock

import aio_pika
import pytest
import pytest_asyncio
from aio_pika.abc import AbstractConnection, DateType
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from testcontainers.rabbitmq import RabbitMqContainer

from outbox import Outbox, OutboxTable, Reject, Retry


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
async def outbox(db_engine: AsyncEngine, rmq_connection: AbstractConnection) -> Outbox:
    outbox = Outbox(db_engine=db_engine, rmq_connection=rmq_connection, clean_up_after="NEVER")
    return outbox


class EmitType(Protocol):
    async def __call__(
        self, session: AsyncSession, routing_key: str, body: Any, *, expiration: DateType = None
    ) -> None: ...


@pytest.fixture
def emit(outbox: Outbox) -> EmitType:
    return outbox.emit


class ListenType(Protocol):
    def __call__(
        self, binding_key: str, queue_name: str | None = None, retry_on_error: bool | None = None
    ) -> Callable[[Callable[..., Coroutine[None, None, None]]], None]: ...


@pytest.fixture
def listen(outbox: Outbox) -> ListenType:
    return outbox.listen


@pytest.mark.asyncio(loop_scope="session")
async def test_emit(emit: EmitType, session: AsyncSession) -> None:
    # test
    await emit(session, "test_routing_key", "test_body")

    # assert
    messages = (await session.execute(select(OutboxTable))).scalars().all()
    assert len(messages) == 1
    assert messages[0].routing_key == "test_routing_key"
    assert messages[0].body == b'"test_body"'
    assert messages[0].created_at is not None
    assert messages[0].sent_at is None


@pytest.mark.asyncio(loop_scope="session")
async def test_emit_with_pydantic(emit: EmitType, session: AsyncSession) -> None:
    # test
    await emit(session, "my_routing_key", Person(name="MyName"))

    # assert
    messages = (await session.execute(select(OutboxTable))).scalars().all()
    assert len(messages) == 1
    assert messages[0].routing_key == "my_routing_key"
    assert json.loads(messages[0].body) == {"name": "MyName"}
    assert messages[0].created_at is not None
    assert messages[0].sent_at is None


@pytest.mark.asyncio(loop_scope="session")
async def test_message_relay(
    emit: EmitType,
    outbox: Outbox,
    monkeypatch: pytest.MonkeyPatch,
    session: AsyncSession,
) -> None:
    # arrange
    rmq_connection_mock = AsyncMock(name="rmq_connection")

    async def _get_rmq_connection():
        return rmq_connection_mock

    monkeypatch.setattr(outbox, "_get_rmq_connection", _get_rmq_connection)
    rmq_connection_mock.channel.return_value = (channel_mock := AsyncMock(name="channel"))
    channel_mock.declare_exchange.return_value = (exchange_mock := AsyncMock(name="exchange"))

    await emit(session, "test_routing_key", "test_body")
    await session.commit()

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

    message = (await session.execute(select(OutboxTable))).scalars().one()
    assert message.sent_at is not None


@pytest.mark.asyncio(loop_scope="session")
async def test_register_listener(listen: ListenType, outbox: Outbox) -> None:
    # test
    @listen("test_binding_key", queue_name="test_queue_name")
    async def _(_):  # pragma: no cover
        pass

    # assert
    ((queue_name, binding_key, _),) = outbox._listeners
    assert queue_name == "test_queue_name"
    assert binding_key == "test_binding_key"


@pytest.mark.asyncio(loop_scope="session")
async def test_worker(
    listen: ListenType, emit: EmitType, session: AsyncSession, outbox: Outbox
) -> None:
    # setup
    callcount = 0
    retrieved_argument = None

    @listen("routing_key")
    async def _(person):
        nonlocal callcount, retrieved_argument
        callcount += 1
        retrieved_argument = person

    await emit(session, "routing_key", {"name": "MyName"})
    await session.commit()

    async def _message_relay():
        await asyncio.sleep(0.05)  # Give a small lead time for the worker to setup up the queue
        await outbox.message_relay()

    # test
    try:
        await asyncio.wait_for(asyncio.gather(_message_relay(), outbox.worker()), timeout=0.1)
    except asyncio.TimeoutError:
        pass

    # assert
    assert callcount == 1
    assert retrieved_argument == {"name": "MyName"}


@pytest.mark.asyncio(loop_scope="session")
async def test_worker_with_pydantic(
    listen: ListenType, emit: EmitType, session: AsyncSession, outbox: Outbox
) -> None:
    # setup
    callcount = 0
    retrieved_argument = None

    @listen("routing_key")
    async def _(person: Person):
        nonlocal callcount, retrieved_argument
        callcount += 1
        retrieved_argument = person

    await emit(session, "routing_key", Person(name="MyName"))
    await session.commit()

    async def _message_relay():
        await asyncio.sleep(0.05)  # Give a small lead time for the worker to setup up the queue
        await outbox.message_relay()

    # test
    try:
        await asyncio.wait_for(asyncio.gather(_message_relay(), outbox.worker()), timeout=0.1)
    except asyncio.TimeoutError:
        pass

    # assert
    assert callcount == 1
    assert retrieved_argument == Person(name="MyName")


@pytest.mark.asyncio(loop_scope="session")
async def test_worker_with_wildcard(
    listen: ListenType, emit: EmitType, outbox: Outbox, session: AsyncSession
) -> None:
    # setup
    callcount = 0
    retrieved_argument = None
    retrieved_routing_key = None

    @listen("routing_key.*")
    async def _(routing_key: str, person):
        nonlocal callcount, retrieved_routing_key, retrieved_argument
        callcount += 1
        retrieved_routing_key = routing_key
        retrieved_argument = person

    await emit(session, "routing_key.foo", {"name": "MyName"})
    await session.commit()

    async def _message_relay():
        await asyncio.sleep(0.05)  # Give a small lead time for the worker to setup up the queue
        await outbox.message_relay()

    # test
    try:
        await asyncio.wait_for(asyncio.gather(_message_relay(), outbox.worker()), timeout=0.1)
    except asyncio.TimeoutError:
        pass

    # assert
    assert callcount == 1
    assert retrieved_routing_key == "routing_key.foo"
    assert retrieved_argument == {"name": "MyName"}


@pytest.mark.asyncio(loop_scope="session")
async def test_retry(
    listen: ListenType, emit: EmitType, outbox: Outbox, session: AsyncSession
) -> None:
    # setup
    callcount = 0
    retrieved_argument = None

    @listen("routing_key")
    async def _(person):
        nonlocal callcount, retrieved_argument
        callcount += 1
        if callcount < 3:
            raise ValueError("Simulated failure")
        retrieved_argument = person

    await emit(session, "routing_key", {"name": "MyName"})
    await session.commit()

    async def _message_relay():
        await asyncio.sleep(0.05)  # Give a small lead time for the worker to setup up the queue
        await outbox.message_relay()

    # test
    try:
        await asyncio.wait_for(asyncio.gather(_message_relay(), outbox.worker()), timeout=0.1)
    except asyncio.TimeoutError:
        pass

    # assert
    assert callcount == 3
    assert retrieved_argument == {"name": "MyName"}


@pytest.mark.asyncio(loop_scope="session")
async def test_no_retry_with_setup(
    listen: ListenType, emit: EmitType, outbox: Outbox, session: AsyncSession
) -> None:
    # setup
    outbox.setup(retry_on_error=False)
    callcount = 0
    retrieved_argument = None

    @listen("routing_key")
    async def _(person):
        nonlocal callcount, retrieved_argument
        callcount += 1
        if callcount < 3:
            raise ValueError("Simulated failure")
        retrieved_argument = person

    await emit(session, "routing_key", {"name": "MyName"})
    await session.commit()

    async def _message_relay():
        await asyncio.sleep(0.05)  # Give a small lead time for the worker to setup up the queue
        await outbox.message_relay()

    # test
    try:
        await asyncio.wait_for(asyncio.gather(_message_relay(), outbox.worker()), timeout=0.1)
    except asyncio.TimeoutError:
        pass

    # assert
    assert callcount == 1
    assert retrieved_argument is None


@pytest.mark.asyncio(loop_scope="session")
async def test_no_retry_with_listen(
    listen: ListenType, emit: EmitType, outbox: Outbox, session: AsyncSession
) -> None:
    # setup
    callcount = 0
    retrieved_argument = None

    @listen("routing_key", retry_on_error=False)
    async def _(person):
        nonlocal callcount, retrieved_argument
        callcount += 1
        if callcount < 3:
            raise ValueError("Simulated failure")
        retrieved_argument = person

    await emit(session, "routing_key", {"name": "MyName"})
    await session.commit()

    async def _message_relay():
        await asyncio.sleep(0.05)  # Give a small lead time for the worker to setup up the queue
        await outbox.message_relay()

    # test
    try:
        await asyncio.wait_for(asyncio.gather(_message_relay(), outbox.worker()), timeout=0.1)
    except asyncio.TimeoutError:
        pass

    # assert
    assert callcount == 1
    assert retrieved_argument is None


@pytest.mark.asyncio(loop_scope="session")
async def test_force_retry_with_setup(
    listen: ListenType, emit: EmitType, session: AsyncSession, outbox: Outbox
) -> None:
    # setup
    outbox.setup(retry_on_error=False)
    callcount = 0
    retrieved_argument = None

    @listen("routing_key")
    async def _(person):
        nonlocal callcount, retrieved_argument
        callcount += 1
        if callcount < 3:
            raise Retry("Simulated failure")
        retrieved_argument = person

    await emit(session, "routing_key", {"name": "MyName"})
    await session.commit()

    async def _message_relay():
        await asyncio.sleep(0.05)  # Give a small lead time for the worker to setup up the queue
        await outbox.message_relay()

    # test
    try:
        await asyncio.wait_for(asyncio.gather(_message_relay(), outbox.worker()), timeout=0.1)
    except asyncio.TimeoutError:
        pass

    # assert
    assert callcount == 3
    assert retrieved_argument == {"name": "MyName"}


@pytest.mark.asyncio(loop_scope="session")
async def test_force_retry_with_listen(
    listen: ListenType, emit: EmitType, outbox: Outbox, session: AsyncSession
) -> None:
    # setup
    callcount = 0
    retrieved_argument = None

    @listen("routing_key", retry_on_error=False)
    async def _(person):
        nonlocal callcount, retrieved_argument
        callcount += 1
        if callcount < 3:
            raise Retry("Simulated failure")
        retrieved_argument = person

    await emit(session, "routing_key", {"name": "MyName"})
    await session.commit()

    async def _message_relay():
        await asyncio.sleep(0.05)  # Give a small lead time for the worker to setup up the queue
        await outbox.message_relay()

    # test
    try:
        await asyncio.wait_for(asyncio.gather(_message_relay(), outbox.worker()), timeout=0.1)
    except asyncio.TimeoutError:
        pass

    # assert
    assert callcount == 3
    assert retrieved_argument == {"name": "MyName"}


@pytest.mark.asyncio(loop_scope="session")
async def test_emit_and_consume_binary(
    listen: ListenType, emit: EmitType, outbox: Outbox, session: AsyncSession
) -> None:
    # setup
    callcount = 0
    retrieved_argument = None

    @listen("routing_key")
    async def _(person: bytes):
        nonlocal callcount, retrieved_argument
        callcount += 1
        retrieved_argument = person

    await emit(session, "routing_key", "hεllo".encode())
    await session.commit()

    async def _message_relay():
        await asyncio.sleep(0.05)  # Give a small lead time for the worker to setup up the queue
        await outbox.message_relay()

    # test
    try:
        await asyncio.wait_for(asyncio.gather(_message_relay(), outbox.worker()), timeout=0.1)
    except asyncio.TimeoutError:
        pass

    # assert
    assert callcount == 1
    assert retrieved_argument == "hεllo".encode()
    assert retrieved_argument is not None and len(retrieved_argument) == 6


@pytest.mark.asyncio(loop_scope="session")
async def test_dead_letter(
    listen: ListenType, emit: EmitType, session: AsyncSession, outbox: Outbox
):
    @listen("routing_key", queue_name="test_queue_name")
    async def _(person):
        raise Reject("test")

    await emit(session, "routing_key", {})
    await session.commit()

    async def _message_relay():
        await asyncio.sleep(0.05)  # Give a small lead time for the worker to setup up the queue
        await outbox.message_relay()

    # test
    try:
        await asyncio.wait_for(asyncio.gather(_message_relay(), outbox.worker()), timeout=0.1)
    except asyncio.TimeoutError:
        pass

    # get queue info
    connection = await outbox._get_rmq_connection()
    assert connection is not None
    channel = await connection.channel()
    dlq = await channel.get_queue("dlq_test_queue_name")
    assert dlq.declaration_result.message_count == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_dead_letter_with_expiration(
    listen: ListenType, emit: EmitType, session: AsyncSession, outbox: Outbox
):
    @listen("routing_key", queue_name="test_queue_name2")
    async def _(_):
        raise Retry("test")

    await emit(session, "routing_key", {}, expiration=0.01)
    await session.commit()

    async def _message_relay():
        await asyncio.sleep(0.05)  # Give a small lead time for the worker to setup up the queue
        await outbox.message_relay()

    # test
    try:
        await asyncio.wait_for(asyncio.gather(_message_relay(), outbox.worker()), timeout=0.1)
    except asyncio.TimeoutError:
        pass

    # get queue info
    connection = await outbox._get_rmq_connection()
    assert connection is not None
    channel = await connection.channel()
    dlq = await channel.get_queue("dlq_test_queue_name2")
    assert dlq.declaration_result.message_count == 1
