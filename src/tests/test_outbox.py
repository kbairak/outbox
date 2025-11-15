import asyncio
import itertools
import json
import uuid
from unittest.mock import AsyncMock

import aio_pika
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from outbox import Outbox, OutboxTable, Reject, Retry, listen

from .utils import EmitType, Person, get_dlq_message_count, run_worker


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
    await outbox._consume_outbox_table()

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
async def test_register_listener() -> None:
    # test
    @listen("test_register_listener_binding_key", queue="test_register_listener_queue")
    async def handler(_):  # pragma: no cover
        pass

    # assert
    assert handler.queue == "test_register_listener_queue"
    assert handler.binding_key == "test_register_listener_binding_key"
    assert handler.queue_obj is None
    assert handler.consumer_tag is None


@pytest.mark.asyncio(loop_scope="session")
async def test_worker(emit: EmitType, session: AsyncSession, outbox: Outbox) -> None:
    # arrange
    callcount = 0
    retrieved_argument = None

    @listen("routing_key", queue="test_worker_queue")
    async def handler(person):
        nonlocal callcount, retrieved_argument
        callcount += 1
        retrieved_argument = person

    await emit(session, "routing_key", {"name": "MyName"})
    await session.commit()
    await outbox._set_up_queues([handler])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [handler], timeout=0.2)

    # assert
    assert callcount == 1
    assert retrieved_argument == {"name": "MyName"}


@pytest.mark.asyncio(loop_scope="session")
async def test_worker_with_pydantic(emit: EmitType, session: AsyncSession, outbox: Outbox) -> None:
    # arrange
    callcount = 0
    retrieved_argument = None

    @listen("routing_key", queue="test_worker_with_pydantic_queue")
    async def handler(person: Person):
        nonlocal callcount, retrieved_argument
        callcount += 1
        retrieved_argument = person

    await emit(session, "routing_key", Person(name="MyName"))
    await session.commit()
    await outbox._set_up_queues([handler])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [handler], timeout=0.2)

    # assert
    assert callcount == 1
    assert retrieved_argument == Person(name="MyName")


@pytest.mark.asyncio(loop_scope="session")
async def test_worker_with_wildcard(emit: EmitType, outbox: Outbox, session: AsyncSession) -> None:
    # arrange
    callcount = 0
    retrieved_routing_key = None
    retrieved_argument = None

    @listen("routing_key.*", queue="test_worker_with_wildcard_queue")
    async def handler(routing_key: str, person):
        nonlocal callcount, retrieved_routing_key, retrieved_argument
        callcount += 1
        retrieved_routing_key = routing_key
        retrieved_argument = person

    await emit(session, "routing_key.foo", {"name": "MyName"})
    await session.commit()
    await outbox._set_up_queues([handler])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [handler], timeout=0.2)

    # assert
    assert callcount == 1
    assert retrieved_routing_key == "routing_key.foo"
    assert retrieved_argument == {"name": "MyName"}


@pytest.mark.asyncio(loop_scope="session")
async def test_retry(emit: EmitType, outbox: Outbox, session: AsyncSession) -> None:
    # arrange
    callcount = 0
    retrieved_argument = None

    @listen("routing_key", queue="test_retry_queue")
    async def handler(person):
        nonlocal callcount, retrieved_argument
        callcount += 1
        if callcount < 3:
            raise ValueError("Simulated failure")
        retrieved_argument = person

    await emit(session, "routing_key", {"name": "MyName"})
    await session.commit()
    await outbox._set_up_queues([handler])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [handler], timeout=0.2)

    # assert
    assert callcount == 3
    assert retrieved_argument == {"name": "MyName"}


@pytest.mark.asyncio(loop_scope="session")
async def test_no_retry_with_setup(emit: EmitType, outbox: Outbox, session: AsyncSession) -> None:
    # arrange
    outbox.setup(retry_on_error=False)
    callcount = 0
    retrieved_argument = None

    @listen("routing_key", queue="test_no_retry_with_setup_queue")
    async def handler(person):
        nonlocal callcount, retrieved_argument
        callcount += 1
        if callcount < 3:
            raise ValueError("Simulated failure")
        retrieved_argument = person

    await emit(session, "routing_key", {"name": "MyName"})
    await session.commit()
    await outbox._set_up_queues([handler])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [handler], timeout=0.2)

    # assert
    assert callcount == 1
    assert retrieved_argument is None


@pytest.mark.asyncio(loop_scope="session")
async def test_no_retry_with_listen(emit: EmitType, outbox: Outbox, session: AsyncSession) -> None:
    # arrange
    callcount = 0
    retrieved_argument = None

    @listen("routing_key", retry_on_error=False, queue="test_no_retry_with_listen_queue")
    async def handler(person):
        nonlocal callcount, retrieved_argument
        callcount += 1
        if callcount < 3:
            raise ValueError("Simulated failure")
        retrieved_argument = person

    await emit(session, "routing_key", {"name": "MyName"})
    await session.commit()
    await outbox._set_up_queues([handler])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [handler], timeout=0.2)

    # assert
    assert callcount == 1
    assert retrieved_argument is None


@pytest.mark.asyncio(loop_scope="session")
async def test_force_retry_with_setup(
    emit: EmitType, session: AsyncSession, outbox: Outbox
) -> None:
    # arrange
    outbox.setup(retry_on_error=False)
    callcount = 0
    retrieved_argument = None

    @listen("routing_key", queue="test_force_retry_with_setup_queue")
    async def handler(person):
        nonlocal callcount, retrieved_argument
        callcount += 1
        if callcount < 3:
            raise Retry("Simulated failure")
        retrieved_argument = person

    await emit(session, "routing_key", {"name": "MyName"})
    await session.commit()
    await outbox._set_up_queues([handler])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [handler], timeout=0.2)

    # assert
    assert callcount == 3
    assert retrieved_argument == {"name": "MyName"}


@pytest.mark.asyncio(loop_scope="session")
async def test_force_retry_with_listen(
    emit: EmitType, outbox: Outbox, session: AsyncSession
) -> None:
    # arrange
    callcount = 0
    retrieved_argument = None

    @listen("routing_key", retry_on_error=False, queue="test_force_retry_with_listen_queue")
    async def handler(person):
        nonlocal callcount, retrieved_argument
        callcount += 1
        if callcount < 3:
            raise Retry("Simulated failure")
        retrieved_argument = person

    await emit(session, "routing_key", {"name": "MyName"})
    await session.commit()
    await outbox._set_up_queues([handler])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [handler], timeout=0.2)

    # assert
    assert callcount == 3
    assert retrieved_argument == {"name": "MyName"}


@pytest.mark.asyncio(loop_scope="session")
async def test_emit_and_consume_binary(
    emit: EmitType, outbox: Outbox, session: AsyncSession
) -> None:
    # arrange
    callcount = 0
    retrieved_argument = None

    @listen("routing_key", queue="test_emit_and_consume_binary_queue")
    async def handler(person: bytes):
        nonlocal callcount, retrieved_argument
        callcount += 1
        retrieved_argument = person

    await emit(session, "routing_key", "hεllo".encode())
    await session.commit()
    await outbox._set_up_queues([handler])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [handler], timeout=0.2)

    # assert
    assert callcount == 1
    assert retrieved_argument == "hεllo".encode()
    assert retrieved_argument is not None and len(retrieved_argument) == 6


@pytest.mark.asyncio(loop_scope="session")
async def test_dead_letter(emit: EmitType, session: AsyncSession, outbox: Outbox):
    # arrange
    @listen("routing_key", queue="test_dead_letter_queue_name")
    async def handler(person):
        raise Reject("test")

    await emit(session, "routing_key", {})
    await session.commit()
    await outbox._set_up_queues([handler])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [handler], timeout=0.2)

    # assert
    assert (await get_dlq_message_count(outbox, "test_dead_letter_queue_name")) == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_dead_letter_with_expiration(emit: EmitType, session: AsyncSession, outbox: Outbox):
    # arrange
    @listen("routing_key", queue="test_dead_letter_with_expiration_queue_name2")
    async def handler(_):
        raise Retry("test")

    await emit(session, "routing_key", {}, expiration=0.02)
    await session.commit()
    await outbox._set_up_queues([handler])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [handler], timeout=0.2)

    # assert
    assert (
        await get_dlq_message_count(outbox, "test_dead_letter_with_expiration_queue_name2")
    ) == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_graceful_shutdown(emit, session, outbox: Outbox):
    # arrange
    before, after = 0, 0

    @listen("routing_key", queue="test_graceful_shutdown_queue")
    async def handler(_):
        nonlocal before, after
        before += 1
        await asyncio.sleep(0.6)
        after += 1

    await emit(session, "routing_key", {})
    await session.commit()
    await outbox._set_up_queues([handler])
    await outbox._consume_outbox_table()

    worker_task = asyncio.create_task(outbox.worker([handler]))

    await asyncio.sleep(0.2)  # Give some time for the task to reach `sleep`
    assert (before, after) == (1, 0)

    # test
    assert outbox._shutdown_future is not None
    outbox._shutdown_future.set_result(None)  # Simulate SIGINT/TERM
    await asyncio.wait((worker_task,))

    # assert
    assert (before, after) == (1, 1)


@pytest.mark.asyncio(loop_scope="session")
async def test_messages_not_lost_during_graceful_shutdown(emit, session, outbox: Outbox):
    # arrange
    before, after = 0, 0

    @listen("routing_key", queue="test_messages_not_lost_during_graceful_shutdown_queue")
    async def handler(_):
        nonlocal before, after
        before += 1
        await asyncio.sleep(0.6)
        after += 1

    await emit(session, "routing_key", {})
    await session.commit()
    await outbox._set_up_queues([handler])
    await outbox._consume_outbox_table()

    worker_task = asyncio.create_task(outbox.worker([handler]))

    await asyncio.sleep(0.2)  # Give some time for the task to reach `sleep`
    assert (before, after) == (1, 0)

    assert outbox._shutdown_future is not None
    outbox._shutdown_future.set_result(None)  # Simulate SIGINT/TERM

    # Send another message while the worker is in shutdown mode
    await asyncio.sleep(0.2)
    await emit(session, "routing_key", {})
    await session.commit()
    await outbox._consume_outbox_table()

    await asyncio.wait((worker_task,))
    assert (before, after) == (1, 1)

    # test
    # Start the worker again
    worker_task = asyncio.create_task(outbox.worker([handler]))
    await asyncio.sleep(0.4)  # Give some time for the task to reach `sleep`
    assert (before, after) == (2, 1)

    # assert
    assert outbox._shutdown_future is not None
    outbox._shutdown_future.set_result(None)  # Simulate SIGINT/TERM
    await asyncio.wait((worker_task,))
    assert (before, after) == (2, 2)


@pytest.mark.asyncio(loop_scope="session")
async def test_track_ids(
    monkeypatch: pytest.MonkeyPatch,
    outbox: Outbox,
    emit: EmitType,
    session: AsyncSession,
):
    # arrange
    counter = itertools.count(0)
    monkeypatch.setattr(uuid, "uuid4", lambda: next(counter))

    logs = []

    with outbox.tracking():
        logs.append(outbox.get_track_ids())
        await emit(session, "r1", {})
        await session.commit()

    @listen("r1", queue="test_track_ids_queue_1")
    async def handler1(_) -> None:
        logs.append(outbox.get_track_ids())
        await emit(session, "r2", {})
        await emit(session, "r2", {})
        await session.commit()
        await outbox._consume_outbox_table()

    @listen("r2", queue="test_track_ids_queue_2")
    async def handler2(_) -> None:
        logs.append(outbox.get_track_ids())

    await outbox._set_up_queues([handler1, handler2])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [handler1, handler2], timeout=0.6)

    # assert
    assert logs == [("0",), ("0", "1"), ("0", "1", "2"), ("0", "1", "3")]


@pytest.mark.asyncio(loop_scope="session")
async def test_track_ids_with_parameter(
    monkeypatch: pytest.MonkeyPatch,
    outbox: Outbox,
    emit: EmitType,
    session: AsyncSession,
):
    # arrange
    counter = itertools.count(0)
    monkeypatch.setattr(uuid, "uuid4", lambda: next(counter))

    logs = []

    with outbox.tracking():
        logs.append(outbox.get_track_ids())
        await emit(session, "r1", {})
        await session.commit()

    @listen("r1", queue="test_track_ids_with_parameter_queue1")
    async def handler1(_, track_ids: list[str]) -> None:
        logs.append(track_ids)
        await emit(session, "r2", {})
        await emit(session, "r2", {})
        await session.commit()
        await outbox._consume_outbox_table()

    @listen("r2", queue="test_track_ids_with_parameter_queue2")
    async def handler2(_, track_ids: list[str]) -> None:
        logs.append(track_ids)

    await outbox._set_up_queues([handler1, handler2])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [handler1, handler2], timeout=0.6)

    # assert
    assert logs == [("0",), ("0", "1"), ("0", "1", "2"), ("0", "1", "3")]


@pytest.mark.asyncio(loop_scope="session")
async def test_emit_retry_limit(emit: EmitType, session: AsyncSession, outbox: Outbox):
    # arrange
    await emit(session, "r1", {}, retry_limit=3)
    await session.commit()

    callcount = 0

    @listen("r1", queue="test_emit_retry_limit_queue")
    async def handler(_):
        nonlocal callcount
        callcount += 1
        _ = 3 / 0

    await outbox._set_up_queues([handler])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [handler], timeout=0.4)

    # assert
    assert callcount == 3
    assert (await get_dlq_message_count(outbox, "test_emit_retry_limit_queue")) == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_listen_retry_limit(emit: EmitType, session: AsyncSession, outbox: Outbox):
    # arrange
    await emit(session, "r1", {})
    await session.commit()

    callcount = 0

    @listen("r1", retry_limit=3, queue="test_listen_retry_limit_queue")
    async def handler(_):
        nonlocal callcount
        callcount += 1
        _ = 3 / 0

    await outbox._set_up_queues([handler])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [handler], timeout=0.4)

    # assert
    assert callcount == 3
    assert (await get_dlq_message_count(outbox, "test_listen_retry_limit_queue")) == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_setup_retry_limit(emit: EmitType, session: AsyncSession, outbox: Outbox):
    # arrange
    outbox.setup(retry_limit=3)

    await emit(session, "r1", {})
    await session.commit()

    callcount = 0

    @listen("r1", queue="test_setup_retry_limit_queue")
    async def handler(_):
        nonlocal callcount
        callcount += 1
        _ = 3 / 0

    await outbox._set_up_queues([handler])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [handler], timeout=0.4)

    # assert
    assert callcount == 3
    assert (await get_dlq_message_count(outbox, "test_setup_retry_limit_queue")) == 1
