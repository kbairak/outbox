import asyncio
import itertools
import json
import uuid
from collections.abc import Sequence
from unittest.mock import AsyncMock

import aio_pika
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from outbox import Outbox, OutboxTable, Reject, listen

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

    async def _get_rmq_connection() -> AsyncMock:
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
        "outbox", aio_pika.ExchangeType.TOPIC, durable=True
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
    async def handler(_: object) -> None:  # pragma: no cover
        pass

    # assert
    assert handler.queue == "test_register_listener_queue"
    assert handler.binding_key == "test_register_listener_binding_key"
    assert handler._queue_obj is None
    assert handler._consumer_tag is None


@pytest.mark.asyncio(loop_scope="session")
async def test_worker(emit: EmitType, session: AsyncSession, outbox: Outbox) -> None:
    # arrange
    callcount = 0
    retrieved_argument = None

    @listen("routing_key", queue="test_worker_queue")
    async def handler(person: object) -> None:
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
    async def handler(person: Person) -> None:
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
    async def handler(routing_key: str, person: object) -> None:
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

    @listen("routing_key", queue="test_retry_queue", retry_delays=(1, 1))
    async def handler(person: object) -> None:
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
    await run_worker(outbox, [handler], timeout=3.0)

    # assert
    assert callcount == 3
    assert retrieved_argument == {"name": "MyName"}


@pytest.mark.asyncio(loop_scope="session")
async def test_no_retry_with_setup(emit: EmitType, outbox: Outbox, session: AsyncSession) -> None:
    # arrange
    outbox.setup(retry_delays=(), auto_create_table=True)
    callcount = 0
    retrieved_argument = None

    @listen("routing_key", queue="test_no_retry_with_setup_queue")
    async def handler(person: object) -> None:
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

    @listen("routing_key", retry_delays=(), queue="test_no_retry_with_listen_queue")
    async def handler(person: object) -> None:
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
async def test_no_retry_with_empty_delays_setup(
    emit: EmitType, session: AsyncSession, outbox: Outbox
) -> None:
    # arrange - empty retry_delays means no retries, go to DLQ
    outbox.setup(retry_delays=(), auto_create_table=True)
    callcount = 0

    @listen("routing_key", queue="test_no_retry_with_empty_delays_setup_queue")
    async def handler(person: object) -> None:
        nonlocal callcount
        callcount += 1
        raise Exception("Simulated failure")

    await emit(session, "routing_key", {"name": "MyName"})
    await session.commit()
    await outbox._set_up_queues([handler])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [handler], timeout=0.2)

    # assert - only 1 attempt, then goes to DLQ
    assert callcount == 1
    assert (
        await get_dlq_message_count(outbox, "test_no_retry_with_empty_delays_setup_queue")
    ) == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_no_retry_with_empty_delays_listen(
    emit: EmitType, outbox: Outbox, session: AsyncSession
) -> None:
    # arrange - empty retry_delays on listener means no retries, go to DLQ
    callcount = 0

    @listen("routing_key", retry_delays=(), queue="test_no_retry_with_empty_delays_listen_queue")
    async def handler(person: object) -> None:
        nonlocal callcount
        callcount += 1
        raise Exception("Simulated failure")

    await emit(session, "routing_key", {"name": "MyName"})
    await session.commit()
    await outbox._set_up_queues([handler])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [handler], timeout=0.2)

    # assert - only 1 attempt, then goes to DLQ
    assert callcount == 1
    assert (
        await get_dlq_message_count(outbox, "test_no_retry_with_empty_delays_listen_queue")
    ) == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_emit_and_consume_binary(
    emit: EmitType, outbox: Outbox, session: AsyncSession
) -> None:
    # arrange
    callcount = 0
    retrieved_argument = None

    @listen("routing_key", queue="test_emit_and_consume_binary_queue")
    async def handler(person: bytes) -> None:
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
async def test_dead_letter(emit: EmitType, session: AsyncSession, outbox: Outbox) -> None:
    # arrange
    @listen("routing_key", queue="test_dead_letter_queue_name")
    async def handler(person: object) -> None:
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
async def test_dead_letter_with_expiration(
    emit: EmitType, session: AsyncSession, outbox: Outbox
) -> None:
    # arrange
    @listen("routing_key", queue="test_dead_letter_with_expiration_queue_name2")
    async def handler(_: object) -> None:
        raise Exception("test")

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
async def test_graceful_shutdown(emit: EmitType, session: AsyncSession, outbox: Outbox) -> None:
    # arrange
    before, after = 0, 0

    @listen("routing_key", queue="test_graceful_shutdown_queue")
    async def handler(_: object) -> None:
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
async def test_messages_not_lost_during_graceful_shutdown(
    emit: EmitType, session: AsyncSession, outbox: Outbox
) -> None:
    # arrange
    before, after = 0, 0

    @listen("routing_key", queue="test_messages_not_lost_during_graceful_shutdown_queue")
    async def handler(_: object) -> None:
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
async def test_tracking_ids(
    monkeypatch: pytest.MonkeyPatch,
    outbox: Outbox,
    emit: EmitType,
    session: AsyncSession,
) -> None:
    # arrange
    counter = itertools.count(0)
    monkeypatch.setattr(uuid, "uuid4", lambda: next(counter))

    logs: list[tuple[str, ...]] = []

    with outbox.tracking():
        logs.append(outbox.get_tracking_ids())
        await emit(session, "r1", {})
        await session.commit()

    @listen("r1", queue="test_tracking_ids_queue_1")
    async def handler1(_: object) -> None:
        logs.append(outbox.get_tracking_ids())
        await emit(session, "r2", {})
        await emit(session, "r2", {})
        await session.commit()
        await outbox._consume_outbox_table()

    @listen("r2", queue="test_tracking_ids_queue_2")
    async def handler2(_: object) -> None:
        logs.append(outbox.get_tracking_ids())

    await outbox._set_up_queues([handler1, handler2])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [handler1, handler2], timeout=0.6)

    # assert
    assert logs == [("0",), ("0", "1"), ("0", "1", "2"), ("0", "1", "3")]


@pytest.mark.asyncio(loop_scope="session")
async def test_tracking_ids_with_parameter(
    monkeypatch: pytest.MonkeyPatch,
    outbox: Outbox,
    emit: EmitType,
    session: AsyncSession,
) -> None:
    # arrange
    counter = itertools.count(0)
    monkeypatch.setattr(uuid, "uuid4", lambda: next(counter))

    logs: list[Sequence[str]] = []

    with outbox.tracking():
        logs.append(outbox.get_tracking_ids())
        await emit(session, "r1", {})
        await session.commit()

    @listen("r1", queue="test_tracking_ids_with_parameter_queue1")
    async def handler1(_: object, tracking_ids: Sequence[str]) -> None:
        logs.append(tracking_ids)
        await emit(session, "r2", {})
        await emit(session, "r2", {})
        await session.commit()
        await outbox._consume_outbox_table()

    @listen("r2", queue="test_tracking_ids_with_parameter_queue2")
    async def handler2(_: object, tracking_ids: Sequence[str]) -> None:
        logs.append(tracking_ids)

    await outbox._set_up_queues([handler1, handler2])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [handler1, handler2], timeout=0.6)

    # assert
    assert logs == [("0",), ("0", "1"), ("0", "1", "2"), ("0", "1", "3")]


@pytest.mark.asyncio(loop_scope="session")
async def test_emit_retry_delays(emit: EmitType, session: AsyncSession, outbox: Outbox) -> None:
    # arrange - test that default retry_delays from Outbox are used
    outbox.setup(retry_delays=(0.1,) * 2, auto_create_table=True)
    await emit(session, "r1", {})
    await session.commit()

    callcount = 0

    @listen("r1", queue="test_emit_retry_delays_queue")
    async def handler(_: object) -> None:
        nonlocal callcount
        callcount += 1
        _ = 3 / 0

    await outbox._set_up_queues([handler])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [handler], timeout=1.0)

    # assert
    assert callcount == 3
    assert (await get_dlq_message_count(outbox, "test_emit_retry_delays_queue")) == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_listen_retry_delays(emit: EmitType, session: AsyncSession, outbox: Outbox) -> None:
    # arrange
    await emit(session, "r1", {})
    await session.commit()

    callcount = 0

    @listen("r1", retry_delays=(1,) * 2, queue="test_listen_retry_delays_queue")
    async def handler(_: object) -> None:
        nonlocal callcount
        callcount += 1
        _ = 3 / 0

    await outbox._set_up_queues([handler])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [handler], timeout=3.0)

    # assert
    assert callcount == 3
    assert (await get_dlq_message_count(outbox, "test_listen_retry_delays_queue")) == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_setup_retry_delays(emit: EmitType, session: AsyncSession, outbox: Outbox) -> None:
    # arrange
    outbox.setup(retry_delays=(0.1,) * 2, auto_create_table=True)

    await emit(session, "r1", {})
    await session.commit()

    callcount = 0

    @listen("r1", queue="test_setup_retry_delays_queue")
    async def handler(_: object) -> None:
        nonlocal callcount
        callcount += 1
        _ = 3 / 0

    await outbox._set_up_queues([handler])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [handler], timeout=1.0)

    # assert
    assert callcount == 3
    assert (await get_dlq_message_count(outbox, "test_setup_retry_delays_queue")) == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_wildcard_routing_key_preserved_through_retries(
    emit: EmitType, session: AsyncSession, outbox: Outbox
) -> None:
    # arrange - test that wildcard routing keys are preserved through delay exchange retries
    callcount = 0
    retrieved_routing_keys = []

    @listen("order.*", queue="test_wildcard_retry_queue", retry_delays=(1, 1))
    async def handler(routing_key: str, person: object) -> None:
        nonlocal callcount
        callcount += 1
        retrieved_routing_keys.append(routing_key)
        if callcount < 3:
            raise ValueError("Simulated failure")

    await emit(session, "order.created", {"name": "MyName"})
    await session.commit()
    await outbox._set_up_queues([handler])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [handler], timeout=3.0)

    # assert - routing key should be preserved across all retry attempts
    assert callcount == 3
    assert retrieved_routing_keys == ["order.created", "order.created", "order.created"]


@pytest.mark.asyncio(loop_scope="session")
async def test_sync_callback(emit: EmitType, session: AsyncSession, outbox: Outbox) -> None:
    """Test that sync (non-async) callbacks are supported via auto-wrapping."""
    # arrange
    callcount = 0
    retrieved_data = None

    @listen("sync.test", queue="test_sync_callback")
    def sync_handler(data: object) -> None:
        nonlocal callcount, retrieved_data
        callcount += 1
        retrieved_data = data

    await emit(session, "sync.test", {"message": "hello"})
    await session.commit()
    await outbox._set_up_queues([sync_handler])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [sync_handler], timeout=0.2)

    # assert
    assert callcount == 1
    assert retrieved_data == {"message": "hello"}


@pytest.mark.asyncio(loop_scope="session")
async def test_sync_callback_with_pydantic(
    emit: EmitType, session: AsyncSession, outbox: Outbox
) -> None:
    """Test that sync callbacks work with Pydantic models."""
    # arrange
    callcount = 0
    retrieved_person = None

    @listen("person.created", queue="test_sync_pydantic")
    def sync_handler(person: Person) -> None:
        nonlocal callcount, retrieved_person
        callcount += 1
        retrieved_person = person

    await emit(session, "person.created", Person(name="John"))
    await session.commit()
    await outbox._set_up_queues([sync_handler])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [sync_handler], timeout=0.2)

    # assert
    assert callcount == 1
    assert retrieved_person == Person(name="John")


@pytest.mark.asyncio(loop_scope="session")
async def test_sync_callback_with_special_params(
    emit: EmitType, session: AsyncSession, outbox: Outbox
) -> None:
    """Test that sync callbacks receive special parameters correctly."""
    # arrange
    retrieved_routing_key = None
    retrieved_tracking_ids = None
    retrieved_queue_name = None

    @listen("special.test", queue="test_sync_special_params")
    def sync_handler(
        data: object,
        routing_key: str,
        tracking_ids: Sequence[str],
        queue_name: str,
    ) -> None:
        nonlocal retrieved_routing_key, retrieved_tracking_ids, retrieved_queue_name
        retrieved_routing_key = routing_key
        retrieved_tracking_ids = tracking_ids
        retrieved_queue_name = queue_name

    await emit(session, "special.test", {"value": 42})
    await session.commit()
    await outbox._set_up_queues([sync_handler])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [sync_handler], timeout=0.2)

    # assert
    assert retrieved_routing_key == "special.test"
    assert retrieved_tracking_ids is not None
    assert len(retrieved_tracking_ids) == 1
    assert retrieved_queue_name == "test_sync_special_params"


@pytest.mark.asyncio(loop_scope="session")
async def test_sync_callback_exception_retry(
    emit: EmitType, session: AsyncSession, outbox: Outbox
) -> None:
    """Test that sync callbacks are retried on exception."""
    # arrange
    callcount = 0

    @listen("retry.test", queue="test_sync_retry", retry_delays=(1,))
    def sync_handler(data: object) -> None:
        nonlocal callcount
        callcount += 1
        if callcount < 2:
            raise Exception("Temporary error")

    await emit(session, "retry.test", {})
    await session.commit()
    await outbox._set_up_queues([sync_handler])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [sync_handler], timeout=2.0)

    # assert - should be called twice (initial + 1 retry)
    assert callcount == 2


@pytest.mark.asyncio(loop_scope="session")
async def test_sync_callback_reject(emit: EmitType, session: AsyncSession, outbox: Outbox) -> None:
    """Test that sync callbacks can raise Reject exception."""
    # arrange
    callcount = 0

    @listen("reject.test", queue="test_sync_reject")
    def sync_handler(data: object) -> None:
        nonlocal callcount
        callcount += 1
        raise Reject()

    await emit(session, "reject.test", {})
    await session.commit()
    await outbox._set_up_queues([sync_handler])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [sync_handler], timeout=0.2)

    # assert - should be called once and sent to DLQ
    assert callcount == 1
    assert await get_dlq_message_count(outbox, "test_sync_reject") == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_mixed_sync_async_listeners(
    emit: EmitType, session: AsyncSession, outbox: Outbox
) -> None:
    """Test that sync and async listeners can coexist in the same worker."""
    # arrange
    sync_callcount = 0
    async_callcount = 0

    @listen("mixed.sync", queue="test_mixed_sync")
    def sync_handler(data: object) -> None:
        nonlocal sync_callcount
        sync_callcount += 1

    @listen("mixed.async", queue="test_mixed_async")
    async def async_handler(data: object) -> None:
        nonlocal async_callcount
        async_callcount += 1

    await emit(session, "mixed.sync", {})
    await emit(session, "mixed.async", {})
    await session.commit()
    await outbox._set_up_queues([sync_handler, async_handler])
    await outbox._consume_outbox_table()

    # test
    await run_worker(outbox, [sync_handler, async_handler], timeout=0.2)

    # assert - both should be called
    assert sync_callcount == 1
    assert async_callcount == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_listener_direct_instantiation_with_sync(outbox: Outbox) -> None:
    """Test that Listener class works with sync callbacks when instantiated directly."""
    # arrange
    from outbox import Listener

    callcount = 0

    def sync_func(data: object) -> None:
        nonlocal callcount
        callcount += 1

    listener = Listener("test.key", sync_func, queue="test_direct_sync")

    # assert - callback should be wrapped to async
    assert asyncio.iscoroutinefunction(listener.callback)
    assert listener.queue == "test_direct_sync"


@pytest.mark.asyncio(loop_scope="session")
async def test_sync_callback_queue_name_generation(outbox: Outbox) -> None:
    """Test that queue names are auto-generated correctly for sync callbacks."""

    # arrange
    @listen("queue.test")
    def my_sync_handler(data: object) -> None:
        pass

    # assert - queue name should be generated from function module and name
    assert "my_sync_handler" in my_sync_handler.queue
    assert "test_outbox" in my_sync_handler.queue
