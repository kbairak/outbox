import asyncio
import itertools
import uuid
from collections.abc import Sequence

import pytest
from aio_pika.abc import AbstractConnection
from sqlalchemy.ext.asyncio import AsyncSession

from outbox import Listener, MessageRelay, Reject, Worker, get_tracking_ids, listen, tracking

from .utils import EmitType, Person, get_dlq_message_count, run_worker


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
async def test_worker(
    emit: EmitType, session: AsyncSession, message_relay: MessageRelay, worker: Worker
) -> None:
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
    prev_listeners = worker.listeners
    worker.listeners = [handler]
    await worker._set_up_queues()
    await message_relay._consume_outbox_table()

    # test
    await run_worker(worker, [handler], timeout=0.2)

    # assert
    assert callcount == 1
    assert retrieved_argument == {"name": "MyName"}

    # reset
    worker.listeners = prev_listeners


@pytest.mark.asyncio(loop_scope="session")
async def test_worker_with_pydantic(
    emit: EmitType, session: AsyncSession, worker: Worker, message_relay: MessageRelay
) -> None:
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
    prev_listeners = worker.listeners
    worker.listeners = [handler]
    await worker._set_up_queues()
    await message_relay._consume_outbox_table()

    # test
    await run_worker(worker, [handler], timeout=0.2)

    # assert
    assert callcount == 1
    assert retrieved_argument == Person(name="MyName")

    # reset
    worker.listeners = prev_listeners


@pytest.mark.asyncio(loop_scope="session")
async def test_worker_with_wildcard(
    emit: EmitType, worker: Worker, message_relay: MessageRelay, session: AsyncSession
) -> None:
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
    prev_listeners = worker.listeners
    worker.listeners = [handler]
    await worker._set_up_queues()
    await message_relay._consume_outbox_table()

    # test
    await run_worker(worker, [handler], timeout=0.2)

    # assert
    assert callcount == 1
    assert retrieved_routing_key == "routing_key.foo"
    assert retrieved_argument == {"name": "MyName"}

    # reset
    worker.listeners = prev_listeners


@pytest.mark.asyncio(loop_scope="session")
async def test_retry(
    emit: EmitType, worker: Worker, message_relay: MessageRelay, session: AsyncSession
) -> None:
    # arrange
    callcount = 0
    retrieved_argument = None

    @listen("routing_key", queue="test_retry_queue", retry_delays=("100ms", "100ms"))
    async def handler(person: object) -> None:
        nonlocal callcount, retrieved_argument
        callcount += 1
        if callcount < 3:
            raise ValueError("Simulated failure")
        retrieved_argument = person

    await emit(session, "routing_key", {"name": "MyName"})
    await session.commit()
    prev_listeners = worker.listeners
    worker.listeners = [handler]
    await worker._set_up_queues()
    await message_relay._consume_outbox_table()

    # test
    await run_worker(worker, [handler], timeout=0.5)

    # assert
    assert callcount == 3
    assert retrieved_argument == {"name": "MyName"}

    # reset
    worker.listeners = prev_listeners


@pytest.mark.asyncio(loop_scope="session")
async def test_instant_retry(
    emit: EmitType, worker: Worker, message_relay: MessageRelay, session: AsyncSession
) -> None:
    """Test that delay=0 uses nack(requeue=True) for instant retries."""
    # arrange
    callcount = 0
    retrieved_argument = None

    @listen("routing_key", queue="test_instant_retry_queue", retry_delays=("0ms", "0ms", "100ms"))
    async def handler(person: object) -> None:
        nonlocal callcount, retrieved_argument
        callcount += 1
        if callcount < 4:
            raise ValueError("Simulated failure")
        retrieved_argument = person

    await emit(session, "routing_key", {"name": "InstantRetry"})
    await session.commit()
    prev_listeners = worker.listeners
    worker.listeners = [handler]
    await worker._set_up_queues()
    await message_relay._consume_outbox_table()

    # test - first two retries should be instant (delay=0), third has 100ms delay
    await run_worker(worker, [handler], timeout=0.4)

    # assert - should succeed after 2 instant retries + 1 delayed retry
    assert callcount == 4
    assert retrieved_argument == {"name": "InstantRetry"}

    # reset
    worker.listeners = prev_listeners


@pytest.mark.asyncio(loop_scope="session")
async def test_no_retry_with_setup(
    emit: EmitType, worker: Worker, message_relay: MessageRelay, session: AsyncSession
) -> None:
    # arrange
    prev_retry_delays = worker.retry_delays
    worker.retry_delays = ()
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
    prev_listeners = worker.listeners
    worker.listeners = [handler]
    await worker._set_up_queues()
    await message_relay._consume_outbox_table()

    # test
    await run_worker(worker, [handler], timeout=0.2)

    # assert
    assert callcount == 1
    assert retrieved_argument is None

    # reset
    worker.listeners = prev_listeners
    worker.retry_delays = prev_retry_delays


@pytest.mark.asyncio(loop_scope="session")
async def test_no_retry_with_listen(
    emit: EmitType, worker: Worker, message_relay: MessageRelay, session: AsyncSession
) -> None:
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
    prev_listeners = worker.listeners
    worker.listeners = [handler]
    await worker._set_up_queues()
    await message_relay._consume_outbox_table()

    # test
    await run_worker(worker, [handler], timeout=0.2)

    # assert
    assert callcount == 1

    # reset
    worker.listeners = prev_listeners
    assert retrieved_argument is None


@pytest.mark.asyncio(loop_scope="session")
async def test_no_retry_with_empty_delays_setup(
    emit: EmitType,
    session: AsyncSession,
    worker: Worker,
    message_relay: MessageRelay,
    rmq_connection: AbstractConnection,
) -> None:
    # arrange - empty retry_delays means no retries, go to DLQ
    prev_retry_delays = worker.retry_delays
    worker.retry_delays = ()
    callcount = 0

    @listen("routing_key", queue="test_no_retry_with_empty_delays_setup_queue")
    async def handler(_: object) -> None:
        nonlocal callcount
        callcount += 1
        raise Exception("Simulated failure")

    await emit(session, "routing_key", {"name": "MyName"})
    await session.commit()
    prev_listeners = worker.listeners
    worker.listeners = [handler]
    await worker._set_up_queues()
    await message_relay._consume_outbox_table()

    # test
    await run_worker(worker, [handler], timeout=0.2)

    # assert - only 1 attempt, then goes to DLQ
    assert callcount == 1
    assert (
        await get_dlq_message_count(rmq_connection, "test_no_retry_with_empty_delays_setup_queue")
    ) == 1

    # reset
    worker.listeners = prev_listeners
    worker.retry_delays = prev_retry_delays


@pytest.mark.asyncio(loop_scope="session")
async def test_no_retry_with_empty_delays_listen(
    emit: EmitType,
    worker: Worker,
    message_relay: MessageRelay,
    session: AsyncSession,
    rmq_connection: AbstractConnection,
) -> None:
    # arrange - empty retry_delays on listener means no retries, go to DLQ
    callcount = 0

    @listen("routing_key", retry_delays=(), queue="test_no_retry_with_empty_delays_listen_queue")
    async def handler(_: object) -> None:
        nonlocal callcount
        callcount += 1
        raise Exception("Simulated failure")

    await emit(session, "routing_key", {"name": "MyName"})
    await session.commit()
    prev_listeners = worker.listeners
    worker.listeners = [handler]
    await worker._set_up_queues()
    await message_relay._consume_outbox_table()

    # test
    await run_worker(worker, [handler], timeout=0.2)

    # assert
    assert callcount == 1
    assert (
        await get_dlq_message_count(rmq_connection, "test_no_retry_with_empty_delays_listen_queue")
    ) == 1

    # reset
    worker.listeners = prev_listeners


@pytest.mark.asyncio(loop_scope="session")
async def test_emit_and_consume_binary(
    emit: EmitType, worker: Worker, message_relay: MessageRelay, session: AsyncSession
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
    prev_listeners = worker.listeners
    worker.listeners = [handler]
    await worker._set_up_queues()
    await message_relay._consume_outbox_table()

    # test
    await run_worker(worker, [handler], timeout=0.2)

    # assert
    assert callcount == 1
    assert retrieved_argument == "hεllo".encode()
    assert retrieved_argument is not None and len(retrieved_argument) == 6

    # reset
    worker.listeners = prev_listeners


@pytest.mark.asyncio(loop_scope="session")
async def test_dead_letter(
    emit: EmitType,
    session: AsyncSession,
    worker: Worker,
    message_relay: MessageRelay,
    rmq_connection: AbstractConnection,
) -> None:
    # arrange
    @listen("routing_key", queue="test_dead_letter_queue_name")
    async def handler(_: object) -> None:
        raise Reject("test")

    await emit(session, "routing_key", {})
    await session.commit()
    prev_listeners = worker.listeners
    worker.listeners = [handler]
    await worker._set_up_queues()
    await message_relay._consume_outbox_table()

    # test
    await run_worker(worker, [handler], timeout=0.2)

    # assert
    assert (await get_dlq_message_count(rmq_connection, "test_dead_letter_queue_name")) == 1

    # reset
    worker.listeners = prev_listeners


@pytest.mark.asyncio(loop_scope="session")
async def test_dead_letter_with_expiration(
    emit: EmitType,
    session: AsyncSession,
    worker: Worker,
    message_relay: MessageRelay,
    rmq_connection: AbstractConnection,
) -> None:
    # arrange
    @listen("routing_key", queue="test_dead_letter_with_expiration_queue_name2")
    async def handler(_: object) -> None:
        raise Exception("test")

    await emit(session, "routing_key", {}, expiration=0.02)
    await session.commit()
    prev_listeners = worker.listeners
    worker.listeners = [handler]
    await worker._set_up_queues()
    await message_relay._consume_outbox_table()

    # test
    await run_worker(worker, [handler], timeout=0.2)

    # assert
    assert (
        await get_dlq_message_count(rmq_connection, "test_dead_letter_with_expiration_queue_name2")
    ) == 1

    # reset
    worker.listeners = prev_listeners


@pytest.mark.asyncio(loop_scope="session")
async def test_graceful_shutdown(
    emit: EmitType, session: AsyncSession, worker: Worker, message_relay: MessageRelay
) -> None:
    # arrange
    before, after = 0, 0

    @listen("routing_key", queue="test_graceful_shutdown_queue")
    async def handler(_: object) -> None:
        nonlocal before, after
        before += 1
        await asyncio.sleep(0.3)
        after += 1

    await emit(session, "routing_key", {})
    await session.commit()
    prev_listeners = worker.listeners
    worker.listeners = [handler]
    await worker._set_up_queues()
    await message_relay._consume_outbox_table()

    worker_task = asyncio.create_task(worker.run())

    await asyncio.sleep(0.15)  # Give some time for the task to reach `sleep`
    assert (before, after) == (1, 0)

    # test
    assert worker._shutdown_future is not None
    worker._shutdown_future.set_result(None)  # Simulate SIGINT/TERM
    await asyncio.wait((worker_task,))

    # assert
    assert (before, after) == (1, 1)

    # reset
    worker.listeners = prev_listeners


@pytest.mark.asyncio(loop_scope="session")
async def test_messages_not_lost_during_graceful_shutdown(
    emit: EmitType, session: AsyncSession, worker: Worker, message_relay: MessageRelay
) -> None:
    # arrange
    before, after = 0, 0

    @listen("routing_key", queue="test_messages_not_lost_during_graceful_shutdown_queue")
    async def handler(_: object) -> None:
        nonlocal before, after
        before += 1
        await asyncio.sleep(0.3)
        after += 1

    await emit(session, "routing_key", {})
    await session.commit()
    prev_listeners = worker.listeners
    worker.listeners = [handler]
    await worker._set_up_queues()
    await message_relay._consume_outbox_table()

    worker_task = asyncio.create_task(worker.run())

    await asyncio.sleep(0.15)  # Give some time for the task to reach `sleep`
    assert (before, after) == (1, 0)

    assert worker._shutdown_future is not None
    worker._shutdown_future.set_result(None)  # Simulate SIGINT/TERM

    # Send another message while the worker is in shutdown mode
    await asyncio.sleep(0.1)
    await emit(session, "routing_key", {})
    await session.commit()
    await message_relay._consume_outbox_table()

    await asyncio.wait((worker_task,))
    assert (before, after) == (1, 1)

    # test
    # Start the worker again
    worker_task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.2)  # Give some time for the task to reach `sleep`
    assert (before, after) == (2, 1)

    # assert
    assert worker._shutdown_future is not None
    worker._shutdown_future.set_result(None)  # Simulate SIGINT/TERM
    await asyncio.wait((worker_task,))
    assert (before, after) == (2, 2)

    # reset
    worker.listeners = prev_listeners


@pytest.mark.asyncio(loop_scope="session")
async def test_tracking_ids(
    monkeypatch: pytest.MonkeyPatch,
    emit: EmitType,
    session: AsyncSession,
    message_relay: MessageRelay,
    worker: Worker,
) -> None:
    # arrange
    counter = itertools.count(0)
    monkeypatch.setattr(uuid, "uuid4", lambda: next(counter))

    logs: list[tuple[str, ...]] = []

    with tracking():
        logs.append(get_tracking_ids())
        await emit(session, "r1", {})
        await session.commit()

    @listen("r1", queue="test_tracking_ids_queue_1")
    async def handler1(_: object) -> None:
        logs.append(get_tracking_ids())
        await emit(session, "r2", {})
        await emit(session, "r2", {})
        await session.commit()
        await message_relay._consume_outbox_table()

    @listen("r2", queue="test_tracking_ids_queue_2")
    async def handler2(_: object) -> None:
        logs.append(get_tracking_ids())

    prev_listeners = worker.listeners
    worker.listeners = [handler1, handler2]
    await worker._set_up_queues()
    await message_relay._consume_outbox_table()

    # test
    await run_worker(worker, [handler1, handler2], timeout=0.6)

    # assert
    assert logs == [("0",), ("0", "1"), ("0", "1", "2"), ("0", "1", "3")]

    # reset
    worker.listeners = prev_listeners


@pytest.mark.asyncio(loop_scope="session")
async def test_tracking_ids_with_parameter(
    monkeypatch: pytest.MonkeyPatch,
    emit: EmitType,
    session: AsyncSession,
    message_relay: MessageRelay,
    worker: Worker,
) -> None:
    # arrange
    counter = itertools.count(0)
    monkeypatch.setattr(uuid, "uuid4", lambda: next(counter))

    logs: list[Sequence[str]] = []

    with tracking():
        logs.append(get_tracking_ids())
        await emit(session, "r1", {})
        await session.commit()

    @listen("r1", queue="test_tracking_ids_with_parameter_queue1")
    async def handler1(_: object, tracking_ids: Sequence[str]) -> None:
        logs.append(tracking_ids)
        await emit(session, "r2", {})
        await emit(session, "r2", {})
        await session.commit()
        await message_relay._consume_outbox_table()

    @listen("r2", queue="test_tracking_ids_with_parameter_queue2")
    async def handler2(_: object, tracking_ids: Sequence[str]) -> None:
        logs.append(tracking_ids)

    prev_listeners = worker.listeners
    worker.listeners = [handler1, handler2]
    await worker._set_up_queues()
    await message_relay._consume_outbox_table()

    # test
    await run_worker(worker, [handler1, handler2], timeout=0.6)

    # assert
    assert logs == [("0",), ("0", "1"), ("0", "1", "2"), ("0", "1", "3")]

    # reset
    worker.listeners = prev_listeners


@pytest.mark.asyncio(loop_scope="session")
async def test_emit_retry_delays(
    emit: EmitType,
    session: AsyncSession,
    worker: Worker,
    message_relay: MessageRelay,
    rmq_connection: AbstractConnection,
) -> None:
    # arrange - test that default retry_delays from Outbox are used
    prev_retry_delays = worker.retry_delays
    worker.retry_delays = ("100ms", "100ms")
    await emit(session, "r1", {})
    await session.commit()

    callcount = 0

    @listen("r1", queue="test_emit_retry_delays_queue")
    async def handler(_: object) -> None:
        nonlocal callcount
        callcount += 1
        _ = 3 / 0

    prev_listeners = worker.listeners
    worker.listeners = [handler]
    await worker._set_up_queues()
    await message_relay._consume_outbox_table()

    # test
    await run_worker(worker, [handler], timeout=0.5)

    # assert
    assert callcount == 3
    assert (await get_dlq_message_count(rmq_connection, "test_emit_retry_delays_queue")) == 1

    # reset
    worker.listeners = prev_listeners
    worker.retry_delays = prev_retry_delays


@pytest.mark.asyncio(loop_scope="session")
async def test_listen_retry_delays(
    emit: EmitType,
    session: AsyncSession,
    worker: Worker,
    message_relay: MessageRelay,
    rmq_connection: AbstractConnection,
) -> None:
    # arrange
    await emit(session, "r1", {})
    await session.commit()

    callcount = 0

    @listen("r1", retry_delays=("100ms", "100ms"), queue="test_listen_retry_delays_queue")
    async def handler(_: object) -> None:
        nonlocal callcount
        callcount += 1
        _ = 3 / 0

    prev_listeners = worker.listeners
    worker.listeners = [handler]

    await worker._set_up_queues()
    await message_relay._consume_outbox_table()

    # test
    await run_worker(worker, [handler], timeout=0.5)

    # assert
    assert callcount == 3
    assert (await get_dlq_message_count(rmq_connection, "test_listen_retry_delays_queue")) == 1

    # reset
    worker.listeners = prev_listeners


@pytest.mark.asyncio(loop_scope="session")
async def test_setup_retry_delays(
    emit: EmitType,
    session: AsyncSession,
    worker: Worker,
    message_relay: MessageRelay,
    rmq_connection: AbstractConnection,
) -> None:
    # arrange
    prev_retry_delays = worker.retry_delays
    worker.retry_delays = ("100ms", "100ms")

    await emit(session, "r1", {})
    await session.commit()

    callcount = 0

    @listen("r1", queue="test_setup_retry_delays_queue")
    async def handler(_: object) -> None:
        nonlocal callcount
        callcount += 1
        _ = 3 / 0

    prev_listeners = worker.listeners
    worker.listeners = [handler]
    await worker._set_up_queues()
    await message_relay._consume_outbox_table()

    # test
    await run_worker(worker, [handler], timeout=0.5)

    # assert
    assert callcount == 3
    assert (await get_dlq_message_count(rmq_connection, "test_setup_retry_delays_queue")) == 1

    # reset
    worker.listeners = prev_listeners
    worker.retry_delays = prev_retry_delays


@pytest.mark.asyncio(loop_scope="session")
async def test_wildcard_routing_key_preserved_through_retries(
    emit: EmitType, session: AsyncSession, worker: Worker, message_relay: MessageRelay
) -> None:
    # arrange - test that wildcard routing keys are preserved through delay exchange retries
    callcount = 0
    retrieved_routing_keys = []

    @listen("order.*", queue="test_wildcard_retry_queue", retry_delays=("100ms", "100ms"))
    async def handler(routing_key: str, _: object) -> None:
        nonlocal callcount
        callcount += 1
        retrieved_routing_keys.append(routing_key)
        if callcount < 3:
            raise ValueError("Simulated failure")

    await emit(session, "order.created", {"name": "MyName"})
    await session.commit()
    prev_listeners = worker.listeners
    worker.listeners = [handler]
    await worker._set_up_queues()
    await message_relay._consume_outbox_table()

    # test
    await run_worker(worker, [handler], timeout=0.5)

    # assert - routing key should be preserved across all retry attempts
    assert callcount == 3
    assert retrieved_routing_keys == ["order.created", "order.created", "order.created"]

    # reset
    worker.listeners = prev_listeners


@pytest.mark.asyncio(loop_scope="session")
async def test_retry_routes_to_single_queue_only(
    emit: EmitType, session: AsyncSession, worker: Worker, message_relay: MessageRelay
) -> None:
    "Test that retries only go to the failing queue, not all queues matching the routing pattern."
    # arrange - two queues with overlapping wildcard patterns
    queue1_callcount = 0
    queue2_callcount = 0

    @listen("user.*", queue="test_retry_isolation_queue1", retry_delays=("100ms",))
    async def handler1(_: object) -> None:
        nonlocal queue1_callcount
        queue1_callcount += 1
        if queue1_callcount == 1:
            raise ValueError("Simulated failure in queue1")

    @listen("*.created", queue="test_retry_isolation_queue2", retry_delays=("100ms",))
    async def handler2(_: object) -> None:
        nonlocal queue2_callcount
        queue2_callcount += 1
        # This handler always succeeds

    await emit(session, "user.created", {"name": "TestUser"})
    await session.commit()
    prev_listeners = worker.listeners
    worker.listeners = [handler1, handler2]
    await worker._set_up_queues()
    await message_relay._consume_outbox_table()

    # test
    await run_worker(worker, [handler1, handler2], timeout=0.35)

    # assert
    # Queue1: called twice (initial + 1 retry after failure)
    assert queue1_callcount == 2
    # Queue2: called ONCE only (initial message, NO retry from queue1's failure)
    assert queue2_callcount == 1

    # reset
    worker.listeners = prev_listeners


@pytest.mark.asyncio(loop_scope="session")
async def test_sync_callback(
    emit: EmitType, session: AsyncSession, worker: Worker, message_relay: MessageRelay
) -> None:
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
    prev_listeners = worker.listeners
    worker.listeners = [sync_handler]
    await worker._set_up_queues()
    await message_relay._consume_outbox_table()

    # test
    await run_worker(worker, [sync_handler], timeout=0.2)

    # assert
    assert callcount == 1
    assert retrieved_data == {"message": "hello"}

    # reset
    worker.listeners = prev_listeners


@pytest.mark.asyncio(loop_scope="session")
async def test_sync_callback_with_pydantic(
    emit: EmitType, session: AsyncSession, worker: Worker, message_relay: MessageRelay
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
    prev_listeners = worker.listeners
    worker.listeners = [sync_handler]
    await worker._set_up_queues()
    await message_relay._consume_outbox_table()

    # test
    await run_worker(worker, [sync_handler], timeout=0.2)

    # assert
    assert callcount == 1
    assert retrieved_person == Person(name="John")

    # reset
    worker.listeners = prev_listeners


@pytest.mark.asyncio(loop_scope="session")
async def test_sync_callback_with_special_params(
    emit: EmitType, session: AsyncSession, worker: Worker, message_relay: MessageRelay
) -> None:
    """Test that sync callbacks receive special parameters correctly."""
    # arrange
    retrieved_routing_key = None
    retrieved_tracking_ids = None
    retrieved_queue_name = None

    @listen("special.test", queue="test_sync_special_params")
    def sync_handler(
        _: object,
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
    prev_listeners = worker.listeners
    worker.listeners = [sync_handler]
    await worker._set_up_queues()
    await message_relay._consume_outbox_table()

    # test
    await run_worker(worker, [sync_handler], timeout=0.2)

    # assert
    assert retrieved_routing_key == "special.test"
    assert retrieved_tracking_ids is not None
    assert len(retrieved_tracking_ids) == 1
    assert retrieved_queue_name == "test_sync_special_params"

    # reset
    worker.listeners = prev_listeners


@pytest.mark.asyncio(loop_scope="session")
async def test_sync_callback_exception_retry(
    emit: EmitType,
    session: AsyncSession,
    worker: Worker,
    message_relay: MessageRelay,
) -> None:
    """Test that sync callbacks are retried on exception."""
    # arrange
    callcount = 0

    @listen("retry.test", queue="test_sync_retry", retry_delays=("100ms",))
    def sync_handler(_: object) -> None:
        nonlocal callcount
        callcount += 1
        if callcount < 2:
            raise Exception("Temporary error")

    await emit(session, "retry.test", {})
    await session.commit()
    prev_listeners = worker.listeners
    worker.listeners = [sync_handler]
    await worker._set_up_queues()
    await message_relay._consume_outbox_table()

    # test
    await run_worker(worker, [sync_handler], timeout=0.35)

    # assert - should be called twice (initial + 1 retry)
    assert callcount == 2

    # reset
    worker.listeners = prev_listeners


@pytest.mark.asyncio(loop_scope="session")
async def test_sync_callback_reject(
    emit: EmitType,
    session: AsyncSession,
    worker: Worker,
    message_relay: MessageRelay,
    rmq_connection: AbstractConnection,
) -> None:
    """Test that sync callbacks can raise Reject exception."""
    # arrange
    callcount = 0

    @listen("reject.test", queue="test_sync_reject")
    def sync_handler(_: object) -> None:
        nonlocal callcount
        callcount += 1
        raise Reject()

    await emit(session, "reject.test", {})
    await session.commit()
    prev_listeners = worker.listeners
    worker.listeners = [sync_handler]
    await worker._set_up_queues()
    await message_relay._consume_outbox_table()

    # test
    await run_worker(worker, [sync_handler], timeout=0.2)

    # assert
    assert callcount == 1
    assert await get_dlq_message_count(rmq_connection, "test_sync_reject") == 1

    # reset
    worker.listeners = prev_listeners


@pytest.mark.asyncio(loop_scope="session")
async def test_mixed_sync_async_listeners(
    emit: EmitType, session: AsyncSession, worker: Worker, message_relay: MessageRelay
) -> None:
    """Test that sync and async listeners can coexist in the same worker."""
    # arrange
    sync_callcount = 0
    async_callcount = 0

    @listen("mixed.sync", queue="test_mixed_sync")
    def sync_handler(_: object) -> None:
        nonlocal sync_callcount
        sync_callcount += 1

    @listen("mixed.async", queue="test_mixed_async")
    async def async_handler(_: object) -> None:
        nonlocal async_callcount
        async_callcount += 1

    await emit(session, "mixed.sync", {})
    await emit(session, "mixed.async", {})
    await session.commit()
    prev_listeners = worker.listeners
    worker.listeners = [sync_handler, async_handler]
    await worker._set_up_queues()
    await message_relay._consume_outbox_table()

    # test
    await run_worker(worker, [sync_handler, async_handler], timeout=0.2)

    # assert - both should be called
    assert sync_callcount == 1
    assert async_callcount == 1

    # reset
    worker.listeners = prev_listeners


@pytest.mark.asyncio(loop_scope="session")
async def test_listener_direct_instantiation_with_sync() -> None:
    """Test that Listener class works with sync callbacks when instantiated directly."""

    # arrange
    def sync_func(_: object) -> None:
        pass

    listener = Listener("test.key", sync_func, queue="test_direct_sync")

    # assert - callback should be wrapped to async
    assert asyncio.iscoroutinefunction(listener.callback)
    assert listener.queue == "test_direct_sync"


@pytest.mark.asyncio(loop_scope="session")
async def test_sync_callback_queue_name_generation() -> None:
    """Test that queue names are auto-generated correctly for sync callbacks."""

    # arrange
    @listen("queue.test")
    def my_sync_handler(_: object) -> None:
        pass

    # assert
    assert "my_sync_handler" in my_sync_handler.queue
    assert "test_worker" in my_sync_handler.queue
