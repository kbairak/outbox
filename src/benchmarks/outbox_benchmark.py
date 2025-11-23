"""Outbox benchmark - supports single-process and multi-process workers."""

import argparse
import asyncio
import multiprocessing
import time
from typing import Generator, Optional, Union, cast

import aio_pika
from sqlalchemy.ext.asyncio import AsyncSession
from testcontainers.postgres import PostgresContainer  # type: ignore[import-untyped]
from testcontainers.rabbitmq import RabbitMqContainer  # type: ignore[import-untyped]

from outbox import Listener, OutboxMessage, bulk_emit, message_relay, outbox, setup, worker


def _generate_batch_sizes(
    rate: Union[int, float], duration: Union[int, float]
) -> Generator[int, None, None]:
    start = time.perf_counter()
    yield (sent := 1)
    while (now := time.perf_counter()) - start <= duration:
        target = int((now - start) * rate)
        if target - sent > 0:
            yield target - sent
            sent = target
    if (remaining := int(rate * duration) - sent) > 0:
        yield remaining


def message_relay_process(postgres_url: str, rabbitmq_url: str, batch_size: int) -> None:
    setup(
        db_engine_url=postgres_url,
        rmq_connection_url=rabbitmq_url,
        auto_create_table=False,  # Already created by main process
        enable_metrics=False,
        batch_size=batch_size,
    )
    asyncio.run(message_relay())


def worker_process(
    postgres_url: str, rabbitmq_url: str, prefetch_count: int, timestamp_list: list[float]
) -> None:
    """Run a worker process that consumes messages."""
    # Track metrics locally in this process
    first_message_time: Optional[float] = None
    last_message_time: Optional[float] = None

    async def on_message(_: str) -> None:
        """Handler that tracks latency."""
        nonlocal first_message_time, last_message_time

        now = time.perf_counter()
        if first_message_time is None:
            first_message_time = now
        last_message_time = now

    setup(
        db_engine_url=postgres_url,
        rmq_connection_url=rabbitmq_url,
        auto_create_table=False,  # Already created by main process
        enable_metrics=False,
        prefetch_count=prefetch_count,
    )
    try:
        asyncio.run(worker([Listener("benchmark.test", on_message, "benchmark.test")]))
    finally:
        timestamp_list.extend([cast(float, first_message_time), cast(float, last_message_time)])


async def benchmark(
    message_rate: int,
    batch_size: int,
    prefetch_count: int,
    message_size: int,
    worker_count: int,
    relay_count: int,
) -> None:
    """Run benchmark with configurable parameters."""
    duration = 5.0
    message_count = int(message_rate * duration)

    print("Starting containers...")
    with (
        PostgresContainer("postgres:17.5-alpine") as postgres,
        RabbitMqContainer("rabbitmq:4.1-management") as rabbitmq,
    ):
        postgres_url = postgres.get_connection_url().replace("psycopg2", "asyncpg")
        rabbitmq_url = (
            f"amqp://{rabbitmq.username}:{rabbitmq.password}@{rabbitmq.get_container_host_ip()}:"
            f"{rabbitmq.get_exposed_port(rabbitmq.port)}/"
        )
        print(f"PostgreSQL: {postgres_url}")
        print(f"RabbitMQ: {rabbitmq_url}")

        # Setup in main process (creates table)
        setup(
            db_engine_url=postgres_url,
            rmq_connection_url=rabbitmq_url,
            auto_create_table=True,
            enable_metrics=False,
            batch_size=batch_size,
        )
        await outbox._ensure_database()

        # Start message relay processes
        print(f"Starting {relay_count} message relay process(es)...")
        relay_processes = []
        for _ in range(relay_count):
            relay_processes.append(
                multiprocessing.Process(
                    target=message_relay_process,
                    args=(postgres_url, rabbitmq_url, batch_size),
                )
            )
            relay_processes[-1].start()

        print(f"Starting {worker_count} worker process(es)...")
        worker_processes = []
        timestamp_list = multiprocessing.Manager().list()

        for _ in range(worker_count):
            worker_processes.append(
                multiprocessing.Process(
                    target=worker_process,
                    args=(postgres_url, rabbitmq_url, prefetch_count, timestamp_list),
                )
            )
            worker_processes[-1].start()

        # Give workers and relay time to initialize
        await asyncio.sleep(2)

        # Emit messages at controlled rate
        print(
            f"Emitting {message_count:,} messages at {message_rate:,} msgs/sec for {duration} "
            "seconds..."
        )
        emit_start_time = time.perf_counter()
        async with AsyncSession(outbox.db_engine) as session:
            message = OutboxMessage(routing_key="benchmark.test", body="*" * message_size)
            for emit_batch_size in _generate_batch_sizes(message_rate, duration):
                await bulk_emit(
                    session,
                    [message for _ in range(emit_batch_size)],
                )
                await session.commit()

        emit_duration = time.perf_counter() - emit_start_time
        actual_rate = message_count / emit_duration if emit_duration > 0 else 0
        print(
            f"Emitted {message_count:,} messages in {emit_duration:.2f}s "
            f"(target: {message_rate:,} msgs/sec, actual: {actual_rate:.0f} msgs/sec)"
        )

        # Wait for processing to complete by checking the queue
        print("Waiting for messages to be processed...")
        timeout = 60
        wait_start_time = time.perf_counter()

        # Connect to RabbitMQ to check queue status
        connection = await aio_pika.connect_robust(rabbitmq_url)
        channel = await connection.channel()

        while True:
            try:
                queue = await channel.get_queue("benchmark.test")
                messages_in_queue = queue.declaration_result.message_count

                if messages_in_queue == 0:
                    # Give a bit more time to ensure all in-flight messages are processed
                    await asyncio.sleep(1)
                    break
            except Exception:
                # Queue might not exist yet, wait and retry
                await asyncio.sleep(0.2)
                continue

            if time.perf_counter() - wait_start_time > timeout:
                print(f"Timeout! Still {messages_in_queue} messages in queue")
                break

            await asyncio.sleep(0.2)

        await connection.close()

        print("Terminating relay processes...")
        for p in relay_processes:
            p.terminate()

        print("Terminating worker processes...")
        for p in worker_processes:
            p.terminate()
        for p in worker_processes:
            p.join()

        throughput = message_count / (max(timestamp_list) - min(timestamp_list))

        # Display results
        print("\n" + "=" * 80)
        print("BENCHMARK RESULTS")
        print("=" * 80)
        print(f"Workers:           {worker_count}")
        print(f"Relays:            {relay_count}")
        print(f"Target Rate:       {message_rate:,} msgs/sec")
        print(f"Batch Size:        {batch_size}")
        print(f"Prefetch Count:    {prefetch_count}")
        print(f"Message Size:      {message_size} bytes")
        print(f"Emit duration:     {duration} seconds")
        print(f"Messages Emitted:  {message_count:,}")
        print(f"Processing Rate:   {throughput:,.0f} msgs/sec")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run outbox benchmark - emits messages at specified rate for 5 seconds"
    )
    parser.add_argument(
        "--message-rate",
        type=int,
        default=1000,
        help="Message emission rate in msgs/sec (default: 1000)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Relay batch size for pulling messages from DB (default: 50)",
    )
    parser.add_argument(
        "--prefetch-count",
        type=int,
        default=10,
        help="RabbitMQ prefetch count for workers (default: 10)",
    )
    parser.add_argument(
        "--message-size",
        type=int,
        default=256,
        help="Message payload size in bytes (default: 256)",
    )
    parser.add_argument(
        "--worker-count",
        type=int,
        default=1,
        help="Number of worker processes (default: 1, use >1 for multiprocessing)",
    )
    parser.add_argument(
        "--relay-count",
        type=int,
        default=1,
        help="Number of relay tasks (default: 1, use >1 to test relay scaling)",
    )

    args = parser.parse_args()

    asyncio.run(
        benchmark(
            message_rate=args.message_rate,
            batch_size=args.batch_size,
            prefetch_count=args.prefetch_count,
            message_size=args.message_size,
            worker_count=args.worker_count,
            relay_count=args.relay_count,
        )
    )
