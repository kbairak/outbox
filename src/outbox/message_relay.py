from __future__ import annotations

import asyncio
import datetime
import json
import time
from dataclasses import dataclass
from typing import Literal, cast

import aio_pika
import asyncpg
from aio_pika.abc import AbstractConnection, AbstractExchange, DateType, HeadersType
from aio_pika.message import encode_expiration
from asyncpg.pool import PoolConnectionProxy

from .log import logger
from .metrics import metrics
from .utils import get_rmq_connection

# Fixed delay for database retry (seconds)
RETRY_DELAY_SECONDS = 10


@dataclass
class MessageRelay:
    db_engine_url: str | None = None
    db_pool: asyncpg.Pool | None = None
    rmq_connection: AbstractConnection | None = None
    rmq_connection_url: str | None = None
    exchange_name: str = "outbox"
    notification_timeout: float = 60.0
    expiration: DateType | None = None
    clean_up_after: (
        Literal["IMMEDIATELY"] | Literal["NEVER"] | datetime.timedelta | int | float | None
    ) = None
    table_name: str = "outbox_table"
    batch_size: int = 50
    enable_metrics: bool = True

    def __post_init__(self) -> None:
        metrics.enable_metrics(self.enable_metrics)
        if self.rmq_connection is not None and self.rmq_connection_url is not None:
            raise ValueError("You cannot set both rmq_connection and rmq_connection_url")
        if self.clean_up_after is not None and (
            isinstance(self.clean_up_after, (int, float))
            or (
                isinstance(self.clean_up_after, str)
                and self.clean_up_after not in ("IMMEDIATELY", "NEVER")
            )
        ):
            milliseconds = encode_expiration(self.clean_up_after)
            if milliseconds is None:
                raise ValueError(
                    f"Invalid clean_up_after value: encode_expiration returned None for "
                    f"{self.clean_up_after!r}"
                )
            self.clean_up_after = datetime.timedelta(milliseconds=int(milliseconds))
        if self.db_engine_url is not None and self.db_pool is not None:
            raise ValueError("You must provide one of db_engine_url or db_pool, not both")

    async def _ensure_pool(self) -> asyncpg.Pool:
        """Ensure the connection pool is created."""
        if self.db_pool is None:
            self.db_pool = await asyncpg.create_pool(self.db_engine_url, min_size=1, max_size=10)
        return self.db_pool

    async def run(self) -> None:
        pool = await self._ensure_pool()

        if self.rmq_connection is None and self.rmq_connection_url is not None:
            self.rmq_connection = await get_rmq_connection(self.rmq_connection_url)
        assert self.rmq_connection is not None

        logger.info(f"Starting message relay on exchange: {self.exchange_name} ...")
        channel = await self.rmq_connection.channel()
        exchange = await channel.declare_exchange(
            self.exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
        )

        # Acquire dedicated connection for LISTEN
        listen_conn = await pool.acquire()
        notification_event = asyncio.Event()
        await listen_conn.add_listener("outbox_channel", lambda *_: notification_event.set())

        try:
            while True:
                try:
                    # Update table backlog gauge
                    async with pool.acquire() as conn:
                        backlog_count = await conn.fetchval(
                            f""" SELECT COUNT(*)
                                 FROM {self.table_name}
                                 WHERE sent_at IS NULL AND send_after <= $1""",
                            datetime.datetime.now(datetime.timezone.utc),
                        )
                        backlog_count = cast(int, backlog_count or 0)
                        metrics.table_backlog.labels(exchange_name=self.exchange_name).set(
                            backlog_count
                        )

                        # Log backlog if significant
                        if backlog_count > 100:
                            logger.warning(f"Outbox backlog: {backlog_count} unsent messages")
                        elif backlog_count > 0:
                            logger.debug(f"Outbox backlog: {backlog_count} unsent messages")

                    # Process all ready messages
                    await self._consume_outbox_table(exchange)

                    # Query for next scheduled message time
                    async with pool.acquire() as conn:
                        next_send_time = cast(
                            datetime.datetime | None,
                            await conn.fetchval(
                                f""" SELECT MIN(send_after)
                                     FROM {self.table_name}
                                     WHERE sent_at IS NULL AND send_after > $1""",
                                datetime.datetime.now(datetime.timezone.utc),
                            ),
                        )

                    # Calculate wait time
                    if next_send_time:
                        timeout = min(
                            (
                                next_send_time - datetime.datetime.now(datetime.timezone.utc)
                            ).total_seconds(),
                            self.notification_timeout,
                        )
                    else:
                        timeout = self.notification_timeout

                    # Wait for notification or timeout
                    try:
                        await asyncio.wait_for(notification_event.wait(), timeout=timeout)
                    except asyncio.TimeoutError:
                        pass
                    finally:
                        notification_event.clear()

                except Exception as exc:
                    logger.error(
                        f"Database operation failed: {exc}. Retrying in {RETRY_DELAY_SECONDS}s...",
                        exc_info=True,
                    )
                    await asyncio.sleep(RETRY_DELAY_SECONDS)
                    continue  # Skip rest of loop, retry
        finally:
            if listen_conn:
                await pool.release(listen_conn)

    async def _consume_outbox_table(self, exchange: AbstractExchange | None = None) -> None:
        "Consume outbox table until it's empty (of pending messages)"

        pool = await self._ensure_pool()

        if exchange is None:
            assert self.rmq_connection is not None
            channel = await self.rmq_connection.channel()
            exchange = await channel.declare_exchange(
                self.exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
            )
        while True:
            async with pool.acquire() as conn, conn.transaction():
                count = await self._consume_outbox_batch(exchange, conn)
                if count == 0:
                    break

    async def _consume_outbox_batch(
        self,
        exchange: AbstractExchange,
        conn: asyncpg.Connection[asyncpg.Record] | PoolConnectionProxy[asyncpg.Record],
    ) -> int:
        """Consume a single batch of messages from the outbox table.

        Returns the number of messages processed.
        """
        poll_start_time = time.perf_counter()
        logger.debug("Checking for unsent messages...")

        outbox_rows = await conn.fetch(
            f""" SELECT id, routing_key, body, tracking_ids, created_at, expiration
                 FROM {self.table_name}
                 WHERE sent_at IS NULL AND send_after <= $1
                 ORDER BY created_at
                 LIMIT $2
                 FOR UPDATE SKIP LOCKED""",
            datetime.datetime.now(datetime.timezone.utc),
            self.batch_size,
        )

        if not outbox_rows:
            logger.debug("No unsent messages found")
            return 0

        logger.debug(f"Processing {len(outbox_rows)} messages...")

        publish_results = await asyncio.gather(
            *[
                exchange.publish(
                    aio_pika.Message(
                        row["body"],
                        content_type="application/json",
                        expiration=row["expiration"] or self.expiration,
                        headers=cast(
                            HeadersType,
                            {
                                "x-outbox-tracking-ids": (
                                    json.dumps(row["tracking_ids"])
                                    if isinstance(row["tracking_ids"], list)
                                    else row["tracking_ids"]
                                )
                            },
                        ),
                    ),
                    row["routing_key"],
                )
                for row in outbox_rows
            ],
            return_exceptions=True,
        )

        # Separate successful vs failed publishes
        successful_ids = []
        now = datetime.datetime.now(datetime.timezone.utc)
        for i, publish_result in enumerate(publish_results):
            row = outbox_rows[i]
            if isinstance(publish_result, Exception):
                error_type = type(publish_result).__name__
                metrics.publish_failures.labels(
                    exchange_name=self.exchange_name,
                    failure_type="main",
                    error_type=error_type,
                ).inc()
                logger.error(
                    f"Failed to publish message id={row['id']} to RabbitMQ: "
                    f"routing_key={row['routing_key']}, exchange={self.exchange_name}, "
                    f"error={error_type}, {publish_result!r}"
                )
            else:
                successful_ids.append(row["id"])
                metrics.messages_published.labels(exchange_name=self.exchange_name).inc()

                # Record message age (time from creation to publish)
                message_age_seconds = (now - row["created_at"]).total_seconds()
                metrics.message_age.labels(exchange_name=self.exchange_name).observe(
                    message_age_seconds
                )

        poll_duration_seconds = time.perf_counter() - poll_start_time
        metrics.poll_duration.labels(exchange_name=self.exchange_name).observe(
            poll_duration_seconds
        )

        # Batch UPDATE/DELETE: Mark only successful messages as sent
        if successful_ids:
            logger.debug(f"Sent {len(successful_ids)} messages to RabbitMQ")
            if self.clean_up_after == "IMMEDIATELY":
                await conn.execute(
                    f"DELETE FROM {self.table_name} WHERE id = ANY($1)",
                    successful_ids,
                )
            else:
                await conn.execute(
                    f"UPDATE {self.table_name} SET sent_at = $1 WHERE id = ANY($2)",
                    now,
                    successful_ids,
                )

        # Time-based cleanup
        if isinstance(self.clean_up_after, datetime.timedelta):
            delete_result = await conn.execute(
                f""" DELETE FROM {self.table_name}
                     WHERE sent_at IS NOT NULL AND sent_at < $1""",
                now - self.clean_up_after,
            )
            deleted_count = int(delete_result.split()[-1])
            if deleted_count > 0:
                logger.debug(
                    f"Cleaned up {deleted_count} messages older than {self.clean_up_after}"
                )

        return len(outbox_rows)
