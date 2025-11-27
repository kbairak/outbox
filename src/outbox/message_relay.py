import asyncio
import datetime
import json
import time
from copy import copy
from dataclasses import dataclass
from typing import Literal, Optional, Union, cast

import aio_pika
import asyncpg
from aio_pika.abc import AbstractConnection, AbstractExchange, DateType, HeadersType
from aio_pika.message import encode_expiration
from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

from outbox.database import OutboxTable
from outbox.utils import ensure_database_async

from .log import logger
from .metrics import metrics


@dataclass
class MessageRelay:
    db_engine: Optional[AsyncEngine] = None
    db_engine_url: Optional[str] = None
    rmq_connection: Optional[AbstractConnection] = None
    rmq_connection_url: Optional[str] = None
    exchange_name: str = "outbox"
    notification_timeout: float = 60.0
    expiration: Optional[DateType] = None
    clean_up_after: Union[
        Literal["IMMEDIATELY"], Literal["NEVER"], datetime.timedelta, int, float, None
    ] = None
    table_name: str = "outbox_table"
    batch_size: int = 50
    auto_create_table: bool = False
    enable_metrics: bool = True

    def __post_init__(self) -> None:
        metrics.enable_metrics(self.enable_metrics)
        if self.db_engine is not None and self.db_engine_url is not None:
            raise ValueError("You cannot set both db_engine and db_engine_url")
        if self.rmq_connection is not None and self.rmq_connection_url is not None:
            raise ValueError("You cannot set both rmq_connection and rmq_connection_url")
        if self.db_engine_url is not None:
            self.db_engine = create_async_engine(self.db_engine_url)
        if self.table_name is not None:
            OutboxTable.__tablename__ = self.table_name
        if self.clean_up_after is not None:
            if isinstance(self.clean_up_after, (int, float)) or (
                isinstance(self.clean_up_after, str)
                and self.clean_up_after not in ("IMMEDIATELY", "NEVER")
            ):
                milliseconds = encode_expiration(self.clean_up_after)
                assert milliseconds is not None
                self.clean_up_after = datetime.timedelta(milliseconds=int(milliseconds))

    async def run(self) -> None:
        if self.db_engine is None:
            raise ValueError("Database engine is not set up.")
        if self.auto_create_table:
            await ensure_database_async(self.db_engine)
        if self.rmq_connection is None:
            if self.rmq_connection_url is not None:
                self.rmq_connection = await aio_pika.connect_robust(self.rmq_connection_url)
            else:
                raise ValueError("RabbitMQ connection is not set up.")
        assert self.rmq_connection is not None

        logger.info(f"Starting message relay on exchange: {self.exchange_name} ...")
        channel = await self.rmq_connection.channel()
        exchange = await channel.declare_exchange(
            self.exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
        )

        # Create dedicated asyncpg connection for LISTEN
        listen_conn = await asyncpg.connect(
            copy(self.db_engine.url)
            .set(drivername="postgresql")
            .render_as_string(hide_password=False)
        )
        notification_event = asyncio.Event()
        await listen_conn.add_listener("outbox_channel", lambda *_: notification_event.set())

        try:
            while True:
                # Update table backlog gauge
                async with AsyncSession(self.db_engine) as session:
                    count_stmt = (
                        select(func.count())
                        .select_from(OutboxTable)
                        .where(
                            OutboxTable.sent_at.is_(None),
                            OutboxTable.send_after <= datetime.datetime.now(datetime.timezone.utc),
                        )
                    )
                    backlog_count = (await session.execute(count_stmt)).scalar()
                    metrics.table_backlog.labels(exchange_name=self.exchange_name).set(
                        backlog_count or 0
                    )

                # Process all ready messages
                await self._consume_outbox_table(exchange)

                # Query for next scheduled message time
                async with AsyncSession(self.db_engine) as session:
                    result = await session.execute(
                        select(func.min(OutboxTable.send_after)).where(
                            OutboxTable.sent_at.is_(None),
                            OutboxTable.send_after > datetime.datetime.now(datetime.timezone.utc),
                        )
                    )
                    next_send_time = result.scalar()

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
        finally:
            if listen_conn:
                await listen_conn.close()

    async def _consume_outbox_table(self, exchange: Optional[AbstractExchange] = None) -> None:
        "Consume outbox table until it's empty (of pending messages)"

        if exchange is None:
            assert self.rmq_connection is not None
            channel = await self.rmq_connection.channel()
            exchange = await channel.declare_exchange(
                self.exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
            )
        while True:
            async with AsyncSession(self.db_engine) as session, session.begin():
                count = await self._consume_outbox_batch(exchange, session)
                if count == 0:
                    break

    async def _consume_outbox_batch(
        self, exchange: AbstractExchange, session: AsyncSession
    ) -> int:
        """Consume a single batch of messages from the outbox table.

        Returns the number of messages processed.
        """
        poll_start_time = time.perf_counter()
        logger.debug("Checking for unsent messages...")
        select_stmt = (
            select(OutboxTable)
            .where(
                OutboxTable.sent_at.is_(None),
                OutboxTable.send_after <= datetime.datetime.now(datetime.timezone.utc),
            )
            .order_by(OutboxTable.created_at)
            .limit(self.batch_size)
            .with_for_update(skip_locked=True)
        )
        outbox_rows = (await session.execute(select_stmt)).scalars().all()

        if not outbox_rows:
            logger.debug("No unsent messages found")
            return 0

        logger.debug(f"Processing {len(outbox_rows)} messages...")

        results = await asyncio.gather(
            *[
                exchange.publish(
                    aio_pika.Message(
                        row.body,
                        content_type="application/json",
                        expiration=row.expiration or self.expiration,
                        headers=cast(
                            HeadersType, {"x-outbox-tracking-ids": json.dumps(row.tracking_ids)}
                        ),
                    ),
                    row.routing_key,
                )
                for row in outbox_rows
            ],
            return_exceptions=True,
        )

        # Separate successful vs failed publishes
        successful_ids = []
        now = datetime.datetime.now(datetime.timezone.utc)
        for i, result in enumerate(results):
            row = outbox_rows[i]
            if isinstance(result, Exception):
                error_type = type(result).__name__
                metrics.publish_failures.labels(
                    exchange_name=self.exchange_name,
                    failure_type="main",
                    error_type=error_type,
                ).inc()
                logger.error(
                    f"Failed to publish message to RabbitMQ: {row}, "
                    f"exchange={self.exchange_name}, error={error_type}, {result!r}"
                )
            else:
                successful_ids.append(row.id)
                metrics.messages_published.labels(exchange_name=self.exchange_name).inc()

                # Record message age (time from creation to publish)
                message_age_seconds = (now - row.created_at).total_seconds()
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
                await session.execute(
                    delete(OutboxTable).where(OutboxTable.id.in_(successful_ids))
                )
            else:
                await session.execute(
                    update(OutboxTable)
                    .where(OutboxTable.id.in_(successful_ids))
                    .values(sent_at=now)
                )

        # Time-based cleanup
        if isinstance(self.clean_up_after, datetime.timedelta):
            delete_stmt = delete(OutboxTable).where(
                OutboxTable.sent_at.is_not(None),
                OutboxTable.sent_at < now - self.clean_up_after,
            )
            await session.execute(delete_stmt)

        return len(outbox_rows)
