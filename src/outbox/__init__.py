import asyncio
import datetime
import inspect
import json
import logging
import reprlib
import signal
import time
import uuid
from collections.abc import Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field, fields
from typing import (
    Any,
    Callable,
    Coroutine,
    Generator,
    Literal,
    Optional,
    Union,
    cast,
)

import aio_pika
from aio_pika.abc import (
    AbstractConnection,
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractQueue,
    ConsumerTag,
    DateType,
    HeadersType,
)
from aio_pika.message import encode_expiration
from pydantic import BaseModel
from sqlalchemy import JSON, DateTime, Index, Text, delete, func, select, text, update
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from .metrics import metrics

logger = logging.getLogger("outbox")


class Base(DeclarativeBase):
    pass


class OutboxTable(Base):
    __tablename__ = "outbox_table"

    id: Mapped[int] = mapped_column(primary_key=True)
    routing_key: Mapped[str] = mapped_column(Text)
    body: Mapped[bytes] = mapped_column()
    tracking_ids: Mapped[list[str]] = mapped_column(JSON)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    retry_limit: Mapped[Optional[int]] = mapped_column()
    expiration: Mapped[Optional[datetime.timedelta]] = mapped_column()
    send_after: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True))
    sent_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        # Partial index for pending messages
        Index(
            "outbox_pending_idx",
            "send_after",
            "created_at",
            postgresql_where=(sent_at.is_(None)),
        ),
        # Partial index for cleanup
        Index(
            "outbox_cleanup_idx",
            "sent_at",
            postgresql_where=(sent_at.is_not(None)),
        ),
    )

    def __repr__(self) -> str:
        routing_key = self.routing_key
        body = reprlib.repr(self.body)
        tracking_ids = self.tracking_ids
        created_at = self.created_at
        args = [f"{routing_key=}", f"{body=}", f"{tracking_ids=}", f"{created_at=}"]
        if self.send_after != self.created_at:
            send_after = self.send_after
            args.append(f"{send_after=}")
        if self.retry_limit is not None:
            retry_limit = self.retry_limit
            args.append(f"{retry_limit=}")
        if self.expiration:
            expiration = self.expiration.total_seconds
            args.append(f"{expiration=}")
        return f"OutboxTable({', '.join(args)})"


class Reject(Exception):
    pass


@dataclass
class Listener:
    binding_key: str
    callback: Union[Callable[..., Any], Callable[..., Coroutine[Any, Any, None]]]
    queue: str = ""
    retry_delays: Optional[Sequence[int]] = None
    _queue_obj: Optional[AbstractQueue] = None
    _consumer_tag: Optional[ConsumerTag] = None
    _delay_exchanges: dict[int, AbstractExchange] = field(default_factory=dict)
    _exchange_name: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.queue:
            # callback is always a function (decorated), safe to access __module__ and __qualname__
            callback_func = cast(Any, self.callback)
            self.queue = f"{callback_func.__module__}.{callback_func.__qualname__}".replace(
                "<", ""
            ).replace(">", "")

        # Auto-wrap sync callbacks to async using asyncio.to_thread
        if not asyncio.iscoroutinefunction(self.callback):
            sync_callback = self.callback

            async def async_wrapper(
                *args: Any, _sync_callback: Callable[..., Any] = sync_callback, **kwargs: Any
            ) -> None:
                return await asyncio.to_thread(_sync_callback, *args, **kwargs)

            # Preserve function signature for introspection
            async_wrapper.__signature__ = inspect.signature(sync_callback)  # type: ignore[attr-defined]

            self.callback = async_wrapper

    def __call__(self, *args: Any, **kwargs: Any) -> Coroutine[Any, Any, None]:
        return self.callback(*args, **kwargs)

    async def _handle(self, message: AbstractIncomingMessage) -> None:
        assert self._exchange_name is not None
        metrics.messages_received.labels(queue=self.queue, exchange_name=self._exchange_name).inc()

        parameters = inspect.signature(self.callback).parameters
        parameter_keys = set(parameters.keys()) - {
            "routing_key",
            "message",
            "tracking_ids",
            "queue_name",
            "attempt_count",
        }
        if len(parameter_keys) != 1:
            raise ValueError("Worker functions must accept exactly one argument")
        body_param_key = parameter_keys.pop()
        body_param = parameters[body_param_key]

        tracking_ids = tuple(
            json.loads(cast(str, message.headers.get("x-outbox-tracking-ids", "[]")))
        )
        token = _tracking_ids.set(tracking_ids)

        attempt_count_header = cast(Optional[str], message.headers.get("x-delivery-count"))
        retry_delays = self.retry_delays or ()

        if attempt_count_header is not None:
            attempt_count = int(attempt_count_header)

            # TODO: check if this is really needed
            # Check if retries are exhausted (only check if retry_delays is configured)
            # If retry_delays is empty, messages go to DLQ (see exception handler below)
            if retry_delays and attempt_count > len(retry_delays) + 1:
                logger.warning(
                    f"Message {message.routing_key} with tracking IDs {tracking_ids} exceeded "
                    f"retry attempts ({attempt_count} > {len(retry_delays) + 1}), sending to DLQ"
                )
                await message.nack(requeue=False)
                _tracking_ids.reset(token)
                return
        else:
            attempt_count = 1

        # Get routing key - use original routing key if preserved in header (for retries)
        # otherwise use message routing key (for initial delivery)
        routing_key = (
            cast(Optional[str], message.headers.get("x-original-routing-key"))
            or message.routing_key
        )
        assert routing_key is not None
        body = message.body
        try:
            if issubclass(body_param.annotation, BaseModel):
                body = body_param.annotation.model_validate_json(message.body)
            elif issubclass(body_param.annotation, bytes):
                body = message.body
            else:
                body = json.loads(message.body)
        except Exception as exc:
            logger.error(
                f"Failed to deserialize message body {routing_key=}, {tracking_ids=}, {body=}, "
                f"{exc=}"
            )
            raise  # TODO: Will (should) this crash the worker?
        else:
            logger.info(f"Processing message {routing_key=}, {tracking_ids=}, {body=}")

        kwargs: dict[str, Any] = {body_param_key: body}
        if "routing_key" in parameters:
            kwargs["routing_key"] = routing_key
        if "message" in parameters:
            kwargs["message"] = message
        if "tracking_ids" in parameters:
            kwargs["tracking_ids"] = tracking_ids
        if "queue_name" in parameters:
            kwargs["queue_name"] = self.queue
        if "attempt_count" in parameters:
            kwargs["attempt_count"] = attempt_count

        processing_start_time = time.perf_counter()

        try:
            await self.callback(**kwargs)
        except Reject:
            logger.warning(
                f"Rejecting, this message will end up in DLQ {routing_key=}, {tracking_ids=}, "
                f"{body=}"
            )
            await message.nack(requeue=False)
            metrics.messages_processed.labels(
                queue=self.queue, exchange_name=self._exchange_name, status="rejected"
            ).inc()
        except Exception:
            if retry_delays:
                logger.warning(f"Retrying {routing_key=}, {tracking_ids=}, {body=}")
                await self._delayed_retry(message, attempt_count, tracking_ids)
            else:
                # TODO: Check if this is really needed, maybe _delayed_retry() will take care of it
                logger.warning(
                    f"No retries configured, sending to DLQ {routing_key=}, {tracking_ids=}, "
                    f"{body=}"
                )
                await message.nack(requeue=False)
            metrics.messages_processed.labels(
                queue=self.queue, exchange_name=self._exchange_name, status="failed"
            ).inc()
        else:
            logger.info(f"Success {routing_key=}, {tracking_ids=}, {body=}")
            await message.ack()
            metrics.messages_processed.labels(
                queue=self.queue, exchange_name=self._exchange_name, status="success"
            ).inc()
        finally:
            processing_duration_seconds = time.perf_counter() - processing_start_time
            metrics.message_processing_duration.labels(
                queue=self.queue, exchange_name=self._exchange_name
            ).observe(processing_duration_seconds)

            _tracking_ids.reset(token)

    async def _delayed_retry(
        self,
        message: AbstractIncomingMessage,
        attempt_count: int,
        tracking_ids: tuple[str, ...],
    ) -> None:
        """Publish message to delay queue for retry after backoff period."""
        retry_delays = self.retry_delays or ()

        if attempt_count > len(retry_delays):
            logger.warning(
                f"Exceeded retry attempts ({attempt_count} > {len(retry_delays)}), sending to DLQ"
            )
            await message.nack(requeue=False)
            return

        # Get delay for this attempt
        delay = retry_delays[attempt_count - 1]

        # For instant retries (delay=0), use nack(requeue=True) to avoid unnecessary exchange/queue overhead
        if delay == 0:
            await message.nack(requeue=True)
            metrics.retry_attempts.labels(queue=self.queue, delay_seconds=str(delay)).inc()
            logger.info(
                f"Message requeued for instant retry (attempt {attempt_count}/{len(retry_delays)}) "
                f"routing_key={message.routing_key}, {tracking_ids=}"
            )
            return

        delay_exchange = self._delay_exchanges[delay]

        # Prepare new headers with incremented delivery count and preserve original routing key
        new_headers = dict(message.headers) if message.headers else {}
        new_headers["x-delivery-count"] = str(attempt_count + 1)

        # Preserve original routing key in header (only set on first retry)
        assert message.routing_key is not None
        if "x-original-routing-key" not in new_headers:
            new_headers["x-original-routing-key"] = message.routing_key

        # Publish to delay exchange using queue name as routing key
        # This ensures retry routes back to the SAME queue (via default exchange DLX)
        # rather than re-routing through topic exchange to multiple queues
        try:
            await delay_exchange.publish(
                aio_pika.Message(
                    body=message.body,
                    content_type=message.content_type,
                    headers=new_headers,
                ),
                routing_key=self.queue,  # Use queue name, not original routing key
            )
        except Exception as exc:
            metrics.publish_failures.labels(
                exchange_name=delay_exchange.name,
                failure_type="retry",
                error_type=type(exc).__name__,
            ).inc()
            logger.error(
                f"Failed to publish message to delay exchange: routing_key={self.queue}, "
                f"original_routing_key={message.routing_key}, {tracking_ids=}, "
                f"delay_exchange={delay_exchange.name}, error={type(exc).__name__}, {exc=}"
            )
            raise

        # Ack original message
        await message.ack()

        metrics.retry_attempts.labels(queue=self.queue, delay_seconds=str(delay)).inc()

        logger.info(
            f"Message sent to delay exchange (attempt {attempt_count}/{len(retry_delays)}, "
            f"delay {delay}s) routing_key={self.queue}, original_routing_key={message.routing_key}, {tracking_ids=}"
        )


def listen(
    binding_key: str,
    queue: str = "",
    retry_delays: Optional[Sequence[int]] = None,
) -> Callable[[Union[Callable[..., Any], Callable[..., Coroutine[Any, Any, None]]]], Listener]:
    def decorator(
        func: Union[Callable[..., Any], Callable[..., Coroutine[Any, Any, None]]],
    ) -> Listener:
        return Listener(binding_key, func, queue, retry_delays)

    return decorator


_tracking_ids: ContextVar[tuple[str, ...]] = ContextVar[tuple[str, ...]](
    "tracking_ids", default=()
)


@dataclass
class Outbox:
    db_engine: Optional[AsyncEngine] = None
    db_engine_url: Optional[str] = None
    rmq_connection: Optional[AbstractConnection] = None
    rmq_connection_url: Optional[str] = None
    exchange_name: str = "outbox"
    poll_interval: float = 1.0
    expiration: Optional[DateType] = None
    clean_up_after: Union[
        Literal["IMMEDIATELY"], Literal["NEVER"], datetime.timedelta, int, float, None
    ] = None
    retry_delays: Sequence[int] = (1, 10, 60, 300)
    table_name: str = "outbox_table"
    prefetch_count: int = 10
    batch_size: int = 50
    auto_create_table: bool = False
    enable_metrics: bool = True
    _table_created: bool = False
    # Instance attribute (not just local var) to allow tests to simulate shutdown signals
    _shutdown_future: Optional[asyncio.Future[None]] = None

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

    def setup(self, **kwargs: Any) -> None:
        field_names = {field.name for field in fields(self)}
        for key, value in kwargs.items():
            if key not in field_names:
                raise ValueError(f"Invalid configuration key: {key}")
            if value is not None:
                setattr(self, key, value)
        self.__post_init__()

    async def _ensure_database(self) -> None:
        if self.auto_create_table and not self._table_created and self.db_engine:
            async with self.db_engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            self._table_created = True

    async def _get_rmq_connection(self) -> Optional[AbstractConnection]:
        if self.rmq_connection is None and self.rmq_connection_url is not None:
            self.rmq_connection = await aio_pika.connect_robust(self.rmq_connection_url)
        return self.rmq_connection

    async def emit(
        self,
        session: AsyncSession,
        routing_key: str,
        body: Any,
        *,
        retry_limit: Optional[int] = None,
        expiration: DateType = None,
        eta: Optional[DateType] = None,
    ) -> None:
        await self._ensure_database()

        if isinstance(body, BaseModel):
            body = body.model_dump_json().encode()
        elif not isinstance(body, bytes):
            body = json.dumps(body).encode()

        now = datetime.datetime.now(datetime.timezone.utc)
        outbox_row = OutboxTable(
            routing_key=routing_key,
            body=body,
            tracking_ids=list(self.get_tracking_ids() + (str(uuid.uuid4()),)),
            retry_limit=retry_limit,
            created_at=now,
        )
        if expiration is not None:
            milliseconds = encode_expiration(expiration)
            assert milliseconds is not None
            outbox_row.expiration = datetime.timedelta(milliseconds=int(milliseconds))
        if eta is not None:
            milliseconds = encode_expiration(eta)
            assert milliseconds is not None
            outbox_row.send_after = datetime.datetime.now(
                datetime.timezone.utc
            ) + datetime.timedelta(milliseconds=int(milliseconds))
        else:
            outbox_row.send_after = outbox_row.created_at
        session.add(outbox_row)

        logger.info(f"Emitted message to outbox: {outbox_row}")

    async def message_relay(self) -> None:
        if self.db_engine is None:
            raise ValueError("Database engine is not set up.")
        rmq_connection = await self._get_rmq_connection()
        if rmq_connection is None:
            raise ValueError("RabbitMQ connection is not set up.")
        await self._ensure_database()

        logger.info(f"Starting message relay on exchange: {self.exchange_name} ...")
        channel = await rmq_connection.channel()
        exchange = await channel.declare_exchange(
            self.exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
        )
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

            await self._consume_outbox_table(exchange)
            await asyncio.sleep(self.poll_interval)

    async def _consume_outbox_table(self, exchange: Optional[AbstractExchange] = None) -> None:
        "Consume outbox table until it's empty (of pending messages)"

        if exchange is None:
            rmq_connection = await self._get_rmq_connection()
            if rmq_connection is None:
                raise ValueError("RabbitMQ connection is not set up.")
            channel = await rmq_connection.channel()
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

    async def _update_dlq_metrics(self, listeners: Sequence[Listener]) -> None:
        """Background task to periodically update DLQ message count metrics."""
        rmq_connection = await self._get_rmq_connection()
        if rmq_connection is None:
            logger.warning("Cannot update DLQ metrics: RabbitMQ connection not available")
            return

        while True:
            if self._shutdown_future is not None and self._shutdown_future.done():
                break

            try:
                channel = await rmq_connection.channel()
                for listener in listeners:
                    dlq_name = f"{listener.queue}.dlq"
                    try:
                        dlq = await channel.get_queue(dlq_name, ensure=False)
                        metrics.dlq_messages.labels(queue=listener.queue).set(
                            float(dlq.declaration_result.message_count or 0)
                        )
                    except Exception as exc:
                        logger.debug(f"Failed to get DLQ message count for {dlq_name}: {exc}")
            except Exception as exc:
                logger.warning(f"Error updating DLQ metrics: {exc}")

            # Update every 30 seconds
            await asyncio.sleep(30)

    async def worker(self, listeners: Sequence[Listener]) -> None:
        await self._set_up_queues(listeners)

        self._shutdown_future = asyncio.Future()
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGINT, self._shutdown_future.set_result, None)
        loop.add_signal_handler(signal.SIGTERM, self._shutdown_future.set_result, None)

        tasks = set()

        dlq_metrics_task = (
            asyncio.create_task(self._update_dlq_metrics(listeners))
            if self.enable_metrics
            else None
        )

        logger.info(f"Starting worker on exchange: {self.exchange_name} ...")
        for listener in listeners:
            assert listener._queue_obj is not None

            async def _task(
                message: AbstractIncomingMessage, listener: Listener = listener
            ) -> None:
                if self._shutdown_future is not None and self._shutdown_future.done():
                    await message.nack(requeue=True)
                    return
                task = asyncio.create_task(listener._handle(message))
                tasks.add(task)
                task.add_done_callback(tasks.discard)

            listener._consumer_tag = await listener._queue_obj.consume(_task)
            metrics.active_consumers.labels(
                queue=listener.queue, exchange_name=self.exchange_name
            ).inc()

        await self._shutdown_future

        logger.info("Received shutdown signal, waiting or ongoing tasks and exiting...")

        for listener in listeners:
            if listener._consumer_tag is None:
                continue
            assert listener._queue_obj is not None
            await listener._queue_obj.cancel(listener._consumer_tag)
            metrics.active_consumers.labels(
                queue=listener.queue, exchange_name=self.exchange_name
            ).dec()

        if tasks:
            await asyncio.wait(tasks)

        if dlq_metrics_task is not None:
            dlq_metrics_task.cancel()

    async def _set_up_queues(self, listeners: Sequence[Listener]) -> None:
        rmq_connection = await self._get_rmq_connection()
        if rmq_connection is None:
            raise ValueError("RabbitMQ connection is not set up.")

        channel = await rmq_connection.channel()
        await channel.set_qos(prefetch_count=self.prefetch_count)
        exchange = await channel.declare_exchange(
            self.exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
        )
        dead_letter_exchange = await channel.declare_exchange(
            f"{self.exchange_name}.dlx", aio_pika.ExchangeType.DIRECT, durable=True
        )

        # Collect all unique delay values across all listeners
        # TODO: Minor: Use a reduce statement to create all_delays faster (with fewer LOC)
        all_delays = set(self.retry_delays)
        for listener in listeners:
            if listener.retry_delays:
                all_delays.update(listener.retry_delays)

        # Create delay exchanges (fanout type) and their queues
        # Skip delay=0 since instant retries use nack(requeue=True)
        delay_exchanges = {}
        for delay in all_delays:
            if delay == 0:
                continue  # Instant retries handled by nack(requeue=True), no exchange needed

            exchange_and_queue_name = f"{self.exchange_name}.delay_{delay}s"
            delay_exchange = await channel.declare_exchange(
                exchange_and_queue_name, aio_pika.ExchangeType.FANOUT, durable=True
            )
            delay_exchanges[delay] = delay_exchange

            # Create delay queue and bind to delay exchange
            delay_queue = await channel.declare_queue(
                exchange_and_queue_name,
                durable=True,
                arguments={
                    "x-message-ttl": int(delay * 1000),
                    "x-dead-letter-exchange": "",  # Default exchange (routes by queue name)
                    "x-queue-type": "quorum",
                },
            )
            # Bind to fanout exchange (no routing key needed)
            await delay_queue.bind(delay_exchange)

        # Set up listener queues and inject configuration
        for listener in listeners:
            # Inject default retry_delays, delay exchanges, and exchange_name
            if listener.retry_delays is None:
                listener.retry_delays = self.retry_delays
            listener._delay_exchanges = delay_exchanges
            listener._exchange_name = self.exchange_name

            dead_letter_queue_obj = await channel.declare_queue(
                f"{listener.queue}.dlq", durable=True, arguments={"x-queue-type": "quorum"}
            )
            await dead_letter_queue_obj.bind(dead_letter_exchange, listener.queue)

            logger.debug(
                f"Binding queue {listener.queue} to exchange {self.exchange_name} with "
                f"binding key {listener.binding_key}"
            )
            listener._queue_obj = await channel.declare_queue(
                listener.queue,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": f"{self.exchange_name}.dlx",
                    "x-dead-letter-routing-key": listener.queue,
                    "x-queue-type": "quorum",
                },
            )
            await listener._queue_obj.bind(exchange, listener.binding_key)

    @contextmanager
    def tracking(self) -> Generator[None, None, None]:
        tracking_ids = _tracking_ids.get()
        tracking_ids = tracking_ids + (str(uuid.uuid4()),)
        token = _tracking_ids.set(tracking_ids)
        yield
        _tracking_ids.reset(token)

    def get_tracking_ids(self) -> tuple[str, ...]:
        return _tracking_ids.get()


outbox = Outbox()
setup = outbox.setup
emit = outbox.emit
message_relay = outbox.message_relay
worker = outbox.worker
tracking = outbox.tracking
get_tracking_ids = outbox.get_tracking_ids
