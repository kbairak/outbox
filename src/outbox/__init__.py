import asyncio
import datetime
import inspect
import json
import logging
import reprlib
import signal
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
from sqlalchemy import JSON, DateTime, Text, delete, func, select
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

logger = logging.getLogger("outbox")


class Base(DeclarativeBase):
    pass


class OutboxTable(Base):
    __tablename__ = "outbox_table"

    id: Mapped[int] = mapped_column(primary_key=True)
    routing_key: Mapped[str] = mapped_column(Text)
    body: Mapped[bytes] = mapped_column()
    track_ids: Mapped[list[str]] = mapped_column(JSON)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    retry_limit: Mapped[int | None] = mapped_column()
    expiration: Mapped[datetime.timedelta | None] = mapped_column()
    send_after: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True))
    sent_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))

    def __repr__(self):
        routing_key = self.routing_key
        body = reprlib.repr(self.body)
        track_ids = self.track_ids
        created_at = self.created_at
        args = [f"{routing_key=}", f"{body=}", f"{track_ids=}", f"{created_at=}"]
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


class Retry(Exception):
    pass


class Reject(Exception):
    pass


@dataclass
class Listener:
    binding_key: str
    callback: Callable[..., Coroutine[Any, Any, None]]
    queue: str = ""
    retry_on_error: bool | None = None
    retry_limit: int | None = None
    queue_obj: AbstractQueue | None = None
    consumer_tag: ConsumerTag | None = None

    def __post_init__(self):
        if not self.queue:
            self.queue = f"{self.callback.__module__}.{self.callback.__qualname__}".replace(
                "<", ""
            ).replace(">", "")

    def __call__(self, *args, **kwargs) -> Coroutine[Any, Any, None]:
        return self.callback(*args, **kwargs)

    async def _handle(self, message: AbstractIncomingMessage) -> None:
        parameters = inspect.signature(self.callback).parameters
        parameter_keys = set(parameters.keys()) - {
            "routing_key",
            "message",
            "track_ids",
            "retry_limit",
            "retry_on_error",
            "queue_name",
            "attempt_count",
        }
        if len(parameter_keys) != 1:
            raise ValueError("Worker functions must accept exactly one argument")
        body_param_key = parameter_keys.pop()
        body_param = parameters[body_param_key]

        track_ids = tuple(json.loads(cast(str, message.headers.get("x-outbox-track-ids", "[]"))))
        token = _track_ids.set(track_ids)

        retry_limit = (
            cast(int | None, message.headers.get("x-outbox-retry-limit")) or self.retry_limit
        )
        if retry_limit is not None:
            retry_limit = int(retry_limit)
        attempt_count = cast(str | None, message.headers.get("x-delivery-count"))
        if attempt_count is not None:
            attempt_count = int(attempt_count)

        if retry_limit is not None and attempt_count is not None and attempt_count >= retry_limit:
            logger.warning(
                f"Message {message.routing_key} with track IDs {track_ids} exceeded retry limit "
                f"{retry_limit}, rejecting"
            )
            await message.nack(requeue=False)
            _track_ids.reset(token)
            return

        routing_key, body = message.routing_key, message.body
        try:
            if issubclass(body_param.annotation, BaseModel):
                body = body_param.annotation.model_validate_json(message.body)
            elif issubclass(body_param.annotation, bytes):
                body = message.body
            else:
                body = json.loads(message.body)
        except Exception as exc:
            logger.error(
                f"Failed to deserialize message body {routing_key=}, {track_ids=}, {body=}, {exc=}"
            )
            raise  # TODO: Will (should) this crash the worker?
        else:
            logger.info(f"Processing message {routing_key=}, {track_ids=}, {body=}")

        kwargs = {body_param_key: body}
        for attr in (
            "routing_key",
            "message",
            "track_ids",
            "retry_limit",
            "retry_on_error",
            "queue_name",
            "attempt_count",
        ):
            if attr in parameters:
                kwargs[attr] = locals()[attr]

        try:
            await self.callback(**kwargs)
        except Retry:
            logger.info(f"Retrying (forced) {routing_key=}, {track_ids=}, {body=}")
            await message.nack(requeue=True)
        except Reject:
            logger.warning(
                f"Rejecting, this message will likely end up in DLX {routing_key=}, "
                f"{track_ids=}, {body=}"
            )
            await message.nack(requeue=False)
        except Exception:
            logger.warning(
                f"{'Retrying' if self.retry_on_error else 'Aborting'} {routing_key=}, "
                f"{track_ids=}, {body=}"
            )
            if self.retry_on_error:
                await message.nack(requeue=True)
            else:
                await message.ack()
        else:
            logger.info(f"Success {routing_key=}, {track_ids=}, {body=}")
            await message.ack()
        finally:
            _track_ids.reset(token)


def listen(
    binding_key: str,
    queue: str = "",
    retry_on_error: bool | None = None,
    retry_limit: int | None = None,
):
    def decorator(func: Callable[..., Coroutine[Any, Any, None]]) -> Listener:
        return Listener(binding_key, func, queue, retry_on_error, retry_limit)

    return decorator


_track_ids: ContextVar[tuple[str, ...],] = ContextVar("track_ids", default=())


@dataclass
class Outbox:
    db_engine: AsyncEngine | None = None
    db_engine_url: str | None = None
    rmq_connection: AbstractConnection | None = None
    rmq_connection_url: str | None = None
    exchange_name: str = "outbox_exchange"
    poll_interval: float = 1.0
    retry_on_error: bool = True
    expiration: DateType | None = None
    clean_up_after: (
        Literal["IMMEDIATELY"] | Literal["NEVER"] | datetime.timedelta | int | float | None
    ) = None
    retry_limit: int | None = None
    table_name: str = "outbox_table"
    _table_created: bool = False
    _shutdown_future: asyncio.Future | None = None

    def __post_init__(self):
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

    def setup(self, **kwargs):
        field_names = {field.name for field in fields(self)}
        for key, value in kwargs.items():
            if key not in field_names:
                raise ValueError(f"Invalid configuration key: {key}")
            if value is not None:
                setattr(self, key, value)
        self.__post_init__()

    async def _ensure_database(self) -> None:
        if not self._table_created and self.db_engine:
            async with self.db_engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            self._table_created = True

    async def _get_rmq_connection(self) -> AbstractConnection | None:
        if self.rmq_connection is None and self.rmq_connection_url is not None:
            self.rmq_connection = await aio_pika.connect(self.rmq_connection_url)
        return self.rmq_connection

    async def emit(
        self,
        session: AsyncSession,
        routing_key: str,
        body: Any,
        *,
        retry_limit: int | None = None,
        expiration: DateType = None,
        eta: DateType | None = None,
    ) -> None:
        await self._ensure_database()

        if isinstance(body, BaseModel):
            body = body.model_dump_json().encode()
        elif not isinstance(body, bytes):
            body = json.dumps(body).encode()

        now = datetime.datetime.now(datetime.UTC)
        outbox_row = OutboxTable(
            routing_key=routing_key,
            body=body,
            track_ids=list(self.get_track_ids() + (str(uuid.uuid4()),)),
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
            outbox_row.send_after = datetime.datetime.now(datetime.UTC) + datetime.timedelta(
                milliseconds=int(milliseconds)
            )
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
            await self._consume_outbox_table(exchange)
            await asyncio.sleep(self.poll_interval)

    async def _consume_outbox_table(self, exchange: AbstractExchange | None = None) -> None:
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
                logger.debug("Checking for unsent messages...")
                stmt = (
                    select(OutboxTable)
                    .where(
                        OutboxTable.sent_at.is_(None),
                        OutboxTable.send_after <= datetime.datetime.now(datetime.UTC),
                    )
                    .order_by(OutboxTable.created_at)
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
                try:
                    outbox_row = (await session.execute(stmt)).scalars().one()
                except NoResultFound:
                    logger.debug("No unsent messages found, waiting...")
                    break

                logger.debug(f"Processing message: {outbox_row}")
                expiration = outbox_row.expiration or self.expiration
                headers = {"x-outbox-track-ids": json.dumps(outbox_row.track_ids)}
                if outbox_row.retry_limit is not None:
                    headers["x-outbox-retry-limit"] = str(outbox_row.retry_limit)
                headers = cast(HeadersType, headers)
                await exchange.publish(
                    aio_pika.Message(
                        outbox_row.body,
                        content_type="application/json",
                        expiration=expiration,
                        headers=headers,
                    ),
                    outbox_row.routing_key,
                )
                logger.debug(f"Sent message: {outbox_row} to RabbitMQ")
                if self.clean_up_after == "IMMEDIATELY":
                    await session.delete(outbox_row)
                else:
                    outbox_row.sent_at = datetime.datetime.now(datetime.UTC)
                if isinstance(self.clean_up_after, datetime.timedelta):
                    stmt = delete(OutboxTable).where(
                        OutboxTable.sent_at.is_not(None),
                        OutboxTable.sent_at
                        < datetime.datetime.now(datetime.UTC) - self.clean_up_after,
                    )
                    await session.execute(stmt)

    async def worker(self, listeners: Sequence[Listener]) -> None:
        await self._set_up_queues(listeners)

        self._shutdown_future = asyncio.Future()
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGINT, self._shutdown_future.set_result, None)
        loop.add_signal_handler(signal.SIGTERM, self._shutdown_future.set_result, None)

        tasks = set()

        logger.info(f"Starting worker on exchange: {self.exchange_name} ...")
        for listener in listeners:
            assert listener.queue_obj is not None
            if listener.retry_on_error is None:
                listener.retry_on_error = self.retry_on_error
            if listener.retry_limit is None:
                listener.retry_limit = self.retry_limit

            async def _task(
                message: AbstractIncomingMessage, listener: Listener = listener
            ) -> None:
                if self._shutdown_future is not None and self._shutdown_future.done():
                    await message.nack(requeue=True)
                    return
                task = asyncio.create_task(listener._handle(message))
                tasks.add(task)
                task.add_done_callback(tasks.discard)

            listener.consumer_tag = await listener.queue_obj.consume(_task)

        await self._shutdown_future

        logger.info("Received shutdown signal, waiting or ongoing tasks and exiting...")

        for listener in listeners:
            if listener.consumer_tag is None:
                continue
            assert listener.queue_obj is not None
            assert listener.consumer_tag is not None
            await listener.queue_obj.cancel(listener.consumer_tag)

        if tasks:
            await asyncio.wait(tasks)

    async def _set_up_queues(self, listeners: Sequence[Listener]) -> None:
        rmq_connection = await self._get_rmq_connection()
        if rmq_connection is None:
            raise ValueError("RabbitMQ connection is not set up.")

        channel = await rmq_connection.channel()
        exchange = await channel.declare_exchange(
            self.exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
        )
        dead_letter_exchange = await channel.declare_exchange(
            f"{self.exchange_name}.dlx", aio_pika.ExchangeType.DIRECT, durable=True
        )
        for listener in listeners:
            dead_letter_queue_obj = await channel.declare_queue(
                f"{listener.queue}.dlq", durable=True, arguments={"x-queue-type": "quorum"}
            )
            await dead_letter_queue_obj.bind(dead_letter_exchange, listener.queue)

            logger.debug(
                f"Binding queue {listener.queue} to exchange {self.exchange_name} with "
                f"binding key {listener.binding_key}"
            )
            listener.queue_obj = await channel.declare_queue(
                listener.queue,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": f"{self.exchange_name}.dlx",
                    "x-dead-letter-routing-key": listener.queue,
                    "x-queue-type": "quorum",
                },
            )
            await listener.queue_obj.bind(exchange, listener.binding_key)

    @contextmanager
    def tracking(self) -> Generator[None, None, None]:
        track_ids = _track_ids.get()
        track_ids = track_ids + (str(uuid.uuid4()),)
        token = _track_ids.set(track_ids)
        yield
        _track_ids.reset(token)

    def get_track_ids(self) -> tuple[str, ...]:
        return _track_ids.get()


outbox = Outbox()
setup = outbox.setup
emit = outbox.emit
message_relay = outbox.message_relay
worker = outbox.worker
tracking = outbox.tracking
get_track_ids = outbox.get_track_ids
