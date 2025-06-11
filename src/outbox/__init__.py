import asyncio
import datetime
import inspect
import json
import logging
import reprlib
import signal
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field, fields
from typing import (
    Annotated,
    Any,
    Callable,
    Coroutine,
    Generator,
    Literal,
    cast,
    get_args,
    get_origin,
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
    queue_name: str
    binding_key: str
    handler: Callable[[AbstractIncomingMessage], Coroutine[None, None, None]]
    tags: set[str]
    queue: AbstractQueue | None = None
    consumer_tag: ConsumerTag | None = None


_track_ids: ContextVar[tuple[str, ...],] = ContextVar(
    "track_ids",
    default=(),
)


@dataclass
class Depends:
    func: Callable[[], Any]


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
    _listeners: list[Listener] = field(default_factory=list)
    _table_created: bool = False
    _tasks: set[asyncio.Task] = field(default_factory=set)
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
            setattr(self, key, value)

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

    def listen(
        self,
        binding_key: str,
        queue_name: str | None = None,
        retry_limit: int | None = None,
        retry_on_error: bool | None = None,
        tags: set[str] | None = None,
    ) -> Callable[[Callable[..., Coroutine[None, None, None]]], None]:
        if retry_on_error is None:
            retry_on_error = self.retry_on_error
        if tags is None:
            tags = set()

        def decorator(func: Callable):
            nonlocal queue_name
            if queue_name is None:
                queue_name = f"{func.__module__}.{func.__qualname__}".replace("<", "").replace(
                    ">", ""
                )
            parameters = inspect.signature(func).parameters
            parameter_keys = (
                set(parameters.keys())
                - {
                    "routing_key",
                    "message",
                    "track_ids",
                    "retry_limit",
                    "retry_on_error",
                    "queue_name",
                    "attempt_count",
                }
                - {
                    param_key
                    for param_key, param_value in parameters.items()
                    if (
                        get_origin(param_value.annotation) is Annotated
                        and len(args := get_args(param_value.annotation)) == 2
                        and isinstance(args[1], Depends)
                    )
                }
            )

            if len(parameter_keys) != 1:
                raise ValueError("Worker functions must accept exactly one argument")
            body_param_key = parameter_keys.pop()
            body_param = parameters[body_param_key]

            async def _on_message(message: AbstractIncomingMessage) -> None:
                track_ids = tuple(
                    json.loads(cast(str, message.headers.get("x-outbox-track-ids", "[]")))
                )
                token = _track_ids.set(track_ids)

                nonlocal retry_limit
                retry_limit = (
                    cast(int | None, message.headers.get("x-outbox-retry-limit"))
                    or retry_limit
                    or self.retry_limit
                )
                if retry_limit is not None:
                    retry_limit = int(retry_limit)

                attempt_count = cast(str | None, message.headers.get("x-delivery-count"))
                if attempt_count is not None:
                    attempt_count = int(attempt_count)

                if (
                    retry_limit is not None
                    and attempt_count is not None
                    and attempt_count >= retry_limit
                ):
                    logger.warning(
                        f"Message {message.routing_key} with track IDs {track_ids} "
                        f"exceeded retry limit {retry_limit}, rejecting"
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
                        f"Failed to deserialize mesage body {routing_key=}, {track_ids=}, "
                        f"{body=}, {exc=}"
                    )
                    raise
                else:
                    logger.info(f"Procesing message {routing_key=}, {track_ids=}, {body=}")

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

                generators, async_generators = [], []
                for param_key, param_value in parameters.items():
                    annotation = param_value.annotation
                    if not (
                        get_origin(annotation) is Annotated
                        and len(args := get_args(annotation)) == 2
                        and isinstance(args[1], Depends)
                    ):
                        continue
                    dependency_func = args[1].func
                    if inspect.isgeneratorfunction(dependency_func):
                        generator = dependency_func()
                        generators.append(generator)
                        kwargs[param_key] = next(generator)
                    elif inspect.isasyncgenfunction(dependency_func):
                        generator = dependency_func()
                        async_generators.append(generator)
                        kwargs[param_key] = await anext(generator)
                    elif inspect.iscoroutinefunction(dependency_func):
                        kwargs[param_key] = await dependency_func()
                    else:
                        kwargs[param_key] = dependency_func()

                try:
                    await func(**kwargs)
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
                        f"{'Retrying' if retry_on_error else 'Aborting'} {routing_key=}, "
                        f"{track_ids=}, {body=}"
                    )
                    if retry_on_error:
                        await message.nack(requeue=True)
                    else:
                        await message.ack()
                else:
                    logger.info(f"Sucess {routing_key=}, {track_ids=}, {body=}")
                    await message.ack()
                finally:
                    for generator in generators:
                        list(generator)
                    for async_generator in async_generators:
                        async for _ in async_generator:
                            pass
                _track_ids.reset(token)

            async def _handler(message: AbstractIncomingMessage) -> None:
                if self._shutdown_future is not None and self._shutdown_future.done():
                    await message.nack(requeue=True)
                    return
                task = asyncio.create_task(_on_message(message))
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)

            logger.debug(f"Registering listener for {binding_key=}: {func=}")
            self._listeners.append(Listener(queue_name, binding_key, _handler, tags))

        return decorator

    async def worker(self, tags: set[str] | None = None) -> None:
        await self._set_up_queues()

        self._shutdown_future = asyncio.Future()
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGINT, self._shutdown_future.set_result, None)
        loop.add_signal_handler(signal.SIGTERM, self._shutdown_future.set_result, None)

        logger.info(f"Starting worker on exchange: {self.exchange_name} ...")
        for listener in self._listeners:
            if tags is not None and not (tags & listener.tags):
                continue
            assert listener.queue is not None
            listener.consumer_tag = await listener.queue.consume(listener.handler)

        await self._shutdown_future

        logger.info("Received shutdown signal, waiting or ongoing tasks and exiting...")

        for listener in self._listeners:
            if listener.consumer_tag is None:
                continue
            assert listener.queue is not None
            assert listener.consumer_tag is not None
            await listener.queue.cancel(listener.consumer_tag)

        if self._tasks:
            await asyncio.wait(self._tasks)

    async def _set_up_queues(self):
        rmq_connection = await self._get_rmq_connection()
        if rmq_connection is None:
            raise ValueError("RabbitMQ connection is not set up.")

        channel = await rmq_connection.channel()
        exchange = await channel.declare_exchange(
            self.exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
        )
        dead_letter_exchange = await channel.declare_exchange(
            f"dlx_{self.exchange_name}", aio_pika.ExchangeType.DIRECT, durable=True
        )
        for listener in self._listeners:
            dead_letter_queue = await channel.declare_queue(
                f"dlq_{listener.queue_name}", durable=True, arguments={"x-queue-type": "quorum"}
            )
            await dead_letter_queue.bind(dead_letter_exchange, listener.queue_name)

            logger.debug(
                f"Binding queue {listener.queue_name} to exchange {self.exchange_name} with "
                f"binding key {listener.binding_key}"
            )
            listener.queue = await channel.declare_queue(
                listener.queue_name,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": f"dlx_{self.exchange_name}",
                    "x-dead-letter-routing-key": listener.queue_name,
                    "x-queue-type": "quorum",
                },
            )
            await listener.queue.bind(exchange, listener.binding_key)

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
listen = outbox.listen
worker = outbox.worker
tracking = outbox.tracking
get_track_ids = outbox.get_track_ids
