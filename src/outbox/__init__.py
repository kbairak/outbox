import asyncio
import datetime
import inspect
import json
import logging
import signal
from typing import Any, Callable, Coroutine, Literal

import aio_pika
from aio_pika.abc import AbstractConnection, AbstractExchange, AbstractIncomingMessage, DateType
from aio_pika.message import encode_expiration
from pydantic import BaseModel
from sqlalchemy import DateTime, Text, delete, func, select
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
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    send_after: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True))
    sent_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))
    expiration: Mapped[datetime.timedelta | None] = mapped_column()

    def __repr__(self):
        routing_key, body = self.routing_key, self.body.decode()
        return f"OutboxTable({routing_key=}, {body=})"


class Retry(Exception):
    pass


class Reject(Exception):
    pass


class Outbox:
    def __init__(self, **kwargs):
        self.db_engine: AsyncEngine | None = None
        self._rmq_connection: AbstractConnection | None = None
        self._rmq_connection_url: str | None = None
        self.exchange_name = "outbox_exchange"
        self.poll_interval = 1.0
        self.retry_on_error = True
        self.expiration = None
        self.clean_up_after = "IMMEDIATELY"
        OutboxTable.__tablename__ = "outbox_table"

        self._listeners = []
        self._table_created = False
        self._tasks = set()
        self._shutdown_future: asyncio.Future | None = None

        self.setup(**kwargs)

    def setup(
        self,
        db_engine: AsyncEngine | None = None,
        db_engine_url: str | None = None,
        table_name: str | None = None,
        rmq_connection: AbstractConnection | None = None,
        rmq_connection_url: str | None = None,
        exchange_name: str | None = None,
        poll_interval: float | None = None,
        retry_on_error: bool | None = None,
        expiration: DateType | None = None,
        clean_up_after: (
            Literal["IMMEDIATELY"] | Literal["NEVER"] | datetime.timedelta | None
        ) = None,
    ) -> None:
        if db_engine is not None and db_engine_url is not None:
            raise ValueError("You cannot set both db_engine and db_engine_url")
        if rmq_connection is not None and rmq_connection_url is not None:
            raise ValueError("You cannot set both rmq_connection and rmq_connection_url")

        if db_engine is not None:
            self.db_engine = db_engine
            logger.debug("Set up DB engine")
        if db_engine_url is not None:
            self.db_engine = create_async_engine(db_engine_url)
            logger.debug("Set up DB engine")

        if table_name is not None:
            OutboxTable.__tablename__ = table_name

        if rmq_connection is not None:
            self._rmq_connection = rmq_connection
            logger.debug("Set up RMQ connection")
        if rmq_connection_url is not None:
            self._rmq_connection_url = rmq_connection_url

        if exchange_name is not None:
            self.exchange_name = exchange_name
            logger.debug(f"Set up non-deault exchange name: {self.exchange_name}")

        if poll_interval is not None:
            self.poll_interval = poll_interval
            logger.debug(f"Set up non-deault poll interval: {self.poll_interval}")

        if retry_on_error is not None:
            self.retry_on_error = retry_on_error
            logger.debug(f"Set up non-default retry_on_error: {self.retry_on_error}")

        if expiration is not None:
            self.expiration = expiration
            logger.debug(f"Set up non-default expiration: {self.expiration}")

        if clean_up_after is not None:
            self.clean_up_after = clean_up_after
            logger.debug(f"Set up non-default clean_up_after: {self.clean_up_after}")

    async def _ensure_dataabase(self) -> None:
        if not self._table_created and self.db_engine:
            async with self.db_engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            self._table_created = True
            logger.debug("Created outbox table in the database")

    async def _get_rmq_connection(self) -> AbstractConnection | None:
        if self._rmq_connection is None and self._rmq_connection_url is not None:
            self._rmq_connection = await aio_pika.connect(self._rmq_connection_url)
            logger.debug("Set up RMQ connection")
        return self._rmq_connection

    async def emit(
        self,
        session: AsyncSession,
        routing_key: str,
        body: Any,
        *,
        expiration: DateType = None,
        eta: DateType | None = None,
    ) -> None:
        await self._ensure_dataabase()

        if isinstance(body, BaseModel):
            body = body.model_dump_json().encode()
        elif not isinstance(body, bytes):
            body = json.dumps(body).encode()

        outbox_row = OutboxTable(routing_key=routing_key, body=body)

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
            outbox_row.send_after = datetime.datetime.now(datetime.UTC)
        session.add(outbox_row)

        logger.debug(f"Emitted message to outbox: {routing_key=}, {body=}")

    async def message_relay(self) -> None:
        if self.db_engine is None:
            raise ValueError("Database engine is not set up.")
        rmq_connection = await self._get_rmq_connection()
        if rmq_connection is None:
            raise ValueError("RabbitMQ connection is not set up.")
        await self._ensure_dataabase()

        logger.info(f"Starting message relay on exchange: {self.exchange_name} ...")
        channel = await rmq_connection.channel()
        exchange = await channel.declare_exchange(
            self.exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
        )
        while True:
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
                    else:
                        logger.debug(f"Processing message: {outbox_row}")
                        expiration = outbox_row.expiration or self.expiration
                        await exchange.publish(
                            aio_pika.Message(
                                outbox_row.body,
                                content_type="application/json",
                                expiration=expiration,
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
            await asyncio.sleep(self.poll_interval)

    def listen(
        self, binding_key: str, queue_name: str | None = None, retry_on_error: bool | None = None
    ) -> Callable[[Callable[..., Coroutine[None, None, None]]], None]:
        if retry_on_error is None:
            retry_on_error = self.retry_on_error

        def decorator(func: Callable):
            nonlocal queue_name
            if queue_name is None:
                queue_name = f"{func.__module__}.{func.__qualname__}".replace("<", "").replace(
                    ">", ""
                )
            parameters = dict(inspect.signature(func).parameters.items())

            if "routing_key" in parameters:
                provide_routing_key = True
                del parameters["routing_key"]
            else:
                provide_routing_key = False

            if not len(parameters) == 1:
                raise ValueError("Worker functions must accept exactly one argument")
            body_arg = list(parameters.keys())[0]
            annotation = parameters[body_arg].annotation

            async def _on_message(message: AbstractIncomingMessage) -> None:
                try:
                    logger.debug(
                        f"Received message {message.message_id}: routing key: "
                        f"{message.routing_key}, body: {message.body}"
                    )
                    kwargs = {}

                    if provide_routing_key:
                        kwargs["routing_key"] = message.routing_key

                    if issubclass(annotation, BaseModel):
                        kwargs[body_arg] = annotation.model_validate_json(message.body)
                    elif issubclass(annotation, bytes):
                        kwargs[body_arg] = message.body
                    else:
                        kwargs[body_arg] = json.loads(message.body)

                    await func(**kwargs)
                except Retry:
                    logger.info(
                        f"Retrying (forced) {message.message_id}: routing key: "
                        f"{message.routing_key}, body: {message.body}"
                    )
                    await message.nack(requeue=True)
                except Reject:
                    logger.info(
                        f"Rejecting, this message will likely end up in DLX {message.message_id}: "
                        f"routing key: {message.routing_key}, body: {message.body}"
                    )
                    await message.nack(requeue=False)
                except Exception:
                    logger.info(
                        f"{'Retrying' if retry_on_error else 'Aborting'} {message.message_id}: "
                        f"routing key: {message.routing_key}, body: {message.body}"
                    )
                    if retry_on_error:
                        await message.nack(requeue=True)
                    else:
                        await message.ack()
                else:
                    await message.ack()
                    logger.info(
                        f"Sucess {message.message_id}: routing key: {message.routing_key}, body: "
                        f"{message.body}"
                    )

            async def _handler(message: AbstractIncomingMessage) -> None:
                if self._shutdown_future is not None and self._shutdown_future.done():
                    await message.nack(requeue=True)
                    return
                task = asyncio.create_task(_on_message(message))
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)

            logger.debug(f"Registering listener for {binding_key=}: {func=}")
            self._listeners.append((queue_name, binding_key, _handler))

        return decorator

    async def worker(self) -> None:
        rmq_connection = await self._get_rmq_connection()
        if rmq_connection is None:
            raise ValueError("RabbitMQ connection is not set up.")

        self._shutdown_future = asyncio.Future()
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGINT, self._shutdown_future.set_result, None)
        loop.add_signal_handler(signal.SIGTERM, self._shutdown_future.set_result, None)

        logger.info(f"Starting worker on exchange: {self.exchange_name} ...")
        channel = await rmq_connection.channel()
        exchange = await channel.declare_exchange(
            self.exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
        )
        dead_letter_exchange = await channel.declare_exchange(
            f"dlx_{self.exchange_name}", aio_pika.ExchangeType.DIRECT, durable=True
        )
        consumer_tags = []
        for queue_name, binding_key, handler in self._listeners:
            dead_letter_queue = await channel.declare_queue(f"dlq_{queue_name}", durable=True)
            await dead_letter_queue.bind(dead_letter_exchange, queue_name)

            logger.debug(
                f"Binding queue {queue_name} to exchange {self.exchange_name} with binding key "
                f"{binding_key}"
            )
            queue = await channel.declare_queue(
                queue_name,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": f"dlx_{self.exchange_name}",
                    "x-dead-letter-routing-key": queue_name,
                },
            )
            await queue.bind(exchange, binding_key)

            consumer_tag = await queue.consume(handler)
            consumer_tags.append((queue, consumer_tag))

        await self._shutdown_future

        logger.info("Received shutdown signal, waiting or ongoing tasks and exiting...")

        for queue, consumer_tag in consumer_tags:
            await queue.cancel(consumer_tag)

        if self._tasks:
            await asyncio.wait(self._tasks)


outbox = Outbox()
setup = outbox.setup
emit = outbox.emit
message_relay = outbox.message_relay
listen = outbox.listen
worker = outbox.worker
