import asyncio
import datetime
import inspect
import json
import logging
from typing import Any, Callable, Coroutine

import aio_pika
from aio_pika.abc import AbstractConnection, AbstractIncomingMessage, DateType
from pydantic import BaseModel
from sqlalchemy import DateTime, Text, func, select
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
    body: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    sent_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))


class Retry(Exception):
    pass


class Abort(Exception):
    pass


class Reject(Exception):
    pass


class Outbox:
    def __init__(self):
        self.db_engine: AsyncEngine | None = None
        self.rmq_connection: AbstractConnection | None = None
        self.exchange_name = "outbox_exchange"
        self.poll_interval = 1.0
        self.retry_on_error = True
        self.expiration = None
        OutboxTable.__tablename__ = "outbox_table"
        self._reset_listeners()

    async def setup(
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
    ):
        if db_engine is not None and db_engine_url is not None:
            raise ValueError("You cannot set both db_engine and db_engine_url")
        if db_engine is not None:
            self.db_engine = db_engine
            logger.debug("Set up DB engine")
        if db_engine_url is not None:
            self.db_engine = create_async_engine(db_engine_url)
            logger.debug("Set up DB engine")

        if table_name is not None:
            OutboxTable.__tablename__ = table_name

        if self.db_engine:
            async with self.db_engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            logger.debug("Created outbox table in the database")

        if rmq_connection is not None and rmq_connection_url is not None:
            raise ValueError("You cannot set both rmq_connection and rmq_connection_url")
        if rmq_connection is not None:
            self.rmq_connection = rmq_connection
            logger.debug("Set up RMQ connection")
        if rmq_connection_url is not None:
            self.rmq_connection = await aio_pika.connect(rmq_connection_url)
            logger.debug("Set up RMQ connection")

        if exchange_name is not None:
            self.exchange_name = exchange_name
            logger.debug(f"Set up non-deault exchange name: {self.exchange_name!r}")

        if poll_interval is not None:
            self.poll_interval = poll_interval
            logger.debug(f"Set up non-deault poll interval: {self.poll_interval}")

        if retry_on_error is not None:
            self.retry_on_error = retry_on_error
            logger.debug(f"Set up non-default retry_on_error: {self.retry_on_error}")

        if expiration is not None:
            self.expiration = expiration

    def _reset_listeners(self):
        self.listeners = {}

    async def emit(
        self,
        session: AsyncSession,
        routing_key: str,
        body: Any,
        *,
        commit: bool = False,
    ) -> None:
        if isinstance(body, BaseModel):
            body = body.model_dump_json()
        else:
            body = json.dumps(body)

        session.add(OutboxTable(routing_key=routing_key, body=body))
        if commit:
            await session.commit()

        logger.debug(f"Emitted message to outbox: {routing_key=}, {body=}")

    async def message_relay(self):
        if self.db_engine is None:
            raise ValueError("Database engine is not set up.")
        if self.rmq_connection is None:
            raise ValueError("RabbitMQ connection is not set up.")

        logger.info(f"Starting message relay on exchange: {self.exchange_name} ...")
        channel = await self.rmq_connection.channel()
        exchange = await channel.declare_exchange(
            self.exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
        )
        while True:
            while True:
                async with AsyncSession(self.db_engine) as session, session.begin():
                    logger.debug("Checking for unsent messages...")
                    stmt = (
                        select(OutboxTable)
                        .where(OutboxTable.sent_at.is_(None))
                        .order_by(OutboxTable.created_at)
                        .limit(1)
                        .with_for_update(skip_locked=True)
                    )
                    try:
                        message = (await session.execute(stmt)).scalars().one()
                    except NoResultFound:
                        logger.debug("No unsent messages found, waiting...")
                        break
                    else:
                        logger.debug(f"Processing message: {message=}")
                        message.sent_at = datetime.datetime.now(datetime.UTC)
                        await exchange.publish(
                            aio_pika.Message(
                                message.body.encode(),
                                content_type="application/json",
                                expiration=self.expiration,
                            ),
                            message.routing_key,
                        )
                        logger.debug(f"Sent message: {message=} to RabbitMQ")
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
            signature = inspect.signature(func)
            parameters = dict(signature.parameters.items())

            # lets find the (hopefully at most one) parameter that has a string annotation
            str_parameters = [
                param_name for param_name, param in parameters.items() if param.annotation is str
            ]
            if len(str_parameters) == 1:
                routing_key_arg = str_parameters[0]
                del parameters[routing_key_arg]
            elif len(str_parameters) > 1:
                raise ValueError(
                    "Worker functions can have at most one parameter with a string annotation"
                )
            else:
                routing_key_arg = None

            if not len(parameters) == 1:
                raise ValueError("Worker functions must accept exactly one argument")
            body_arg = list(parameters.keys())[0]
            annotation = parameters[body_arg].annotation

            async def _on_message(message: AbstractIncomingMessage) -> None:
                logger.debug(
                    f"Received message: {message=}, {message.routing_key=}, {message.body=}"
                )
                try:
                    kwargs = {}
                    if issubclass(annotation, BaseModel):
                        kwargs[body_arg] = annotation.model_validate_json(message.body)
                    else:
                        kwargs[body_arg] = json.loads(message.body)
                    if routing_key_arg is not None:
                        kwargs[routing_key_arg] = message.routing_key
                    await func(**kwargs)
                except Retry:
                    logger.info(
                        f"Retrying (forced): {message=}, {message.routing_key=}, {message.body=}"
                    )
                    await message.nack(requeue=True)
                except Abort:
                    logger.info(
                        f"Aborting (forced): {message=}, {message.routing_key=}, {message.body=}"
                    )
                    await message.ack()
                except Reject:
                    logger.info(
                        f"Rejecting, this message will likely end up in DLX: {message=}, "
                        f"{message.routing_key=}, {message.body=}"
                    )
                    await message.nack(requeue=False)
                except Exception:
                    logger.info(
                        f"{'Retrying' if retry_on_error else 'Aborting'}: {message=}, "
                        f"{message.routing_key=}, {message.body=}"
                    )
                    if retry_on_error:
                        await message.nack(requeue=True)
                    else:
                        await message.ack()
                else:
                    await message.ack()
                    logger.info(f"Sucess: {message=}, {message.routing_key=}, {message.body=}")

            logger.debug(f"Registering listener for {binding_key=}: {func=}")
            self.listeners.setdefault(binding_key, []).append((queue_name, _on_message))

        return decorator

    async def worker(self):
        if self.rmq_connection is None:
            raise ValueError("RabbitMQ connection is not set up.")

        logger.info(f"Starting worker on exchange: {self.exchange_name} ...")
        channel = await self.rmq_connection.channel()
        exchange = await channel.declare_exchange(
            self.exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
        )
        dead_letter_exchange = await channel.declare_exchange(
            f"dlx_{self.exchange_name}", aio_pika.ExchangeType.DIRECT, durable=True
        )
        for binding_key, handlers in self.listeners.items():
            for queue_name, handler in handlers:
                dead_letter_queue = await channel.declare_queue(f"dlx_{queue_name}", durable=True)
                await dead_letter_queue.bind(dead_letter_exchange, queue_name)

                logger.debug(
                    f"Binding queue {queue_name} to exchange {self.exchange_name} with binding "
                    f"key {binding_key}"
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

                await queue.consume(handler)
        await asyncio.Future()


outbox = Outbox()
setup = outbox.setup
emit = outbox.emit
message_relay = outbox.message_relay
listen = outbox.listen
worker = outbox.worker
