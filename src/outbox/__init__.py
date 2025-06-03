import asyncio
import datetime
import functools
import inspect
import json
import logging
from typing import Any, Callable

import aio_pika
from aio_pika.abc import AbstractConnection, AbstractIncomingMessage
from pydantic import BaseModel
from sqlalchemy import Text, select
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
        default=lambda: datetime.datetime.now(datetime.UTC),
    )
    sent_at: Mapped[datetime.datetime | None] = mapped_column()


class Outbox:
    def __init__(self, **kwargs):
        self.db_engine: AsyncEngine | None = None
        self.rmq_connection: AbstractConnection | None = None
        self.exchange_name = "outbox_exchange"
        OutboxTable.__tablename__ = "outbox_table"
        self.listeners = {}

        self.setup_sync(**kwargs)

    def setup_sync(self, **kwargs):  # pragma: no cover
        if kwargs:
            asyncio.run(self.setup_async(**kwargs))

    async def setup_async(
        self,
        db_engine: AsyncEngine | None = None,
        db_engine_url: str | None = None,
        table_name: str | None = None,
        rmq_connection: AbstractConnection | None = None,
        rmq_connection_url: str | None = None,
        exchange_name: str | None = None,
    ):
        if db_engine is not None:
            if db_engine_url is not None:
                raise ValueError("You cannot set both db_engine and db_engine_url")
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

        if rmq_connection is not None:
            if rmq_connection_url is not None:
                raise ValueError("You cannot set both rmq_connection and rmq_connection_url")
            self.rmq_connection = rmq_connection
            logger.debug("Set up RMQ connection")
        if rmq_connection_url is not None:
            self.rmq_connection = await aio_pika.connect(rmq_connection_url)
            logger.debug("Set up RMQ connection")

        if exchange_name is not None:
            self.exchange_name = exchange_name

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

    async def message_relay(self, *, poll_interval: float = 1.0):
        if self.db_engine is None:
            raise ValueError("Database engine is not set up.")
        if self.rmq_connection is None:
            raise ValueError("RabbitMQ connection is not set up.")
        channel = await self.rmq_connection.channel()
        exchange = await channel.declare_exchange(
            self.exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
        )
        while True:
            async with AsyncSession(self.db_engine) as session, session.begin():
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
                    await asyncio.sleep(poll_interval)
                else:
                    logger.debug(f"Processing message: {message=}")
                    message.sent_at = datetime.datetime.now(datetime.UTC)
                    await exchange.publish(
                        aio_pika.Message(message.body.encode(), content_type="application/json"),
                        message.routing_key,
                    )

    def listen(self, binding_key: str) -> Callable[[Callable[[Any], None]], None]:
        def decorator(func: Callable):
            queue_name = f"{func.__module__}.{func.__qualname__}".replace("<", "").replace(">", "")
            signature = inspect.signature(func)
            if not len(signature.parameters) == 1:
                raise ValueError("Worker functions must accept exactly one argument")
            param_name = list(signature.parameters.keys())[0]
            param = signature.parameters[param_name]
            annotation = param.annotation

            async def _on_message(message: AbstractIncomingMessage) -> None:
                async with message.process():
                    if isinstance(annotation, BaseModel):
                        body = annotation.model_validate_json(message.body)
                    else:
                        body = json.loads(message.body)
                    await func(**{param_name: body})

            self.listeners.setdefault(binding_key, []).append((queue_name, _on_message))

        return decorator

    async def worker(self):
        if self.rmq_connection is None:
            raise ValueError("RabbitMQ connection is not set up.")
        channel = await self.rmq_connection.channel()
        exchange = await channel.declare_exchange(
            self.exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
        )
        for binding_key, handlers in self.listeners.items():
            for queue_name, handler in handlers:
                queue = await channel.declare_queue(queue_name, durable=True)
                await queue.bind(exchange, binding_key)
                await queue.consume(handler)
        await asyncio.Future()


outbox = Outbox()
setup_sync = outbox.setup_sync
setup_async = outbox.setup_async
emit = outbox.emit
message_relay = outbox.message_relay
listen = outbox.listen
worker = outbox.worker
