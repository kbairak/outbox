import asyncio
import datetime
import json
import logging
from typing import Any, Callable

import aio_pika
from aio_pika.abc import AbstractConnection
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
        default=datetime.datetime.now(datetime.UTC)
    )
    sent_at: Mapped[datetime.datetime | None] = mapped_column()


class Outbox:
    def __init__(self, **kwargs):
        self.db_engine: AsyncEngine | None = None
        self.rmq_connection: AbstractConnection | None = None
        self.exchange_name = "outbox_exchange"
        OutboxTable.__tablename__ = "outbox_table"
        self.queues = {}

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

    def listen(self, routing_key: str):
        def decorator(func: Callable):
            self.queues.setdefault(routing_key, []).append(func)

        return decorator

    async def worker(self):
        pass


outbox = Outbox()
setup_sync = outbox.setup_sync
setup_async = outbox.setup_async
emit = outbox.emit
message_relay = outbox.message_relay
listen = outbox.listen
worker = outbox.worker
