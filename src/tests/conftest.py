from typing import AsyncGenerator, Generator

import aio_pika
import pytest
import pytest_asyncio
from aio_pika.abc import AbstractConnection
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import Session
from testcontainers.postgres import PostgresContainer  # type: ignore[import-untyped]
from testcontainers.rabbitmq import RabbitMqContainer  # type: ignore[import-untyped]

from outbox import Emitter, MessageRelay, Worker

from .utils import EmitType


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def db_engine() -> AsyncGenerator[AsyncEngine, None]:
    with PostgresContainer("postgres:17.5-alpine") as postgres:
        connection_url = postgres.get_connection_url().replace("psycopg2", "asyncpg")
        engine = create_async_engine(connection_url)
        yield engine
        await engine.dispose()


@pytest.fixture(scope="session")
def db_engine_sync() -> Generator[Engine, None]:
    with PostgresContainer("postgres:17.5-alpine") as postgres:
        connection_url = postgres.get_connection_url()
        engine = create_engine(connection_url)
        yield engine
        engine.dispose()


@pytest_asyncio.fixture(loop_scope="session")
async def session(db_engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSession(db_engine) as session:
        yield session


@pytest.fixture(scope="session")
def session_sync(db_engine_sync: Engine) -> Generator[Session]:
    with Session(db_engine_sync) as session:
        yield session


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def rmq_connection() -> AsyncGenerator[AbstractConnection, None]:
    with RabbitMqContainer("rabbitmq:4.1") as rabbitmq:
        connection = await aio_pika.connect(
            f"amqp://{rabbitmq.username}:{rabbitmq.password}@{rabbitmq.get_container_host_ip()}:"
            f"{rabbitmq.get_exposed_port(rabbitmq.port)}/"
        )
        async with connection:
            yield connection


@pytest.fixture
def emitter(db_engine: AsyncEngine) -> Emitter:
    return Emitter(db_engine=db_engine, auto_create_table=True)


@pytest.fixture
def emit(emitter: Emitter) -> EmitType:
    return emitter.emit_async


@pytest.fixture
def message_relay(db_engine: AsyncEngine, rmq_connection: AbstractConnection) -> MessageRelay:
    return MessageRelay(
        db_engine=db_engine,
        rmq_connection=rmq_connection,
        clean_up_after="NEVER",
        auto_create_table=True,
        enable_metrics=False,
    )


@pytest.fixture
def worker(rmq_connection: AbstractConnection) -> Worker:
    return Worker(rmq_connection=rmq_connection, enable_metrics=False)


@pytest_asyncio.fixture(autouse=True, loop_scope="session")
async def cleanup_database(db_engine: AsyncEngine) -> AsyncGenerator[None, None]:
    """Clean up the outbox table after each test to ensure test isolation"""
    yield
    async with AsyncSession(db_engine) as session:
        await session.execute(text("TRUNCATE TABLE outbox_table"))
        await session.commit()
