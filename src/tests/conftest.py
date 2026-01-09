from collections.abc import AsyncGenerator, Generator

import aio_pika
import asyncpg
import psycopg2
import pytest
import pytest_asyncio
from aio_pika.abc import AbstractConnection
from psycopg2.extensions import connection as Psycopg2Connection
from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import Session
from testcontainers.postgres import PostgresContainer  # type: ignore[import-untyped]
from testcontainers.rabbitmq import RabbitMqContainer  # type: ignore[import-untyped]

from outbox import MessageRelay, Publisher, Worker
from outbox.utils import ensure_outbox_table_async, ensure_outbox_table_sync

from .utils import PublishType


@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer]:
    with PostgresContainer("postgres:17.5-alpine") as postgres:
        yield postgres


@pytest.fixture
def db_url_async(postgres_container: PostgresContainer) -> str:
    return postgres_container.get_connection_url(driver="asyncpg") or ""


@pytest.fixture
def db_url_sync(postgres_container: PostgresContainer) -> str:
    return postgres_container.get_connection_url() or ""


@pytest_asyncio.fixture
async def db_engine_async(db_url_async: str) -> AsyncEngine:
    # Convert SQLAlchemy URL format to asyncpg format for table creation
    url = db_url_async.replace("postgresql+asyncpg://", "postgresql://")
    await ensure_outbox_table_async(url)
    # Create SQLAlchemy engine for backward compatible tests
    engine = create_async_engine(db_url_async)
    return engine


@pytest.fixture
def db_engine_sync(db_url_sync: str) -> Engine:
    # Convert SQLAlchemy URL format to psycopg2 format for table creation
    url = db_url_sync.replace("postgresql+psycopg2://", "postgresql://")
    ensure_outbox_table_sync(url)
    # Create SQLAlchemy engine for backward compatible tests
    engine = create_engine(db_url_sync)
    return engine


@pytest_asyncio.fixture
async def session_async(db_engine_async: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSession(db_engine_async) as session:
        yield session


@pytest.fixture
def session_sync(db_engine_sync: Engine) -> Generator[Session]:
    with Session(db_engine_sync) as session:
        yield session


@pytest_asyncio.fixture
async def db_connection_async(db_url_async: str) -> AsyncGenerator[asyncpg.Connection, None]:
    # Convert SQLAlchemy URL format to asyncpg format
    url = db_url_async.replace("postgresql+asyncpg://", "postgresql://")
    await ensure_outbox_table_async(url)
    conn = await asyncpg.connect(url)
    try:
        yield conn
    finally:
        await conn.close()


@pytest.fixture
def db_connection_sync(
    postgres_container: PostgresContainer, db_url_sync: str
) -> Generator[Psycopg2Connection]:
    # Ensure the outbox table exists
    url = db_url_sync.replace("postgresql+psycopg2://", "postgresql://")
    ensure_outbox_table_sync(url)

    with psycopg2.connect(
        host=postgres_container.get_container_host_ip(),
        port=postgres_container.get_exposed_port(postgres_container.port),
        dbname=postgres_container.dbname,
        user=postgres_container.username,
        password=postgres_container.password,
    ) as connection:
        yield connection


@pytest_asyncio.fixture(scope="session")
async def rmq_connection() -> AsyncGenerator[AbstractConnection, None]:
    with RabbitMqContainer("rabbitmq:4.1") as rabbitmq:
        connection = await aio_pika.connect(
            f"amqp://{rabbitmq.username}:{rabbitmq.password}@{rabbitmq.get_container_host_ip()}:"
            f"{rabbitmq.get_exposed_port(rabbitmq.port)}/"
        )
        async with connection:
            yield connection


@pytest.fixture
def publisher() -> Publisher:
    return Publisher()


@pytest.fixture
def publish(publisher: Publisher) -> PublishType:
    return publisher.publish


@pytest.fixture
def message_relay(db_url_async: str, rmq_connection: AbstractConnection) -> MessageRelay:
    # Convert SQLAlchemy URL format to asyncpg format
    url = db_url_async.replace("postgresql+asyncpg://", "postgresql://")
    return MessageRelay(
        db_engine_url=url,
        rmq_connection=rmq_connection,
        clean_up_after="NEVER",
        enable_metrics=False,
    )


@pytest.fixture
def worker(rmq_connection: AbstractConnection) -> Worker:
    return Worker(rmq_connection=rmq_connection, enable_metrics=False)


@pytest_asyncio.fixture(autouse=True)
async def cleanup_database(db_url_async: str) -> AsyncGenerator[None, None]:
    """Clean up the outbox table after each test to ensure test isolation"""
    yield
    try:
        # Convert SQLAlchemy URL format to asyncpg format
        url = db_url_async.replace("postgresql+asyncpg://", "postgresql://")
        conn = await asyncpg.connect(url)
        try:
            await conn.execute("TRUNCATE TABLE outbox_table")
        finally:
            await conn.close()
    except Exception:
        pass
