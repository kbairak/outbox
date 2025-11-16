from typing import AsyncGenerator

import aio_pika
import pytest
import pytest_asyncio
from aio_pika.abc import AbstractConnection
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from testcontainers.rabbitmq import RabbitMqContainer

from outbox import Outbox

from .utils import EmitType


@pytest.fixture
def db_engine() -> AsyncEngine:
    return create_async_engine("sqlite+aiosqlite:///:memory:")


@pytest_asyncio.fixture(loop_scope="session")
async def session(db_engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSession(db_engine) as session:
        yield session


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def rmq_connection():
    with RabbitMqContainer("rabbitmq:4.1") as rabbitmq:
        connection = await aio_pika.connect(
            f"amqp://{rabbitmq.username}:{rabbitmq.password}@{rabbitmq.get_container_host_ip()}:"
            f"{rabbitmq.get_exposed_port(rabbitmq.port)}/"
        )
        async with connection:
            yield connection


@pytest_asyncio.fixture(loop_scope="session")
async def outbox(db_engine: AsyncEngine, rmq_connection: AbstractConnection) -> Outbox:
    outbox = Outbox(db_engine=db_engine, rmq_connection=rmq_connection, clean_up_after="NEVER")
    return outbox


@pytest.fixture
def emit(outbox: Outbox) -> EmitType:
    return outbox.emit
