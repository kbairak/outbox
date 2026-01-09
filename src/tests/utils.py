from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Sequence
from typing import Any, Protocol, overload

import asyncpg
from aio_pika.abc import AbstractConnection, DateType
from psycopg2.extensions import connection as Psycopg2Connection
from psycopg2.extensions import cursor as Psycopg2Cursor
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from outbox import Consumer, Worker


class Person(BaseModel):
    name: str


class PublishType(Protocol):
    @overload
    def __call__(
        self,
        handle: Session | Psycopg2Connection | Psycopg2Cursor,
        routing_key: str,
        body: Any,
        *,
        expiration: DateType | None = None,
        eta: DateType | None = None,
    ) -> None: ...

    @overload
    def __call__(
        self,
        handle: AsyncSession | asyncpg.Connection,
        routing_key: str,
        body: Any,
        *,
        expiration: DateType | None = None,
        eta: DateType | None = None,
    ) -> Awaitable[None]: ...

    def __call__(
        self,
        handle: AsyncSession | asyncpg.Connection | Session | Psycopg2Connection | Psycopg2Cursor,
        routing_key: str,
        body: Any,
        *,
        expiration: DateType | None = None,
        eta: DateType | None = None,
    ) -> Awaitable[None] | None: ...


async def run_worker(worker: Worker, consumers: Sequence[Consumer], timeout: float) -> None:
    prev_consumers = worker.consumers
    worker.consumers = consumers
    with contextlib.suppress(asyncio.TimeoutError):
        await asyncio.wait_for(worker.run(), timeout=timeout)
    worker.consumers = prev_consumers


async def get_dlq_message_count(rmq_connection: AbstractConnection, queue_name: str) -> int:
    connection = rmq_connection
    assert connection is not None
    channel = await connection.channel()
    dlq = await channel.get_queue(f"{queue_name}.dlq")
    message_count = dlq.declaration_result.message_count
    assert message_count is not None
    return message_count
