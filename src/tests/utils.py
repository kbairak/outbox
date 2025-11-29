import asyncio
from collections.abc import Sequence
from typing import Any, Awaitable, Optional, Protocol

from aio_pika.abc import AbstractConnection, DateType
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from outbox import Consumer, Worker


class Person(BaseModel):
    name: str


class PublishType(Protocol):
    def __call__(
        self,
        session: AsyncSession,
        routing_key: str,
        body: Any,
        *,
        expiration: Optional[DateType] = None,
        eta: Optional[DateType] = None,
    ) -> Awaitable[None]: ...


async def run_worker(worker: Worker, consumers: Sequence[Consumer], timeout: float) -> None:
    prev_consumers = worker.consumers
    worker.consumers = consumers
    try:
        await asyncio.wait_for(worker.run(), timeout=timeout)
    except asyncio.TimeoutError:
        pass
    worker.consumers = prev_consumers


async def get_dlq_message_count(rmq_connection: AbstractConnection, queue_name: str) -> int:
    connection = rmq_connection
    assert connection is not None
    channel = await connection.channel()
    dlq = await channel.get_queue(f"{queue_name}.dlq")
    message_count = dlq.declaration_result.message_count
    assert message_count is not None
    return message_count
