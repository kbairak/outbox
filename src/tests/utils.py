import asyncio
from typing import Any, Protocol

from aio_pika.abc import DateType
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession


class Person(BaseModel):
    name: str


class EmitType(Protocol):
    async def __call__(
        self,
        session: AsyncSession,
        routing_key: str,
        body: Any,
        *,
        retry_limit: int | None = None,
        expiration: DateType = None,
        eta: DateType | None = None,
    ) -> None: ...


async def run_worker(outbox, listeners, timeout):
    try:
        await asyncio.wait_for(outbox.worker(listeners), timeout=timeout)
    except asyncio.TimeoutError:
        pass


async def get_dlq_message_count(outbox, queue_name):
    connection = await outbox._get_rmq_connection()
    assert connection is not None
    channel = await connection.channel()
    dlq = await channel.get_queue(f"{queue_name}.dlq")
    return dlq.declaration_result.message_count
