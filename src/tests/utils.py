import asyncio
from typing import Any, Callable, Coroutine, Protocol

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


class ListenType(Protocol):
    def __call__(
        self,
        binding_key: str,
        queue_name: str | None = None,
        retry_limit: int | None = None,
        retry_on_error: bool | None = None,
        tags: set[str] | None = None,
    ) -> Callable[[Callable[..., Coroutine[None, None, None]]], None]: ...


async def run_worker(outbox, timeout, tags=None):
    try:
        await asyncio.wait_for(outbox.worker(tags=tags), timeout=timeout)
    except asyncio.TimeoutError:
        pass


async def get_dlq_message_count(outbox, queue_name):
    connection = await outbox._get_rmq_connection()
    assert connection is not None
    channel = await connection.channel()
    dlq = await channel.get_queue(f"dlq_{queue_name}")
    return dlq.declaration_result.message_count
