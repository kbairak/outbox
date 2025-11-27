import asyncio
from collections.abc import Sequence
from typing import Any, Optional, Protocol

from aio_pika.abc import DateType
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from outbox import Listener, Outbox


class Person(BaseModel):
    name: str


class EmitType(Protocol):
    async def __call__(
        self,
        session: AsyncSession,
        routing_key: str,
        body: Any,
        *,
        expiration: DateType = None,
        eta: Optional[DateType] = None,
    ) -> None: ...


async def run_worker(outbox: Outbox, listeners: Sequence[Listener], timeout: float) -> None:
    try:
        await asyncio.wait_for(outbox.worker(listeners), timeout=timeout)
    except asyncio.TimeoutError:
        pass


async def get_dlq_message_count(outbox: Outbox, queue_name: str) -> int:
    connection = await outbox._get_rmq_connection()
    assert connection is not None
    channel = await connection.channel()
    dlq = await channel.get_queue(f"{queue_name}.dlq")
    message_count = dlq.declaration_result.message_count
    assert message_count is not None
    return message_count
