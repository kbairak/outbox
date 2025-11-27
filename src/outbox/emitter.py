import datetime
import json
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Optional

from aio_pika.abc import DateType
from aio_pika.message import encode_expiration
from pydantic import BaseModel
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

from .database import OutboxTable
from .log import logger
from .utils import ensure_database, get_tracking_ids


@dataclass
class OutboxMessage:
    routing_key: str
    body: Any
    expiration: DateType = None
    eta: Optional[DateType] = None


@dataclass
class Emitter:
    db_engine: Optional[AsyncEngine] = None
    db_engine_url: Optional[str] = None
    expiration: Optional[DateType] = None
    table_name: str = "outbox_table"
    auto_create_table: bool = False

    def __post_init__(self) -> None:
        if self.db_engine is not None and self.db_engine_url is not None:
            raise ValueError("You cannot set both db_engine and db_engine_url")
        if self.db_engine_url is not None:
            self.db_engine = create_async_engine(self.db_engine_url)
        if self.table_name is not None:
            OutboxTable.__tablename__ = self.table_name

    async def bulk_emit(self, session: AsyncSession, messages: Sequence[OutboxMessage]) -> None:
        if self.auto_create_table and self.db_engine is not None:
            await ensure_database(self.db_engine)
        rows = []
        now = datetime.datetime.now(datetime.timezone.utc)
        for message in messages:
            if isinstance(message.body, BaseModel):
                body = message.body.model_dump_json().encode()
            elif not isinstance(message.body, bytes):
                body = json.dumps(message.body).encode()
            else:
                body = message.body
            if message.expiration is not None:
                milliseconds = encode_expiration(message.expiration)
                assert milliseconds is not None
                expiration = datetime.timedelta(milliseconds=int(milliseconds))
            else:
                expiration = None
            if message.eta is not None:
                milliseconds = encode_expiration(message.eta)
                assert milliseconds is not None
                send_after = now + datetime.timedelta(milliseconds=int(milliseconds))
            else:
                send_after = now
            rows.append(
                {
                    "routing_key": message.routing_key,
                    "body": body,
                    "tracking_ids": list(get_tracking_ids() + (str(uuid.uuid4()),)),
                    "created_at": now,
                    "expiration": expiration,
                    "send_after": send_after,
                }
            )
        await session.execute(insert(OutboxTable).values(rows))
        logger.info(f"Emitted {len(rows)} messages to outbox")

    async def emit(
        self,
        session: AsyncSession,
        routing_key: str,
        body: Any,
        *,
        expiration: DateType = None,
        eta: Optional[DateType] = None,
    ) -> None:
        await self.bulk_emit(session, [OutboxMessage(routing_key, body, expiration, eta)])
