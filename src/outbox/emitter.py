import datetime
import json
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Awaitable, Optional, Union, overload

from aio_pika.abc import DateType
from aio_pika.message import encode_expiration
from pydantic import BaseModel
from sqlalchemy import Engine, insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import Session

from .database import OutboxTable
from .log import logger
from .utils import ensure_database_async, ensure_database_sync, get_tracking_ids


@dataclass
class OutboxMessage:
    routing_key: str
    body: Any
    expiration: DateType = None
    eta: Optional[DateType] = None


@dataclass
class Emitter:
    db_engine: Optional[Union[AsyncEngine, Engine]] = None
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

    async def bulk_emit_async(
        self, session: AsyncSession, messages: Sequence[OutboxMessage]
    ) -> None:
        if not isinstance(self.db_engine, AsyncEngine):
            raise ValueError("db_engine must be an AsyncEngine for async bulk_emit")

        if self.auto_create_table and self.db_engine is not None:
            await ensure_database_async(self.db_engine)
        rows = []
        now = datetime.datetime.now(datetime.timezone.utc)
        for message in messages:
            try:
                if isinstance(message.body, BaseModel):
                    body = message.body.model_dump_json().encode()
                elif not isinstance(message.body, bytes):
                    body = json.dumps(message.body).encode()
                else:
                    body = message.body
            except (TypeError, ValueError) as exc:
                # Don't log - user called emit(), they'll see the exception
                raise ValueError(
                    f"Cannot serialize message body for routing_key={message.routing_key!r}: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            if message.expiration is not None:
                milliseconds = encode_expiration(message.expiration)
                if milliseconds is None:
                    raise ValueError(
                        "Invalid expiration value: encode_expiration returned None for "
                        f"{message.expiration!r}"
                    )
                expiration = datetime.timedelta(milliseconds=int(milliseconds))
            else:
                expiration = None
            if message.eta is not None:
                milliseconds = encode_expiration(message.eta)
                if milliseconds is None:
                    raise ValueError(
                        f"Invalid eta value: encode_expiration returned None for {message.eta!r}"
                    )
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

    def bulk_emit_sync(self, session: Session, messages: Sequence[OutboxMessage]) -> None:
        if not isinstance(self.db_engine, Engine):
            raise ValueError("db_engine must be an Engine for sync bulk_emit")

        if self.auto_create_table and self.db_engine is not None:
            ensure_database_sync(self.db_engine)
        rows = []
        now = datetime.datetime.now(datetime.timezone.utc)
        for message in messages:
            try:
                if isinstance(message.body, BaseModel):
                    body = message.body.model_dump_json().encode()
                elif not isinstance(message.body, bytes):
                    body = json.dumps(message.body).encode()
                else:
                    body = message.body
            except (TypeError, ValueError) as exc:
                # Don't log - user called emit(), they'll see the exception
                raise ValueError(
                    f"Cannot serialize message body for routing_key={message.routing_key!r}: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            if message.expiration is not None:
                milliseconds = encode_expiration(message.expiration)
                if milliseconds is None:
                    raise ValueError(
                        f"Invalid expiration value: encode_expiration returned None for {message.expiration!r}"
                    )
                expiration = datetime.timedelta(milliseconds=int(milliseconds))
            else:
                expiration = None
            if message.eta is not None:
                milliseconds = encode_expiration(message.eta)
                if milliseconds is None:
                    raise ValueError(
                        f"Invalid eta value: encode_expiration returned None for {message.eta!r}"
                    )
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
        session.execute(insert(OutboxTable).values(rows))
        logger.info(f"Emitted {len(rows)} messages to outbox")

    @overload
    def bulk_emit(self, session: Session, messages: Sequence[OutboxMessage]) -> None: ...

    @overload
    def bulk_emit(
        self, session: AsyncSession, messages: Sequence[OutboxMessage]
    ) -> Awaitable[None]: ...

    def bulk_emit(
        self, session: Union[Session, AsyncSession], messages: Sequence[OutboxMessage]
    ) -> Optional[Awaitable[None]]:
        if isinstance(session, Session):
            self.bulk_emit_sync(session, messages)
            return None
        elif isinstance(session, AsyncSession):
            return self.bulk_emit_async(session, messages)
        else:  # pragma: no cover
            raise Exception("Unreachable code")

    async def emit_async(
        self,
        session: AsyncSession,
        routing_key: str,
        body: Any,
        *,
        expiration: Optional[DateType] = None,
        eta: Optional[DateType] = None,
    ) -> None:
        await self.bulk_emit_async(session, [OutboxMessage(routing_key, body, expiration, eta)])

    def emit_sync(
        self,
        session: Session,
        routing_key: str,
        body: Any,
        *,
        expiration: Optional[DateType] = None,
        eta: Optional[DateType] = None,
    ) -> None:
        self.bulk_emit_sync(session, [OutboxMessage(routing_key, body, expiration, eta)])

    @overload
    def emit(
        self,
        session: Session,
        routing_key: str,
        body: Any,
        *,
        expiration: Optional[DateType] = None,
        eta: Optional[DateType] = None,
    ) -> None: ...

    @overload
    def emit(
        self,
        session: AsyncSession,
        routing_key: str,
        body: Any,
        *,
        expiration: Optional[DateType] = None,
        eta: Optional[DateType] = None,
    ) -> Awaitable[None]: ...

    def emit(
        self,
        session: Union[Session, AsyncSession],
        routing_key: str,
        body: Any,
        *,
        expiration: Optional[DateType] = None,
        eta: Optional[DateType] = None,
    ) -> Optional[Awaitable[None]]:
        if isinstance(session, Session):
            self.emit_sync(session, routing_key, body, expiration=expiration, eta=eta)
            return None
        elif isinstance(session, AsyncSession):
            return self.emit_async(session, routing_key, body, expiration=expiration, eta=eta)
        else:  # pragma: no cover
            raise Exception("Unreachable code")
