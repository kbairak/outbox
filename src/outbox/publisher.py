from __future__ import annotations

import datetime
import json
import uuid
from collections.abc import Awaitable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast, overload

import asyncpg
from aio_pika.abc import DateType
from aio_pika.message import encode_expiration
from psycopg2.extensions import connection as Psycopg2Connection
from psycopg2.extensions import cursor as Psycopg2Cursor
from pydantic import BaseModel

from .log import logger
from .utils import get_tracking_ids

# Optional SQLAlchemy support
try:
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy.orm import Session
except ImportError:
    if not TYPE_CHECKING:
        AsyncSession = type(None)  # type: ignore[misc, assignment]
        Session = type(None)  # type: ignore[misc, assignment]


@dataclass
class OutboxMessage:
    routing_key: str
    body: Any
    expiration: DateType = None
    eta: DateType | None = None

    def to_sql_params(
        self, now: datetime.datetime
    ) -> tuple[str, bytes, str, datetime.datetime, datetime.datetime, datetime.timedelta | None]:
        """Convert message to SQL parameters for insert.

        Returns: (routing_key, body_bytes, tracking_ids_json, created_at, send_after, expiration)
        """
        # Serialize body
        try:
            if isinstance(self.body, BaseModel):
                body_bytes = self.body.model_dump_json().encode()
            elif isinstance(self.body, bytes):
                body_bytes = self.body
            else:
                body_bytes = json.dumps(self.body).encode()
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Cannot serialize message body for routing_key={self.routing_key!r}: "
                f"{type(exc).__name__}: {exc}"
            ) from exc

        # Calculate expiration
        expiration_td: datetime.timedelta | None = None
        if self.expiration is not None:
            milliseconds = encode_expiration(self.expiration)
            if milliseconds is None:
                raise ValueError(
                    f"Invalid expiration value: encode_expiration returned None for "
                    f"{self.expiration!r}"
                )
            expiration_td = datetime.timedelta(milliseconds=int(milliseconds))

        # Calculate send_after
        send_after = now
        if self.eta is not None:
            milliseconds = encode_expiration(self.eta)
            if milliseconds is None:
                raise ValueError(
                    f"Invalid eta value: encode_expiration returned None for {self.eta!r}"
                )
            send_after = now + datetime.timedelta(milliseconds=int(milliseconds))

        # Tracking IDs
        tracking_ids = list(get_tracking_ids() + (str(uuid.uuid4()),))
        tracking_ids_json = json.dumps(tracking_ids)

        return (
            self.routing_key,
            body_bytes,
            tracking_ids_json,
            now,
            send_after,
            expiration_td,
        )


@dataclass
class Publisher:
    expiration: DateType | None = None
    table_name: str = "outbox_table"

    async def _bulk_publish_to_connection_async(
        self, connection: asyncpg.Connection, messages: Sequence[OutboxMessage]
    ) -> None:
        now = datetime.datetime.now(datetime.timezone.utc)
        await connection.executemany(
            f""" INSERT INTO {self.table_name}
                 (routing_key, body, tracking_ids, created_at, send_after, expiration)
                 VALUES ($1, $2, $3, $4, $5, $6)""",
            (message.to_sql_params(now) for message in messages),
        )
        logger.info(f"Published {len(messages)} messages to outbox")

    async def bulk_publish_async(
        self,
        handle: AsyncSession | asyncpg.Connection,
        messages: Sequence[OutboxMessage],
    ) -> None:
        # Extract raw connection
        if isinstance(handle, asyncpg.Connection):
            connection = handle
        elif isinstance(handle, AsyncSession):
            # Extract raw asyncpg connection from SQLAlchemy session
            sqla_conn = await handle.connection()
            raw_conn = await sqla_conn.get_raw_connection()
            # SQLAlchemy wraps asyncpg connection - unwrap it
            connection = cast(asyncpg.Connection, raw_conn._connection)  # type: ignore[attr-defined]
        else:
            raise TypeError(
                f"Expected asyncpg.Connection or AsyncSession, got {type(handle).__name__}"
            )

        await self._bulk_publish_to_connection_async(connection, messages)

    def _bulk_publish_to_cursor_sync(
        self, cursor: Psycopg2Cursor, messages: Sequence[OutboxMessage]
    ) -> None:
        now = datetime.datetime.now(datetime.timezone.utc)
        cursor.executemany(
            f""" INSERT INTO {self.table_name}
                 (routing_key, body, tracking_ids, created_at, send_after, expiration)
                 VALUES (%s, %s, %s, %s, %s, %s)""",
            (message.to_sql_params(now) for message in messages),
        )
        logger.info(f"Published {len(messages)} messages to outbox")

    def bulk_publish_sync(
        self,
        handle: Session | Psycopg2Connection | Psycopg2Cursor,
        messages: Sequence[OutboxMessage],
    ) -> None:
        # Extract cursor
        if isinstance(handle, Psycopg2Cursor):
            cursor = handle
        elif isinstance(handle, Psycopg2Connection):
            cursor = handle.cursor()
        elif isinstance(handle, Session):
            # Extract raw psycopg2 connection from SQLAlchemy session
            sqla_conn = handle.connection()
            raw_conn = sqla_conn.connection  # Get DBAPI connection
            cursor = cast(Psycopg2Cursor, raw_conn.cursor())
        else:
            raise TypeError(
                f"Expected Session, psycopg2 connection, or cursor, got {type(handle).__name__}"
            )

        self._bulk_publish_to_cursor_sync(cursor, messages)

    @overload
    def bulk_publish(
        self,
        handle: Session | Psycopg2Connection | Psycopg2Cursor,
        messages: Sequence[OutboxMessage],
    ) -> None: ...

    @overload
    def bulk_publish(
        self,
        handle: AsyncSession | asyncpg.Connection,
        messages: Sequence[OutboxMessage],
    ) -> Awaitable[None]: ...

    def bulk_publish(
        self,
        handle: AsyncSession | asyncpg.Connection | Session | Psycopg2Connection | Psycopg2Cursor,
        messages: Sequence[OutboxMessage],
    ) -> Awaitable[None] | None:
        # Detect async vs sync
        if isinstance(handle, (asyncpg.Connection, AsyncSession)):
            return self.bulk_publish_async(handle, messages)
        elif isinstance(handle, (Session, Psycopg2Connection, Psycopg2Cursor)):
            self.bulk_publish_sync(handle, messages)
            return None
        else:
            raise TypeError(
                f"Expected AsyncSession, asyncpg.Connection, Session, "
                f"psycopg2 connection, or cursor, got {type(handle).__name__}"
            )

    async def publish_async(
        self,
        handle: AsyncSession | asyncpg.Connection,
        routing_key: str,
        body: Any,
        *,
        expiration: DateType | None = None,
        eta: DateType | None = None,
    ) -> None:
        await self.bulk_publish_async(handle, [OutboxMessage(routing_key, body, expiration, eta)])

    def publish_sync(
        self,
        handle: Session | Psycopg2Connection | Psycopg2Cursor,
        routing_key: str,
        body: Any,
        *,
        expiration: DateType | None = None,
        eta: DateType | None = None,
    ) -> None:
        self.bulk_publish_sync(handle, [OutboxMessage(routing_key, body, expiration, eta)])

    @overload
    def publish(
        self,
        handle: Session | Psycopg2Connection | Psycopg2Cursor,
        routing_key: str,
        body: Any,
        *,
        expiration: DateType | None = None,
        eta: DateType | None = None,
    ) -> None: ...

    @overload
    def publish(
        self,
        handle: AsyncSession | asyncpg.Connection,
        routing_key: str,
        body: Any,
        *,
        expiration: DateType | None = None,
        eta: DateType | None = None,
    ) -> Awaitable[None]: ...

    def publish(
        self,
        handle: AsyncSession | asyncpg.Connection | Session | Psycopg2Connection | Psycopg2Cursor,
        routing_key: str,
        body: Any,
        *,
        expiration: DateType | None = None,
        eta: DateType | None = None,
    ) -> Awaitable[None] | None:
        # Detect async vs sync
        if isinstance(handle, (asyncpg.Connection, AsyncSession)):
            return self.publish_async(handle, routing_key, body, expiration=expiration, eta=eta)
        elif isinstance(handle, (Session, Psycopg2Connection, Psycopg2Cursor)):
            self.publish_sync(handle, routing_key, body, expiration=expiration, eta=eta)
            return None
        else:
            raise TypeError(
                f"Expected AsyncSession, asyncpg.Connection, Session, "
                f"psycopg2 connection, or cursor, got {type(handle).__name__}"
            )
