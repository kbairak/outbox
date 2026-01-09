import re
import reprlib
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

import aio_pika
import asyncpg
import psycopg2
from aio_pika.abc import AbstractConnection
from psycopg2.extensions import connection as Psycopg2Connection

from outbox.log import logger


class Reject(Exception):
    pass


# Configure reprlib for intelligent body truncation
_body_repr = reprlib.Repr()
_body_repr.maxstring = 100  # Truncate strings at 100 chars
_body_repr.maxother = 200  # Truncate other reprs at 200 chars
_body_repr.maxlist = 5  # Show first 5 list items
_body_repr.maxdict = 5  # Show first 5 dict items


def truncate_body(body: Any) -> str:
    """Truncate body using reprlib to preserve structure."""
    return _body_repr.repr(body)


async def get_rmq_connection(rmq_connection_url: str) -> AbstractConnection:
    try:
        return await aio_pika.connect_robust(rmq_connection_url)
    except Exception as exc:
        raise ValueError(
            f"Failed to connect to RabbitMQ at '{rmq_connection_url}': {exc}\n"
            "Example: rmq_connection_url='amqp://guest:guest@localhost/'"
        ) from exc


_table_created: set[str] = set()


DDL_STATEMENTS = [
    """ CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            routing_key TEXT NOT NULL,
            body BYTEA NOT NULL,
            tracking_ids JSONB NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            expiration INTERVAL,
            send_after TIMESTAMP WITH TIME ZONE NOT NULL,
            sent_at TIMESTAMP WITH TIME ZONE
        )""",
    """ CREATE INDEX IF NOT EXISTS outbox_pending_idx
        ON {table_name} (send_after, created_at)
        WHERE sent_at IS NULL""",
    """ CREATE INDEX IF NOT EXISTS outbox_cleanup_idx
        ON {table_name} (sent_at)
        WHERE sent_at IS NOT NULL""",
    """ CREATE OR REPLACE FUNCTION notify_outbox_insert() RETURNS TRIGGER AS $$
        BEGIN
            IF NEW.send_after <= NOW() THEN
                PERFORM pg_notify('outbox_channel', '');
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql""",
    "DROP TRIGGER IF EXISTS outbox_notify_trigger ON {table_name}",
    """ CREATE TRIGGER outbox_notify_trigger
        AFTER INSERT ON {table_name}
        FOR EACH ROW
        EXECUTE FUNCTION notify_outbox_insert()""",
]


def ensure_outbox_table_sync(
    db: str | Psycopg2Connection, table_name: str = "outbox_table"
) -> None:
    """Ensure outbox table exists with proper schema and triggers.

    Args:
        db: Either a PostgreSQL connection URL string (e.g., "postgresql://user:pass@host/db")
            or an existing psycopg2 connection object
    """
    # Determine if we need to create/close connection
    if isinstance(db, str):
        cache_key = db
        should_close = True
        conn = psycopg2.connect(db)
    else:
        cache_key = None
        should_close = False
        conn = db

    # Check cache
    if cache_key and cache_key in _table_created:
        if should_close:
            conn.close()
        return

    logger.info("Setting up outbox table with triggers")
    try:
        with conn.cursor() as cursor:
            for ddl in DDL_STATEMENTS:
                cursor.execute(ddl.format(table_name=table_name))
        conn.commit()

        if cache_key:
            _table_created.add(cache_key)
        logger.debug("Outbox table and triggers created successfully")
    except Exception as exc:
        conn.rollback()
        logger.error(f"Failed to create outbox table: {exc}", exc_info=True)
        raise
    finally:
        if should_close:
            conn.close()


async def ensure_outbox_table_async(
    db: str | asyncpg.Connection, table_name: str = "outbox_table"
) -> None:
    """Ensure outbox table exists with proper schema and triggers.

    Args:
        db: Either a PostgreSQL connection URL string (e.g., "postgresql://user:pass@host/db")
            or an existing asyncpg connection object
    """
    # Determine if we need to create/close connection
    if isinstance(db, str):
        cache_key = db
        should_close = True
        conn = await asyncpg.connect(db)
    else:
        cache_key = None
        should_close = False
        conn = db

    # Check cache
    if cache_key and cache_key in _table_created:
        if should_close:
            await conn.close()
        return

    logger.info("Setting up outbox table with triggers")
    try:
        async with conn.transaction():
            for ddl in DDL_STATEMENTS:
                await conn.execute(ddl.format(table_name=table_name))

        if cache_key:
            _table_created.add(cache_key)
        logger.debug("Outbox table and triggers created successfully")
    except Exception as exc:
        logger.error(f"Failed to create outbox table: {exc}", exc_info=True)
        raise
    finally:
        if should_close:
            await conn.close()


tracking_ids_contextvar: ContextVar[tuple[str, ...]] = ContextVar[tuple[str, ...]](
    "tracking_ids", default=()
)


def get_tracking_ids() -> tuple[str, ...]:
    return tracking_ids_contextvar.get()


@contextmanager
def tracking() -> Generator[None, None, None]:
    tracking_ids = tracking_ids_contextvar.get()
    tracking_ids = tracking_ids + (str(uuid.uuid4()),)
    token = tracking_ids_contextvar.set(tracking_ids)
    yield
    tracking_ids_contextvar.reset(token)


def parse_duration(s: str) -> int:
    """Convert a duration string to milliseconds

    Valid inputs: 0, 0m, 0s, 0ms, 1m, 30s, 500ms, 1m30s, 1m500ms, 30s500ms, 1m30s500ms
    Invalid inputs: 1h, 60m, 61m, 60s, 61s, 1000ms, 1001ms, 01m, 01s, 01ms, combinations of these
        and negative values
    """

    if s in ("0", "0ms", "0s", "0m", "0h", "0d"):
        return 0

    match = re.search(r"^([^0]\d*d)?([^0]\d*h)?([^0]\d*m)?([^0]\d*s)?([^0]\d*ms)?$", s)
    if match is None:
        raise ValueError(f"Invalid duration string: {s!r}")
    days_string, hours_string, minutes_string, seconds_string, milliseconds_string = match.groups()

    result = 0

    if days_string:
        days = int(days_string[:-1])
        result += days * 24 * 60 * 60 * 1000

    if hours_string:
        hours = int(hours_string[:-1])
        if not 1 <= hours <= 23:
            raise ValueError(f"Invalid value for {hours=}, must be between 1 and 23")
        result += hours * 60 * 60 * 1000

    if minutes_string:
        minutes = int(minutes_string[:-1])
        if not 1 <= minutes <= 59:
            raise ValueError(f"Invalid value for {minutes=}, must be between 1 and 60")
        result += minutes * 60 * 1000

    if seconds_string:
        seconds = int(seconds_string[:-1])
        if not 1 <= seconds <= 59:
            raise ValueError(f"Invalid value for {seconds=}, must be between 1 and 60")
        result += seconds * 1000

    if milliseconds_string:
        milliseconds = int(milliseconds_string[:-2])
        if not 1 <= milliseconds <= 999:
            raise ValueError(f"Invalid value for {milliseconds=}, must be between 1 and 999")
        result += milliseconds

    if result == 0:
        raise ValueError(f"Invalid duration string: {s!r}")

    return result
