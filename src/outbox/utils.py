import re
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Generator

from sqlalchemy import Engine, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.orm import Session

from outbox.database import Base


class Reject(Exception):
    pass


_table_created: set[str] = set()


notify_statements = [
    text("""
        CREATE OR REPLACE FUNCTION notify_outbox_insert() RETURNS TRIGGER AS $$
        BEGIN
            IF NEW.send_after <= NOW() THEN
                PERFORM pg_notify('outbox_channel', '');
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
    """),
    text("DROP TRIGGER IF EXISTS outbox_notify_trigger ON outbox_table"),
    text("""
        CREATE TRIGGER outbox_notify_trigger
            AFTER INSERT ON outbox_table
            FOR EACH ROW
            EXECUTE FUNCTION notify_outbox_insert()
    """),
]


def ensure_database_sync(db_engine: Engine) -> None:
    if str(db_engine.url) in _table_created:
        return

    Base.metadata.create_all(db_engine)

    # Create NOTIFY trigger for instant message delivery
    with Session(db_engine) as session:
        for notify_statement in notify_statements:
            session.execute(notify_statement)
        session.commit()

    _table_created.add(str(db_engine.url))


async def ensure_database_async(db_engine: AsyncEngine) -> None:
    if str(db_engine.url) in _table_created:
        return

    async with db_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Create NOTIFY trigger for instant message delivery
    async with AsyncSession(db_engine) as session:
        for notify_statement in notify_statements:
            await session.execute(notify_statement)
        await session.commit()

    _table_created.add(str(db_engine.url))


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

    if s in ("0", "0ms", "0s", "0m"):
        return 0

    match = re.search(r"^([^0]\d*m)?([^0]\d*s)?([^0]\d*ms)?$", s)
    if match is None:
        raise ValueError(f"Invalid duration string: {s!r}")
    minutes_string, seconds_string, milliseconds_string = match.groups()

    result = 0

    if minutes_string:
        minutes = int(minutes_string[:-1])
        if not 1 <= minutes <= 59:
            raise ValueError(
                f"Invalid value for minutes {minutes_string!r}, must be between 1 and 60"
            )
        result += minutes * 60 * 1000

    if seconds_string:
        seconds = int(seconds_string[:-1])
        if not 1 <= seconds <= 59:
            raise ValueError(
                f"Invalid value for seconds {seconds_string!r}, must be between 1 and 60"
            )
        result += seconds * 1000

    if milliseconds_string:
        milliseconds = int(milliseconds_string[:-2])
        if not 1 <= milliseconds <= 999:
            raise ValueError(
                f"Invalid value for milliseconds {milliseconds_string!r}, must be between 1 and 999"
            )
        result += milliseconds

    if result == 0:
        raise ValueError(f"Invalid duration string: {s!r}")

    return result
