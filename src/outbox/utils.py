import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Awaitable, Generator, Optional, Union, overload

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
