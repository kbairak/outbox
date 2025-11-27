import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Generator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from outbox.database import Base


class Reject(Exception):
    pass


_table_created = False


async def ensure_database(db_engine: AsyncEngine) -> None:
    global _table_created

    if _table_created:
        return

    async with db_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Create NOTIFY trigger for instant message delivery
    async with AsyncSession(db_engine) as session:
        await session.execute(
            text("""
                CREATE OR REPLACE FUNCTION notify_outbox_insert() RETURNS TRIGGER AS $$
                BEGIN
                    IF NEW.send_after <= NOW() THEN
                        PERFORM pg_notify('outbox_channel', '');
                    END IF;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql
            """)
        )
        await session.execute(text("DROP TRIGGER IF EXISTS outbox_notify_trigger ON outbox_table"))
        await session.execute(
            text("""
                CREATE TRIGGER outbox_notify_trigger
                    AFTER INSERT ON outbox_table
                    FOR EACH ROW
                    EXECUTE FUNCTION notify_outbox_insert()
            """)
        )
        await session.commit()

    _table_created = True


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
