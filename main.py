import asyncio
import datetime
import logging
import sys

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

import outbox
from outbox import emit, listen

logging.basicConfig(level=logging.INFO)
logging.getLogger("outbox").setLevel(logging.DEBUG)

db_engine = create_async_engine("postgresql+asyncpg://postgres:postgres@localhost:5432/postgres")
outbox.setup(
    db_engine=db_engine,
    rmq_connection_url="amqp://guest:guest@localhost:5672/",
    expiration=datetime.timedelta(seconds=5),
)


@listen("foo")
async def foo(_):
    pass


async def main():
    if len(sys.argv) == 2 and sys.argv[1] == "message_relay":
        await outbox.message_relay()
    elif len(sys.argv) == 2 and sys.argv[1] == "worker":
        await outbox.worker()
    elif len(sys.argv) == 1:
        async with AsyncSession(db_engine) as session:
            emit(session, "foo", {})
            await session.commit()
    else:
        raise ValueError("Usage: python main.py [message_relay|worker]")


asyncio.run(main())
