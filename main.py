import asyncio
import datetime
import logging

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
    asyncio.create_task(outbox.worker())
    asyncio.create_task(outbox.message_relay())
    async with AsyncSession(db_engine) as session, session.begin():
        await emit(session, "foo", {"message": "Hello, World!"})
    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
