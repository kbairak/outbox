import asyncio
import logging
import sys

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

import outbox
from outbox import emit, listen

logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO)
logging.getLogger("outbox").setLevel(logging.DEBUG)


db_engine = create_async_engine("postgresql+asyncpg://postgres:postgres@localhost:5432/postgres")
outbox.setup(
    db_engine=db_engine,
    rmq_connection_url="amqp://guest:guest@localhost:5672/",
    poll_interval=0.1,
)


@listen("foo")
async def foo(_):
    await asyncio.sleep(10)


async def main():
    async with AsyncSession(db_engine) as session, session.begin():
        await emit(session, "foo", {"message": "Hello, World!"})


if __name__ == "__main__":
    if sys.argv[1:] == ["worker"]:
        asyncio.run(outbox.worker())
    elif sys.argv[1:] == ["message_relay"]:
        asyncio.run(outbox.message_relay())
    else:
        asyncio.run(main())
