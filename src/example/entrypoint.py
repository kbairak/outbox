import asyncio
import sys

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from outbox import emit, setup

db_engine = create_async_engine("postgresql+asyncpg://postgres:postgres@localhost:5432/postgres")
setup(db_engine=db_engine)


async def main():
    async with AsyncSession(db_engine) as session, session.begin():
        await emit(session, sys.argv[1], sys.argv[2])


if __name__ == "__main__":
    asyncio.run(main())
