import asyncio
import logging
import random

from prometheus_client import start_http_server
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from outbox import emit, listen, message_relay, setup, worker

logging.getLogger("outbox").setLevel(logging.DEBUG)

start_http_server(8000)

db_engine = create_async_engine("postgresql+asyncpg://postgres:postgres@localhost/postgres")

setup(
    db_engine=db_engine,
    rmq_connection_url="amqp://guest:guest@localhost:5672/",
    auto_create_table=True,
    retry_delays=(1, 2, 3),
)


class User(BaseModel):
    username: str
    first_name: str
    last_name: str


@listen("user.*")
async def on_user_event(user: User, routing_key: str) -> None:
    if random.random() < 0.5:
        raise Exception()
    print(f"User event other ({routing_key}): {user=}")


@listen("user.created")
async def on_user_created(user: User, routing_key: str) -> None:
    if random.random() < 0.6:
        raise Exception()
    print(f"User event {routing_key}: {user=}")


@listen("user.updated")
async def on_user_updated(user: User, routing_key: str) -> None:
    if random.random() < 0.7:
        raise Exception()
    print(f"User event {routing_key}: {user=}")


@listen("user.deleted")
async def on_user_deleted(user: User, routing_key: str) -> None:
    if random.random() < 0.8:
        raise Exception()
    print(f"User event {routing_key}: {user=}")


async def main() -> None:
    message_relay_task = asyncio.create_task(message_relay())
    worker_task = asyncio.create_task(
        worker((on_user_event, on_user_created, on_user_updated, on_user_deleted))
    )

    user = User(username="johndoe", first_name="John", last_name="Doe")
    try:
        while True:
            event = await asyncio.to_thread(input, "[c]reated, [u]pdated, [d]eleted, [o]ther:\n")
            if event == "c":
                routing_key = "user.created"
            elif event == "u":
                routing_key = "user.updated"
            elif event == "d":
                routing_key = "user.deleted"
            elif event == "o":
                routing_key = "user.other"
            else:
                continue

            async with AsyncSession(db_engine) as session, session.begin():
                await emit(session, routing_key, user)
    except EOFError:
        pass
    finally:
        message_relay_task.cancel()
        worker_task.cancel()


asyncio.run(main())
