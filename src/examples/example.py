import asyncio
import logging
import random

from prometheus_client import start_http_server
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from outbox import Emitter, MessageRelay, Worker, listen

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("outbox").setLevel(logging.DEBUG)
logging.getLogger("aio_pika").setLevel(logging.INFO)
logging.getLogger("aiormq").setLevel(logging.INFO)

start_http_server(8000)

db_engine = create_async_engine("postgresql+asyncpg://postgres:postgres@localhost/postgres")

emitter = Emitter(db_engine=db_engine, auto_create_table=True)

message_relay = MessageRelay(
    db_engine=db_engine,
    rmq_connection_url="amqp://guest:guest@localhost:5672/",
    auto_create_table=True,
)


class User(BaseModel):
    username: str
    first_name: str
    last_name: str


@listen(binding_key="user.*", queue="on_user_event")
async def on_user_event(user: User, routing_key: str) -> None:
    if random.random() < 0.5:
        raise Exception("Listener error")
    print(f"User event other ({routing_key}): {user=}")


@listen(binding_key="user.created", queue="on_user_created")
async def on_user_created(user: User, routing_key: str) -> None:
    if random.random() < 0.6:
        raise Exception("Listener error")
    print(f"User event {routing_key}: {user=}")


@listen(binding_key="user.updated", queue="on_user_updated")
async def on_user_updated(user: User, routing_key: str) -> None:
    if random.random() < 0.7:
        raise Exception("Listener error")
    print(f"User event {routing_key}: {user=}")


@listen(binding_key="user.deleted", queue="on_user_deleted")
async def on_user_deleted(user: User, routing_key: str) -> None:
    if random.random() < 0.8:
        raise Exception("Listener error")
    print(f"User event {routing_key}: {user=}")


async def main() -> None:
    message_relay_task = asyncio.create_task(message_relay.run())
    worker = Worker(
        rmq_connection_url="amqp://guest:guest@localhost:5672/",
        listeners=(on_user_event, on_user_created, on_user_updated, on_user_deleted),
        retry_delays=("1s", "2s", "3s"),
    )
    worker_task = asyncio.create_task(worker.run())

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
                await emitter.emit(session, routing_key, user)
    except EOFError:
        pass
    finally:
        message_relay_task.cancel()
        worker_task.cancel()


asyncio.run(main())
