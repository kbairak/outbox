import asyncio
import logging
import random

from prometheus_client import start_http_server
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from outbox import MessageRelay, Publisher, Worker, consume
from outbox.utils import ensure_outbox_table_async

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("outbox").setLevel(logging.DEBUG)
logging.getLogger("aio_pika").setLevel(logging.INFO)
logging.getLogger("aiormq").setLevel(logging.INFO)

start_http_server(8000)

db_url = "postgresql://postgres:postgres@localhost/postgres"

# Create tables
asyncio.run(ensure_outbox_table_async(db_url))

# Create SQLAlchemy engine for backward compatible publishing
db_engine = create_async_engine(f"postgresql+asyncpg://{db_url.split('://', 1)[1]}")

publisher = Publisher()

message_relay = MessageRelay(
    db_engine_url=db_url,
    rmq_connection_url="amqp://guest:guest@localhost:5672/",
)


class User(BaseModel):
    username: str
    first_name: str
    last_name: str


@consume(binding_key="user.*", queue="on_user_event")
async def on_user_event(user: User, routing_key: str) -> None:
    if random.random() < 0.5:
        raise Exception("Consumer error")
    print(f"User event other ({routing_key}): {user=}")


@consume(binding_key="user.created", queue="on_user_created")
async def on_user_created(user: User, routing_key: str) -> None:
    if random.random() < 0.6:
        raise Exception("Consumer error")
    print(f"User event {routing_key}: {user=}")


@consume(binding_key="user.updated", queue="on_user_updated")
async def on_user_updated(user: User, routing_key: str) -> None:
    if random.random() < 0.7:
        raise Exception("Consumer error")
    print(f"User event {routing_key}: {user=}")


@consume(binding_key="user.deleted", queue="on_user_deleted")
async def on_user_deleted(user: User, routing_key: str) -> None:
    if random.random() < 0.8:
        raise Exception("Consumer error")
    print(f"User event {routing_key}: {user=}")


async def main() -> None:
    message_relay_task = asyncio.create_task(message_relay.run())
    worker = Worker(
        rmq_connection_url="amqp://guest:guest@localhost:5672/",
        consumers=(on_user_event, on_user_created, on_user_updated, on_user_deleted),
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
                await publisher.publish(session, routing_key, user)
    except EOFError:
        pass
    finally:
        message_relay_task.cancel()
        worker_task.cancel()


asyncio.run(main())
