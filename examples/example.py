"""Usage:

- Start docker compose services (using root docker-compose.yml)
- Run with `uv run examples/example.py`
- Type a routing key (e.g. `user.created`) and a number indicating which attempt should succeed
- The logs should reflect what happens

Use-cases:

- attempt_count=1, routing_key=user.created: 2 listeners will log a succesful run
- attempt_count=1, routing_key=user.updated: 2 listeners will log a succesful run
- attempt_count=1, routing_key=user.deleted: 1 listener (the generic one) will log a succesful run

- 1<attempt_count<=4, routing_key=user.created: 2 listeners will log a failed run, retry and eventually succeed
- 1<attempt_count<=4, routing_key=user.updated: 2 listeners will log a failed run, retry and eventually succeed
- 1<attempt_count<=4, routing_key=user.deleted: 1 listener (the generic one) will log a failed run, retry and eventually succeed

- attempt_count>4, routing_key=user.created: 2 listeners will log a failed run, retry and eventually dead-letter
- attempt_count>4, routing_key=user.updated: 2 listeners will log a failed run, retry and eventually dead-letter
- attempt_count>4, routing_key=user.deleted: 1 listener (the generic one) will log a failed run, retry and eventually dead-letter
"""

import asyncio
from typing import Awaitable, Callable

from prometheus_client import start_http_server
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from outbox import Listener, emit, message_relay, setup, worker

start_http_server(8000)

db_engine = create_async_engine("postgresql+asyncpg://postgres:postgres@localhost/postgres")

setup(
    db_engine=db_engine,
    rmq_connection_url="amqp://guest:guest@localhost:5672/",
    auto_create_table=True,
)


def get_callback(binding: str, name: str) -> Callable[[int, int], Awaitable[None]]:
    async def callback(succeed_at: int, attempt_count: int) -> None:
        print(f"{name} received {succeed_at=}, {attempt_count=}")
        if attempt_count < succeed_at:
            print(f"{name} failed")
            raise Exception(f"{name} failed")
        print(f"{name} succeeded")

    return callback


async def main() -> None:
    message_relay_task = asyncio.create_task(message_relay())
    worker_task = asyncio.create_task(
        worker(
            (
                Listener(
                    binding_key="user.*",
                    callback=get_callback("user.*", "on_user_event"),
                    queue="on_user_event",
                    retry_delays=(3, 1, 1, 1),
                ),
                Listener(
                    binding_key="user.created",
                    callback=get_callback("user.created", "on_user_created"),
                    queue="on_user_created",
                    retry_delays=(4, 1, 1, 1),
                ),
                Listener(
                    binding_key="user.updated",
                    callback=get_callback("user.updated", "on_user_updated"),
                    queue="on_user_updated",
                    retry_delays=(5, 1, 1, 1),
                ),
            )
        )
    )

    try:
        while True:
            routing_key = await asyncio.to_thread(input, "Routing key:\n")

            try:
                succeed_at = int(await asyncio.to_thread(input, "Succeed at:\n"))
            except ValueError:
                continue

            async with AsyncSession(db_engine) as session, session.begin():
                await emit(session, routing_key, succeed_at)
    except EOFError:
        pass
    finally:
        message_relay_task.cancel()
        worker_task.cancel()


asyncio.run(main())
