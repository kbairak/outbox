# Outbox pattern for Python, SQLAlchemy, RabbitMQ and Pydantic

```mermaid
flowchart LR
    MA{Main app} -->|"emit()"| DB[Outbox table]
    DB ~~~ MR{Mesage relay}
    MR -->|SELECT FOR UPDATE| DB
    MR ~~~ DB
    MR --->|publish| ME["Main exchange (topic)"]

    subgraph Database
    DB
    end

    subgraph RabbitMQQ
    ME -->|binding| Q1[Queue 1]
    end

    Q1 ~~~ W{Worker} --->|"consume()"| Q1
    W{Worker} ~~~ Q1
```

## Usage

### Main application

```python
import asyncio

from outbox import setup, emit
from sqlalchemy.ext.asyncio import create_async_engine

db_engine = create_async_engine("postgresql+asyncpg://user:password@localhost/dbname")

async def main():
    await setup(db_engine=db_engine)

    async with AsyncSession(db_engine) as session:
        emit(session, "user.created", {"id": 123, "username": "johndoe"})
        await session.commit()

asyncio.run(main())
```

No need for migrations, `setup` will get-or-create the outbox table automatically.

### Message relay process

```python
import asyncio

from outbox import setup, message_relay

async def main():
    await setup(
        db_engine_url="postgresql+asyncpg://user:password@localhost/dbname",
        rabbitmq_url="amqp://user:password@localhost:5672/",
    )
    await message_relay()

asyncio.run(main())
```

### Worker process

```python
import asyncio

from outbox import setup, listen, worker

@listen("user.created")
async def on_user_created(user):
    print(user)
    # <<< {"id": 123, "username": "johndoe"}

async def main():
    await setup(rabbitmq_url="amqp://user:password@localhost:5672/")
    await worker()

asyncio.run(main())
```

## Features

### Emit inside database transaction

You can (should) call `emit` inside a database transaction. This way, data creation and triggering of side-effects will either succeed together or fail together.

```python
async with AsyncSession(db_engine) as session, session.begin():
    session.add(User(id=123, username="johndoe"))
    emit(session, "user.created", {"id": 123, "username": "johndoe"})
    # commit not needed because of `session.begin()`
```

### Topic exchange and wildcard matching

```python
# Main application
async with AsyncSession(db_engine) as session:
    emit(session, "user.created", {"id": 123, "username": "johndoe"})

# Worker process
@listen("user.*")
async def on_user_event(user):
    print(user)
    # <<< {"id": 123, "username": "johndoe"}
```

### Automatic (de)serialization of Pydantic models

```python
class User(BaseModel):
    id: int
    username: str

# Main application
async with AsyncSession(db_engine) as session:
    emit(session, "user.created", User(id=123, username="johndoe"))

# Worker process
@listen("user.created")
async def on_user_created(user: User):  # inspects type annotation
    print(user)
    # <<< User(id=123, username="johndoe")
```

### Retries

In most cases, an exception in an event handler will cause a retry:

```python
@listen("user.created")
async def on_user_created(user: User):
    if user.id == 123:
        raise ValueError("This is a test error")
    print(user)
```

You can disable this behavior by passing `retry_on_error=False` during setup:

```python
await setup(..., retry_on_error=False)
```

Or during `listen`:

```python
@listen("user.created", retry_on_error=False)
async def on_user_created(user: User):
    ...
```

Regardless of the default behavior, you can force a retry or a non-retry by raising `Retry` or `Abort` exceptions, respectively:

```python
from outbox import Retry, Abort, listen

@listen("user.created")
def on_user_created(user: User):
    if user.id == 123:
        raise Retry("This is a test error, retrying")
    elif user.id == 456:
        raise Abort("This is a test error, aborting")
    print(user)
```

Finally, raising `Reject` will cause the message to be rejected and dead-lettered:

```python
from outbox import Reject, listen

@listen("user.created")
def on_user_created(user: User):
    if user.id == 123:
        raise Reject("This is a test error, rejecting")
    print(user)
```

### Dead-lettering

```mermaid
flowchart LR
    MA{Main app} -->|"emit()"| DB[Outbox table]
    DB ~~~ MR{Mesage relay}
    MR -->|SELECT FOR UPDATE| DB
    MR ~~~ DB
    MR --->|publish| ME["Main exchange (topic)"]

    subgraph Database
    DB
    end

    subgraph RabbitMQQ
    ME -->|binding| Q1[Queue 1]
    Q1 -->|reject| DLX["Dead Letter Exchange (direct)"]
    DLX --->|binding| DQ1[Dead Letter Queue]
    end

    Q1 ~~~ W{Worker} --->|"consume()"| Q1
    W{Worker} ~~~ Q1
```

A Dead-letter exchange and one dead-letter queue per regular queue are created automatically by the worker. If a message is rejected, it will find its way to the relevant dead-letter queues. You can then fix the code, re-launch the worker and use the shovel interface in RabbitMQ to move the message back to its respective queue so that it can be processed correctly by the worker.

Apart from raising `Reject`, another way to cause messages to be rejected is to set an `expiration` on the outbox instance. If the message isn't acknowledged by the worker within its expiration time (this can happen because of retries), it will enter the dead-letter exchange and queues:

```python
await setup(
    db_engine_url="...",
    rabbitmq_url="...",
    expiration=datetime.timedelta(minutes=5),
)
```

The names of the dead-letter queues are the same as their respective counterparts, prefixed with `dlq_`.

## TODOs

- Clean up outbox table
- Use pg notify/listen to avoid polling the database
- Use msgpack (optionally) to reduce size
- Support binary payloads (without base64)
- Dependency injection on listen
- Add expiration to `emit` and `listen`
- Don't retry immediately, implement a backoff strategy
- Turn setup sync, rmq connection can be lazily created using an async private method
- Pass `routing_key` to listener function by argument name, not type
