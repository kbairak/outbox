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

    subgraph RabbitMQ
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
setup(db_engine=db_engine)

async def main():
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

setup(
    db_engine_url="postgresql+asyncpg://user:password@localhost/dbname",
    rabbitmq_url="amqp://user:password@localhost:5672/",
)

asyncio.run(message_relay())
```

### Worker process

```python
import asyncio

from outbox import setup, listen, worker

setup(rabbitmq_url="amqp://user:password@localhost:5672/")

@listen("user.created")
async def on_user_created(user):
    print(user)
    # <<< {"id": 123, "username": "johndoe"}

asyncio.run(worker())
```

## Features

<details>
    <summary>Emit inside database transaction</summary>

You can (should) call `emit` inside a database transaction. This way, data creation and triggering of side-effects will either succeed together or fail together.

```python
async with AsyncSession(db_engine) as session, session.begin():
    session.add(User(id=123, username="johndoe"))
    emit(session, "user.created", {"id": 123, "username": "johndoe"})
    # commit not needed because of `session.begin()`
```

</details>

<details>
    <summary>Topic exchange and wildcard matching</summary>

```python
# Main application
async with AsyncSession(db_engine) as session:
    emit(session, "user.created", {"id": 123, "username": "johndoe"})
    await session.commit()

# Worker process
@listen("user.*")
async def on_user_event(user):
    print(user)
    # <<< {"id": 123, "username": "johndoe"}
```

</details>

<details>
    <summary>Automatic (de)serialization of Pydantic models</summary>

```python
class User(BaseModel):
    id: int
    username: str

# Main application
async with AsyncSession(db_engine) as session:
    emit(session, "user.created", User(id=123, username="johndoe"))
    await session.commit()

# Worker process
@listen("user.created")
async def on_user_created(user: User):  # inspects type annotation
    print(user)
    # <<< User(id=123, username="johndoe")
```

</details>

<details>
    <summary>Retries</summary>

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
setup(..., retry_on_error=False)
```

Or during `listen`:

```python
@listen("user.created", retry_on_error=False)
async def on_user_created(user: User):
    ...
```

Regardless of the default behavior, you can force a retry by raising the `Retry` exception:

```python
from outbox import Retry, listen

@listen("user.created")
def on_user_created(user: User):
    if user.id == 123:
        raise Retry("This is a test error, retrying")
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

</details>

<details>
    <summary>Dead-lettering</summary>

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

    subgraph RabbitMQ
    ME -->|binding| Q1[Queue 1]
    Q1 -->|reject| DLX["Dead Letter Exchange (direct)"]
    DLX --->|binding| DQ1[Dead Letter Queue]
    end

    Q1 ~~~ W{Worker} --->|"consume()"| Q1
    W{Worker} ~~~ Q1
```

A Dead-letter exchange and one dead-letter queue per regular queue are created automatically by the worker. If a message is rejected, it will find its way to the relevant dead-letter queues. You can then fix the code, re-launch the worker and use the shovel interface in RabbitMQ to move the message back to its respective queue so that it can be processed correctly by the worker.

Apart from raising `Reject`, another way to cause messages to be rejected is via expiration. You can setup an expiration time while setting up the outbox instance or during `emit`. If the message isn't acknowledged by the worker within its expiration time (this can happen because of retries), it will enter the dead-letter exchange and queues:

```python
setup(
    db_engine_url="postgresql+asyncpg://user:password@localhost/dbname",
    rabbitmq_url="amqp://user:password@localhost:5672/",
    expiration=datetime.timedelta(minutes=5),
)
```

Or

```python
async with AsyncSession(db_engine) as session:
    emit(
        session,
        "user.created",
        {"id": 123, "username": "johndoe"},
        expiration=datetime.timedelta(minutes=5),
    )
    await session.commit()
```

The names of the dead-letter queues are the same as their respective counterparts, prefixed with `dlq_`.
</details>

<details>
    <summary>Singleton vs multiple instances</summary>

This library has been implemented in such a way that you can run single or multiple outbox setups. Most use-cases will use the singleton approach:

```python
from outbox import setup, emit

db_engine = create_async_engine("postgresql+asyncpg://user:password@localhost/dbname")
setup(db_engine=db_engine)

async def main():
    async with AsyncSession(db_engine) as session:
        emit(session, "user.created", {"id": 123, "username": "johndoe"})
        await session.commit()

asyncio.run(main())
```

or

```python
from outbox import outbox

db_engine = create_async_engine("postgresql+asyncpg://user:password@localhost/dbname")
outbox.setup(db_engine=db_engine)

async def main():
    async with AsyncSession(db_engine) as session:
        outbox.emit(session, "user.created", {"id": 123, "username": "johndoe"})
        await session.commit()

asyncio.run(main())
```

You can, however, setup multiple instances:

```python
from outbox import Outbox

db_engine1 = create_async_engine("postgresql+asyncpg://user:password@localhost/dbname1")
db_engine2 = create_async_engine("postgresql+asyncpg://user:password@localhost/dbname2")

outbox1 = Outbox(db_engine=db_engine1)
outbox2 = Outbox(db_engine=db_engine2)

async def main():
    async with AsyncSession(db_engine1) as session:
        outbox1.emit(session, "user.created", {"id": 123, "username": "johndoe"})
        await session.commit()
    async with AsyncSession(db_engine2) as session:
        outbox2.emit(session, "user.created", {"id": 456, "username": "maryjane"})
        await session.commit()

asyncio.run(main())
```

The whole approach is explained [in this blog post](https://www.kbairak.net/programming/python/2020/09/16/global-singleton-vs-instance-for-libraries.html).
</details>

## TODOs

- Clean up outbox table
- Use pg notify/listen to avoid polling the database
- Use msgpack (optionally) to reduce size
- Dependency injection on listen
- Don't retry immediately, implement a backoff strategy
- Pass `routing_key` to listener function by argument name, not type
- Add ETA to emit (should be easy thanks to the message relay)
- Find a way to distribute multiple workers
- Cold shutdown
