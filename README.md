# Outbox pattern for Python, SQLAlchemy, RabbitMQ and Pydantic

Implementation of the [outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html) for async Python applications with SQLAlchemy and RabbitMQ.

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
        await emit(session, "user.created", {"id": 123, "username": "johndoe"})
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
    <summary><h3>Emit inside database transaction</h3></summary>

You can (and should) call `emit` inside a database transaction. This way, data creation and triggering of side-effects will either succeed together or fail together. This is the main goal of the outbox pattern.

```python
async with AsyncSession(db_engine) as session, session.begin():
    session.add(User(id=123, username="johndoe"))
    await emit(session, "user.created", {"id": 123, "username": "johndoe"})
    # commit not needed because of `session.begin()`
```

</details>

<details>
    <summary><h3>Retries and dead-lettering</h3></summary>

If you take no precaution and your listener raises an exception, the event will be re-queued and run again as soon as possible. This is useful for transient errors, but can lead to infinite loops if the error is not transient. There are several ways you can control how an event will retry:

- Setting a retry limit (integer)
- Setting a message expiration (timedelta or datetime or number of milliseconds)
- Setting `retry_on_error=False`
- Explicitly raising the `outbox.Reject` exception

The first three options can be used with the following ways, in order of precedence:

- As an argument during `emit` (except for `retry_on_error`, which is not supported here)
- As an argument during `listen` (except for `expiration` which is not supported here)
- As an argument during `setup`

```python
from outbox import setup, emit, listen, Reject

setup(..., retry_limit=5)  # or expiration or retry_on_error

async def main():
    async with AsyncSession(db_engine) as session:
        await emit(..., retry_limit=4)  # or expiration
        await session.commit()

@listen(..., retry_limit=5)  # or expiration or retry_on_error
async def on_event(event):
    if some_condition:
        raise Reject()
```

If an event fails to be processed because of any of the reasons above, it will be sent to a dead-letter exchange and from there to a dead-letter queue. One dead-letter queue is created for each regular queue and its name is the same prefixed with `dlq_`. When you encounter messages in your dead-letter queues, you can inspect the logs, figure out what went wrong, fix the code, restart the worker and use the RabbitMQ shovel interface to move the messages back to their respective queues for re-processing.

If your listener function accepts arguments called `retry_limit`, `expiration` or `retry_on_error`, they will be populated by the values that are actually used for the event.

One final note, even if you have set `retry_on_error=False`, you can still force a retry by raising the `outbox.Retry` exception.
</details>

<details>
    <summary><h3>Track IDs</h3></summary>

While using the outbox pattern, you will be emitting messages from an entrypoint (usually and API endpoint) which will be picked up by listeners which will in turn emit their own messages and so on. It can be beneficial to assign tracking IDs so that you can track the entire history of emissions. This library assigns a UUID every time you emit, then the listener will get the tracking history of the current event and then, when it emits, will append its own UUID. You can get the whole list of UUIDs by invoking `outbox.get_track_ids()` inside the listener or by passing a `track_ids` parameter to the listener:

```python
async def entrypoint():
    async with AsyncSession(db_engine) as session:
        await emit(session, "user.created", {"id": 123, "username": "johndoe"})
        await session.commit()

@listen("user.created")
async def on_user_created(user, track_ids: tuple[str, ...]):
    logger.info(f"User created {user.id}, tracking IDs: {track_ids}")
    async with AsyncSession(db_engine) as session:
        await emit(session, "user.welcome_email", {"id": user.id})
        await emit(session, "user.created_notification", {"id": user.id})
        await session.commit()

@listen("user.welcome_email")
async def on_user_welcome_email(user, track_ids: tuple[str, ...]):
    logger.info(f"Welcome email sent for user {user.id}, tracking IDs: {track_ids}")

@listen("user.created_notification")
async def on_user_created_notification(user, track_ids):
    logger.info(f"Notification created for user {user.id}, tracking IDs: {track_ids}")
```

The log statements in this case will output:

```
User created 123, tracking IDs: ['uuid1']
Welcome email sent for user 123, tracking IDs: ['uuid1', 'uuid2']
Notification created for user 123, tracking IDs: ['uuid1', 'uuid3']
```

If you want to include a UUID for the entrypoint as well, you have to wrap your initial emits (or the entire entrypoint) with `with outbox.tracking():`

```python
async def entrypoint():
    with outbox.tracking():
        async with AsyncSession(db_engine) as session:
            await emit(session, "user.created", {"id": 123, "username": "johndoe"})
            await session.commit()
```

In that case, your output would be:

```
User created 123, tracking IDs: ['uuid1', uuid2']
Welcome email sent for user 123, tracking IDs: ['uuid1', 'uuid2', 'uuid3']
Notification created for user 123, tracking IDs: ['uuid1', 'uuid2', 'uuid4']
```

</details>

<details>
    <summary><h3>Graceful shutdown</h3></summary>

When the worker receives a SIGINT or SIGTERM, it will request a disconnect from all the queues. Any messages that are sent before the disconnect request is processed will be rejected by the worker with `requeue=True` (so they will be consumed by other workers, immediately or later). In the meantime, any messages that have already started being processed will keep being processed until the listener function terminates. When all pending tasks have finished, the worker will exit.

Example sequence of events:

```mermaid
sequenceDiagram
    participant Pub as Publisher
    participant Q as RabbitMQ Queue
    participant W as Worker
    participant OW as Other Worker

    W->>W: Start
    Pub->>Q: Publish event 1
    Q-->>W: Send event 1
    W->>W: Start processing event 1

    Note right of W: SIGINT or SIGTERM received
    W->>Q: Request disconnect from all queues

    Pub->>Q: Publish event 2
    Q-->>W: Send event 2
    W->>Q: Reject event 2 (requeue=True)

    Q-->>W: Acknowledge disconnect request

    Pub->>Q: Publish event 3
    Note right of Q: Event 3 not sent to W

    W->>W: Finish processing event 1
    W->>Q: Ack event 1
    W->>W: Exit

    OW->>Q: Start and connect
    Q-->>OW: Send event 2
    Q-->>OW: Send event 3
    OW->>OW: Process event 2
    OW->>Q: Ack event 2
    OW->>OW: Process event 3
    OW->>Q: Ack event 3
```

</details>

<details>
    <summary><h3>Topic exchange and wildcard matching</h3></summary>

```python
# Main application
async with AsyncSession(db_engine) as session:
    await emit(session, "user.created", {"id": 123, "username": "johndoe"})
    await session.commit()

# Worker process
@listen("user.*")
async def on_user_event(user):
    print(user)
    # <<< {"id": 123, "username": "johndoe"}
```

If you are using this and you want to know the routing key inside the body of the listener, you can add a `routing_key` argument to the listener:

```python
# Main application
async with AsyncSession(db_engine) as session:
    await emit(session, "user.created", {"id": 123, "username": "johndoe"})
    await session.commit()

# Worker process
@listen("user.*")
async def on_user_event(routing_key: str, user):
    logger.info(f"Received {routing_key=}")
    # <<< Received routing_key=user.created
    print(user)
    # <<< {"id": 123, "username": "johndoe"}
```

</details>

<details>
    <summary><h3>Distribution of tasks to multiple workers</h3></summary>

By default, workers will consume messages from all queues (1 queue is defined per listener function). If you want more control on which workers will consume from which queues, you can assign a set of tags on each listener and a set of tags when invoking the worker itself:

```python
# Listeners

@listen("binding_key_1", tags={"normal_priority"})
async def on_normal_priority(obj):
    ...

@listen("binding_key_2", tags={"high_priority"})
async def on_high_priority(obj):
    ...

# Worker 1

# Lets add a worker for all tasks to make sure none are left behind
asyncio.run(worker())

# Worker 2

# Lets add an extra worker just or the high-priority tasks
asyncio.run(worker(tags={"high_priority"}))
```

</details>

<details>
    <summary><h3>Delayed execution</h3></summary>

You can cause an event to be sent some time in the future by setting the `eta` argument during `emit`:

```python
async with AsyncSession(db_engine) as session:
    await emit(
        session,
        "user.created",
        {"id": 123, "username": "johndoe"},
        eta=datetime.datetime.now() + datetime.timedelta(minutes=5),
    )
    await session.commit()
```

</details>

<details>
    <summary><h3>Automatic (de)serialization of Pydantic models</h3></summary>

```python
class User(BaseModel):
    id: int
    username: str

# Main application
async with AsyncSession(db_engine) as session:
    await emit(session, "user.created", User(id=123, username="johndoe"))
    await session.commit()

# Worker process
@listen("user.created")
async def on_user_created(user: User):  # inspects type annotation
    print(user)
    # <<< User(id=123, username="johndoe")
```

</details>

<details>
    <summary><h3>Outbox table cleanup</h3></summary>

You can choose a strategy for when already sent messages from the outbox table should be cleaned up by passing the `clean_up_after` argument during setup:

```python
setup(..., clean_up_after=datetime.timedelta(days=7))
```

The options are:

- **`IMMEDIATELY` (the default)**: messages are cleaned up immediately after being sent to RabbitMQ.
- **`NEVER`**: messages are never cleaned up, you will have to do it manually.
- **Any `datetime.timedelta` instance**.

</details>

<details>
    <summary><h3>Singleton vs multiple instances</h3></summary>

This library has been implemented in such a way that you can run single or multiple outbox setups. Most use-cases will use the singleton approach:

```python
from outbox import setup, emit

db_engine = create_async_engine("postgresql+asyncpg://user:password@localhost/dbname")
setup(db_engine=db_engine)

async def main():
    async with AsyncSession(db_engine) as session:
        await emit(session, "user.created", {"id": 123, "username": "johndoe"})
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
        await outbox.emit(session, "user.created", {"id": 123, "username": "johndoe"})
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
        await outbox1.emit(session, "user.created", {"id": 123, "username": "johndoe"})
        await session.commit()
    async with AsyncSession(db_engine2) as session:
        await outbox2.emit(session, "user.created", {"id": 456, "username": "maryjane"})
        await session.commit()

asyncio.run(main())
```

The whole approach is explained [in this blog post](https://www.kbairak.net/programming/python/2020/09/16/global-singleton-vs-instance-for-libraries.html).
</details>

## TODOs

- Use dataclass for Outbox
- Dependency injection on listen
- Console scripts for message relay and worker
- Don't retry immediately, implement a backoff strategy
- uv cache for github actions
- Use msgpack (optionally) to reduce size
- Use pg notify/listen to avoid polling the database
