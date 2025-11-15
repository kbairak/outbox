# Outbox pattern for Python, SQLAlchemy, RabbitMQ and Pydantic

Implementation of the [outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html) for async Python applications with SQLAlchemy and RabbitMQ.

```mermaid
flowchart LR
    MA{Main app} -->|"emit()"| DB[Outbox table]
    DB ~~~ MR{Mesage relay}
    MR -->|SELECT FOR UPDATE| DB
    MR ~~~ DB
    MR --->|publish| ME["Exchange (topic)"]

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
    async with AsyncSession(db_engine) as session, session.begin():
        session.add(User(id=123, username="johndoe"))
        await emit(session, "user.created", {"id": 123, "username": "johndoe"})

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
    <summary><h3>Listener arguments</h3></summary>

Given everything we have discussed so far, the worker will populate the arguments of your listener functions based on their names and/or type annotations.

If you arguments are named:

- `routing_key`: it will be populated with the routing key of the message (this may be useful if the binding key of the queue uses wildcards)
- `message`: it will be populated with the raw aio-pika message object
- `track_ids`: it will be populated with the tracking IDs of the message
- `retry_limit`: it will be populated with the retry limit that has been configured
- `retry_on_error`: it will be populated with the retry_on_error setting that has been configured
- `queue_name`: it will be populated with the name of the queue that the listener is consuming from (may be useful if it was automatically generated by the library)
- `attempt_count`: it will be populated with the number of attempts that have been made to process the message (starting from 1)

You must have exactly **one** argument that doesn't meet the above criteria, which will be populated with the body of the message. If you supply a type annotation that is a subclass of `pydantic.BaseModel`, the library will automatically deserialize the body into an instance of that class. If you don't supply a type annotation, the library will attempt to deserialize it with `json.loads`. If that fails, you will receive the contents of the message as bytes.

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
    <summary><h3>CPU-bound work</h3></summary>

The outbox pattern is designed for I/O-bound operations like sending emails, calling external APIs, or writing to databases. For these tasks, the library's async approach using `asyncio.create_task()` provides excellent concurrency without blocking.

However, if your listener needs to perform CPU-intensive work (image processing, data transformations, heavy computations), you should offload it to a process pool. **This is intentionally not built into the library** because:

1. Most outbox use cases are I/O-bound, not CPU-bound
2. Users have different needs (process pools, thread pools, custom executors)
3. Python's standard library already makes this trivial

Here's how to handle CPU-bound work in your listeners:

```python
import asyncio
from concurrent.futures import ProcessPoolExecutor
from outbox import listen

# Create a process pool (do this once at startup)
process_pool = ProcessPoolExecutor(max_workers=4)

def cpu_intensive_task(image_data: bytes) -> bytes:
    """This runs in a separate process, doesn't block the event loop"""
    # Expensive CPU work: resize, filter, transform, etc.
    from PIL import Image
    import io

    image = Image.open(io.BytesIO(image_data))
    image.thumbnail((800, 600))

    output = io.BytesIO()
    image.save(output, format='JPEG')
    return output.getvalue()

@listen("image.uploaded")
async def process_image(image_data: bytes):
    """Listener remains async and non-blocking"""
    loop = asyncio.get_event_loop()

    # Offload CPU work to process pool
    processed_data = await loop.run_in_executor(
        process_pool,
        cpu_intensive_task,
        image_data
    )

    # Continue with I/O-bound work
    await upload_to_storage(processed_data)
```

This pattern gives you complete control over parallelism while keeping the library focused and simple.

</details>

<details>
    <summary><h3>Logging</h3></summary>

The library logs important events (emitted messages, processing results, retries, errors) using Python's standard logging module under the logger name `"outbox"`. Since the outbox pattern handles critical infrastructure (message processing, retries, dead-letter queues), **logs are enabled by default** to ensure you're aware of any issues.

If you haven't configured logging in your application, you'll see log output automatically. To control the log level or disable logs entirely:

```python
import logging

# Configure logging for your entire application
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Or control just the outbox logger
logging.getLogger("outbox").setLevel(logging.WARNING)  # Only warnings and errors

# Or disable outbox logs entirely
logging.getLogger("outbox").setLevel(logging.CRITICAL)

# Or disable propagation to root logger
logging.getLogger("outbox").propagate = False
```

The library logs at these levels:

- **DEBUG**: Detailed information about queue bindings, message polling
- **INFO**: Normal operations (messages emitted, processed successfully)
- **WARNING**: Retries, rejections, messages sent to dead-letter queues
- **ERROR**: Failures during deserialization or unexpected errors

For production, `logging.INFO` is recommended so you can track message flow without excessive noise.

</details>

<details>
    <summary><h3>API</h3></summary>

#### `setup()`

- `db_engine_url`: A string that indicates database dialect and connection arguments. Will be passed to SQLAlchemy. Follows the pattern `<database_type>+<dbapi>://<username>:<password>@<host>:<port>/<db_name>`. Make sure you use a DBAPI that supports async operations, like `asyncpg` for PostgreSQL or `aiosqlite` for SQLite. Examples: `postgresql+asyncpg://postgres:postgres@localhost:5432/postgres` or `sqlite+aiosqlite:///:memory:`
- `db_engine`: If you already have a SQLAlchemy engine, you can pass it here instead of `db_engine_url` (you must pass either one or the other)
- `rmq_connection_url`: A string that indicates RabbitMQ connection parameters. Follows the pattern `amqp[s]://<username>:<password>@<host>:(<port>)/(virtualhost)`. Example: `amqp://guest:guest@localhost:5672/`
- `rmq_connection`: If you already have a aio-pika connection, you can pass it here instead of `rmq_connection_url` (you must pass either one or the other)
- `exchange_name`: Name of the RabbitMQ exchange to use. Defaults to `outbox_exchange`
- `poll_interval`: How often to poll the outbox table for unsent messages. Defaults to `1` second
- `retry_on_error`: Whether to retry messages that fail to be processed by the listener. Defaults to `True`
- `expiration`: Expiration time in seconds for messages in RabbitMQ. Defaults to `None` (no expiration)
- `clean_up_after`: How long to keep messages in the outbox table after they are sent. Can be `IMMEDIATELY`, `NEVER`, or a float representing seconds
- `retry_limit`: Default maximum number of retries for a message. Defaults to `3`
- `table_name`: Name of the outbox table to use. Defaults to `outbox_table`

#### `emit()`

Positional arguments:

- `session`: An async SQLAlchemy session that is used to emit the message
- `routing_key`: The routing key to use for the message
- `body`: The body of the message. If it is an instance of a Pydantic model, it will be serialized by Pydantic, if it is bytes, it will be used as is, otherwise outbox will attempt to serialize it with `json.dumps`

Keyword-only arguments:

- `retry_limit`: The maximum number of retries for the message. Overrides the default set during `setup`
- `expiration`: The expiration time in seconds for the message. Overrides the default set during `setup`
- `eta`: The time at which the message should be sent. Can be a `datetime`, a `timedelta` or an interval in milliseconds

#### `listen()`

Positional arguments:

- `binding_key`: The binding key to use for the listener. Supports wildcards, e.g. `user.*` will match `user.created`, `user.updated`, etc, according to RabbitMQ's topic exchange rules

Keyword-only arguments:

- `queue_name`: The name of the queue to use for the listener. If not provided, a name based on the current module and the function's name will be used
- `retry_limit`: The maximum number of retries for the message. Overrides the default set during `setup`
- `retry_on_error`: Whether to retry messages that fail to be processed by the listener. Overrides the default set during `setup`
- `tags`: A set of tags to use for the listener. This can be used to limit which queues this listener will consume from when the worker is started with a set of tags

</details>

<details>
    <summary><h3>Singleton vs multiple instances</h3></summary>

> You likely will not have to be aware of this feature, but it was useful for developing unit-tests for this library.

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

### High priority

- [ ] Don't retry immediately, implement a backoff strategy
- [ ] Refactor away `@listen` decorator, implement more explicit alternative

### Medium priority

- [ ] Observability (prometheus)
- [ ] Better/more error messages
- [ ] Mypy
- [ ] Performance tests/benchmarks
- [ ] Heartbeat to verify connection to RabbitMQ is alive
- [ ] Fetch multiple messages at once from outbox table
- [ ] Channel/connection pooling

### Low priority

- [ ] Add `queue_prefix` setup arg
- [ ] Use msgpack (optionally) to reduce size
- [ ] Use pg notify/listen to avoid polling the database
- [ ] Nested dependencies
- [ ] No 'application/json' content type if body is bytes
