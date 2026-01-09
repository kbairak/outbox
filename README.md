# Outbox pattern for Python, PostgreSQL, and RabbitMQ

Implementation of the [outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html) for async Python applications with PostgreSQL and RabbitMQ.

```mermaid
flowchart LR
    MA{Main app} -->|"publish()"| DB[Outbox table]
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

from outbox import Publisher
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

db_engine = create_async_engine("postgresql+asyncpg://user:password@localhost/dbname")
publisher = Publisher()

async def main():
    async with AsyncSession(db_engine) as session, session.begin():
        session.add(User(id=123, username="johndoe"))
        await publisher.publish(session, "user.created", {"id": 123, "username": "johndoe"})

asyncio.run(main())
```

### Message relay process

```python
import asyncio

from outbox import MessageRelay

message_relay = MessageRelay(
    db_engine_url="postgresql+asyncpg://user:password@localhost/dbname",
    rmq_connection_url="amqp://guest:guest@localhost:5672/",
)

asyncio.run(message_relay.run())
```

### Worker process

```python
import asyncio

from outbox import Worker, consume

@consume(binding_key="user.created", queue="on_user_created")
async def on_user_created(user):
    print(user)
    # <<< {"id": 123, "username": "johndoe"}

worker = Worker(
    rmq_connection_url="amqp://guest:guest@localhost:5672/",
    consumers=[on_user_created],
)

asyncio.run(worker.run())
```

<details>
    <summary><h4>Option 2: Using `Consumer` directly</h4></summary>

```python
import asyncio

from outbox import Worker, Consumer

async def on_user_created(user):
    print(user)

worker = Worker(
    rmq_connection_url="amqp://guest:guest@localhost:5672/",
    consumers=[
        Consumer(
            binding_key="user.created",
            queue="analytics_service.on_user_created",  # optional, auto-generated if not provided
            callback=on_user_created,
        )
    ]
)

asyncio.run(worker.run())
```

Both approaches are equivalent. The decorator is more concise, while the explicit `Consumer` instantiation gives you more control and makes it clear which handlers are registered. Essentially, these are identical:

```python
Consumer(binding_key="...", queue="...", callback=..., ...)
consume(binding_key="...", queue="...", ...)(callback)
```

</details>

## Why

### Intro to transactional outbox pattern

When building applications, you often need to perform two operations together: save data to a database and trigger side effects (send an email, call an external API, publish an event). Both operations must succeed together or fail together to maintain consistency. Otherwise, if an error occurs between the database change and the triggering of side effects, your application will be left in an inconsistent state and manual intervention will be required.

**The outbox solution:**

Instead of calling side effects directly, write event messages to an outbox table in the same database transaction as your business data. A separate reliable process (the message relay) reads from this table and publishes messages to your message broker.

**Benefits:**

- **Atomic operations**: Database changes and event publishing succeed or fail together
- **Guaranteed delivery**: At-least-once delivery semantics ensure messages are eventually published
- **No distributed transactions**: Everything happens within a single database transaction
- **Reliable retries**: If publishing fails, the message relay will keep trying

### Retried delays and DLQ (and why celery cannot do them)

This library implements **delayed retries** for failed messages using RabbitMQ's TTL-based delay queues. When a consumer raises an exception, the message is sent to a delay queue (e.g., `outbox.delay_10s`) with a TTL. After the delay expires, RabbitMQ automatically routes the message back to the original queue for retry.

**Dead-letter queues (DLQ):**

After exhausting all retry attempts, messages are sent to a dead-letter queue for manual inspection. This allows you to investigate failures, fix bugs, and reprocess messages.

**Why Celery struggles with this pattern:**

Celery has very flexible retry mechanisms: You can trigger retries explicitly, you can retry only on specific exceptions, you can set max retries and it can apply jitter to distribute a burst of retries over a slightly longer period of time. The main issue with Celery's retries is that, in order for a task to support them, the worker will immediately ACK the message from RabbitMQ which will cause it to be dropped from the queue. After this point, the message only exists inside the worker's memory. If the worker crashes because of an error not handled by Python's exception handling (eg out-of-memory) or if the orchestrator kills the worker because of a deployment or downscaling or for whatever reason, the message will be lost forever.

The trade-off of using this library instead of Celery is that we lose the jitter feature and that we end up with many delay exchanges/queues that will need to be managed. What we get in return is the certainty that messages will never be lost and the fact that we can take advantage of all of RabbitMQ's routing mechanisms.

### Why PostgreSQL, aio-pika (RabbitMQ) and Pydantic

**PostgreSQL** is proven, reliable, and popular. It features a LISTEN/NOTIFY system that makes the message relay more efficient by avoiding polling when there are no messages to send. This is why we focused on PostgreSQL rather than supporting multiple databases.

**aio-pika** is the go-to library for working with RabbitMQ in async Python applications. It provides robust connection handling, automatic reconnection, and full support for RabbitMQ's features.

**Pydantic** is popular and integrated into many existing frameworks (FastAPI, etc.), making automatic message serialization/deserialization seamless.

<details>
    <summary><h4>SQLAlchemy Support</h4></summary>

This library works with **asyncpg** and **psycopg2** connections directly and does not require SQLAlchemy. However, **most applications use SQLAlchemy**, and this library fully supports it.

**Why SQLAlchemy is optional:**

- Keeps the library lightweight with minimal dependencies
- Supports projects that don't use SQLAlchemy
- Allows faster import times

**How to use with SQLAlchemy:**

- Install SQLAlchemy separately: `pip install sqlalchemy[asyncio]` or `uv add sqlalchemy[asyncio]`
- Pass SQLAlchemy sessions to `publisher.publish()` - the library automatically detects and handles them
- Full support for both `AsyncSession` (async) and `Session` (sync)

**How to use without SQLAlchemy:**

- Pass `asyncpg.Connection`, `psycopg2` connection, or `psycopg2` cursor to `publisher.publish()`
- The library works identically with raw connections

The examples use SQLAlchemy since it's the most common use case, but you can substitute raw connections anywhere you see a session.
</details>

## Features

### Overall

<details>
    <summary><h4>Automatic (de)serialization of Pydantic models</h4></summary>

```python
class User(BaseModel):
    id: int
    username: str

# Main application
async with AsyncSession(db_engine) as session:
    await publisher.publish(session, "user.created", User(id=123, username="johndoe"))
    await session.commit()

# Worker process
@consume(binding_key="user.created", queue="on_user_created")
async def on_user_created(user: User):  # inspects type annotation
    print(user)
    # <<< User(id=123, username="johndoe")
```

</details>

<details>
    <summary><h4>Tracking IDs</h4></summary>

While using the outbox pattern, you will be publishing messages from an entrypoint (usually and API endpoint) which will be picked up by consumers which will in turn publish their own messages and so on. It can be beneficial to assign tracking IDs so that you can track the entire history of emissions. This library assigns a UUID every time you publish, then the consumer will get the tracking history of the current event and then, when it publishes, will append its own UUID. You can get the whole list of UUIDs by invoking `outbox.get_tracking_ids()` inside the consumer or by passing a `tracking_ids` parameter to the consumer:

```python
from collections.abc import Sequence

async def entrypoint():
    async with AsyncSession(db_engine) as session:
        await publisher.publish(session, "user.created", {"id": 123, "username": "johndoe"})
        await session.commit()

@consume(binding_key="user.created", queue="on_user_created")
async def on_user_created(user, tracking_ids: Sequence[str]):
    logger.info(f"User created {user.id}, tracking IDs: {tracking_ids}")
    async with AsyncSession(db_engine) as session:
        await publisher.publish(session, "user.welcome_email", {"id": user.id})
        await publisher.publish(session, "user.created_notification", {"id": user.id})
        await session.commit()

@consume(binding_key="user.welcome_email", queue="on_user_welcome_email")
async def on_user_welcome_email(user, tracking_ids: Sequence[str]):
    logger.info(f"Welcome email sent for user {user.id}, tracking IDs: {tracking_ids}")

@consume(binding_key="user.created_notification", queue="on_user_created_notification")
async def on_user_created_notification(user, tracking_ids):
    logger.info(f"Notification created for user {user.id}, tracking IDs: {tracking_ids}")
```

The log statements in this case will output:

```
User created 123, tracking IDs: ['uuid1']
Welcome email sent for user 123, tracking IDs: ['uuid1', 'uuid2']
Notification created for user 123, tracking IDs: ['uuid1', 'uuid3']
```

If you want to include a UUID for the entrypoint as well, you have to wrap your initial publish actions (or the entire entrypoint) with `tracking()`:

```python
from outbox import tracking

async def entrypoint():
    with tracking():
        async with AsyncSession(db_engine) as session:
            await publisher.publish(session, "user.created", {"id": 123, "username": "johndoe"})
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
    <summary><h4>Topic exchange and wildcard matching</h4></summary>

```python
# Main application
async with AsyncSession(db_engine) as session:
    await publish(session, "user.created", {"id": 123, "username": "johndoe"})
    await session.commit()

# Worker process
@consume(binding_key="user.*", queue="on_user_event")
async def on_user_event(user):
    print(user)
    # <<< {"id": 123, "username": "johndoe"}
```

If you are using this and you want to know the routing key inside the body of the consumer, you can add a `routing_key` argument to the consumer:

```python
# Main application
async with AsyncSession(db_engine) as session:
    await publish(session, "user.created", {"id": 123, "username": "johndoe"})
    await session.commit()

# Worker process
@consume(binding_key="user.*", queue="on_user_event")
async def on_user_event(routing_key: str, user):
    logger.info(f"Received {routing_key=}")
    # <<< Received routing_key=user.created
    print(user)
    # <<< {"id": 123, "username": "johndoe"}
```

</details>

<details>
    <summary><h4>Observability</h4></summary>

Outbox provides optional Prometheus metrics for monitoring message flow and performance. Metrics automatically register to prometheus_client's global registry, so they work seamlessly with your existing Prometheus instrumentation:

```python
from prometheus_client import start_http_server, Counter
from outbox import MessageRelay, Worker

# Your application metrics
my_counter = Counter('my_app_requests_total', 'Total requests')

# Start Prometheus HTTP server once
start_http_server(9090)

# Outbox metrics are automatically included
message_relay = MessageRelay(
    db_engine_url="...",
    rmq_connection_url="...",
    enable_metrics=True,  # Default is True
    exchange_name="orders",
)

worker = Worker(enable_metrics=True, ...)  # Default is True

# Both your metrics and outbox metrics are served at :9090/metrics
```

##### Available Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `outbox_messages_published_total` | Counter | `exchange_name` | Messages successfully published from outbox table |
| `outbox_publish_failures_total` | Counter | `exchange_name`, `failure_type`, `error_type` | Failed publish attempts to RabbitMQ |
| `outbox_message_age_seconds` | Histogram | `exchange_name` | Time message spent in outbox table before publishing |
| `outbox_poll_duration_seconds` | Histogram | `exchange_name` | Time to poll DB and publish one message |
| `outbox_table_backlog` | Gauge | `exchange_name` | Current unsent messages in outbox table |
| `outbox_messages_received_total` | Counter | `queue`, `exchange_name` | Messages received from RabbitMQ queue |
| `outbox_messages_processed_total` | Counter | `queue`, `exchange_name`, `status` | Messages processed with outcome (success/failed/rejected) |
| `outbox_retry_attempts_total` | Counter | `queue`, `delay_seconds` | Retry attempts by delay tier |
| `outbox_message_processing_duration_seconds` | Histogram | `queue`, `exchange_name` | Handler execution time |
| `outbox_dlq_messages` | Gauge | `queue` | Current messages in dead letter queue (updated every 30s) |
| `outbox_active_consumers` | Gauge | `queue`, `exchange_name` | Active consumer connections |

##### Disabling Metrics

```python
message_relay = MessageRelay(enable_metrics=False, ...)
worker = Worker(enable_metrics=False, ...)
```

</details>

### Publisher

<details>
    <summary><h4>API</h4></summary>

##### `Publisher`

Class for publishing messages to the outbox table.

**Constructor parameters:**

- `expiration`: Default expiration time for messages in RabbitMQ. Defaults to `None` (no expiration)
- `table_name`: Name of the outbox table to use. Defaults to `outbox_table`

**Methods:**

- `publish(handle, routing_key, body, *, expiration=None, eta=None)`: Publish a single message to the outbox table. Automatically detects sync vs async based on handle type
  - `handle`: Database connection/session. Accepts:
    - **Async**: `AsyncSession` (SQLAlchemy, if installed) or `asyncpg.Connection`
    - **Sync**: `Session` (SQLAlchemy, if installed), `psycopg2.Connection`, or `psycopg2.Cursor`
  - `routing_key`: The routing key to use for the message
  - `body`: The body of the message. If it is an instance of a Pydantic model, it will be serialized by Pydantic, if it is bytes, it will be used as is, otherwise outbox will attempt to serialize it with `json.dumps`
  - `expiration`: The expiration time in seconds for the message. Overrides the default set in the constructor
  - `eta`: The time at which the message should be sent. Can be a `datetime`, a `timedelta` or an interval in milliseconds

- `publish_async(handle, routing_key, body, *, expiration=None, eta=None)`: Async version of `publish()` (explicit)
- `publish_sync(handle, routing_key, body, *, expiration=None, eta=None)`: Sync version of `publish()` (explicit)

- `bulk_publish(handle, messages)`: Publish multiple messages in a single database operation. Automatically detects sync vs async based on handle type
  - `handle`: Database connection/session (same types as `publish()`)
  - `messages`: A sequence of `OutboxMessage` instances

- `bulk_publish_async(handle, messages)`: Async version of `bulk_publish()` (explicit)
- `bulk_publish_sync(handle, messages)`: Sync version of `bulk_publish()` (explicit)

- `OutboxMessage`: A dataclass representing a message to be published. Used in `bulk_publish()`
  - `routing_key`: The routing key to use for the message
  - `body`: The body of the message. If it is an instance of a Pydantic model, it will be serialized by Pydantic, if it is bytes, it will be used as is, otherwise outbox will attempt to serialize it with `json.dumps`
  - `expiration`: The expiration time in seconds for the message. Overrides the default set in the constructor
  - `eta`: The time at which the message should be sent. Can be a `datetime`, a `timedelta` or an interval in milliseconds

**Note:** SQLAlchemy sessions (`AsyncSession` and `Session`) are fully supported if you have SQLAlchemy installed. The library automatically detects the connection type and extracts the underlying database connection.

</details>

<details>
    <summary><h4>Bulk publish for high throughput</h4></summary>

For performance-critical scenarios where you need to publish many messages at once, use `publisher.bulk_publish()` to insert multiple messages in a single database operation:

```python
from outbox import Publisher, OutboxMessage

publisher = Publisher()

async with AsyncSession(db_engine) as session, session.begin():
    messages = [
        OutboxMessage(routing_key="user.created", body={"id": 1, "username": "alice"}),
        OutboxMessage(routing_key="user.created", body={"id": 2, "username": "bob"}),
        OutboxMessage(routing_key="order.placed", body={"order_id": 456}),
    ]
    await publisher.bulk_publish(session, messages)
```

This is significantly faster than calling `publisher.publish()` individually when dealing with large batches of messages.

</details>

<details>
    <summary><h4>Sync and async publish support</h4></summary>

The `Publisher` supports both synchronous and asynchronous operations, allowing integration with legacy codebases that use blocking I/O:

```python
from outbox import Publisher
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

# Sync setup (for legacy codebases)
db_engine = create_engine("postgresql+psycopg2://user:password@localhost/dbname")
publisher = Publisher()

with Session(db_engine) as session, session.begin():
    session.add(User(id=123, username="johndoe"))
    publisher.publish(session, "user.created", {"id": 123, "username": "johndoe"})
```

The `publish()` and `bulk_publish()` methods automatically detect whether you're using an async handle (`AsyncSession` or `asyncpg.Connection`) or a sync handle (`Session`, `psycopg2.Connection`, or `psycopg2.Cursor`). You can also use the explicit `publish_sync()`/`publish_async()` and `bulk_publish_sync()`/`bulk_publish_async()` methods.

**Installation:** To use sync publishing, install the `noasync` dependency group to add psycopg2:

```bash
uv pip install --group noasync
```

Note: `MessageRelay` and `Worker` remain async-only, as they are standalone processes designed for asynchronous operation.

</details>

<details>
    <summary><h4>Using asyncpg or psycopg2 connections directly</h4></summary>

If you're not using SQLAlchemy, you can pass raw database connections directly to the publisher:

**Async with asyncpg.Connection:**

```python
import asyncpg
from outbox import Publisher

publisher = Publisher()

# Using a single connection
conn = await asyncpg.connect("postgresql://user:password@localhost/dbname")
try:
    # Publish within a transaction
    async with conn.transaction():
        await conn.execute("INSERT INTO users (id, username) VALUES ($1, $2)", 123, "johndoe")
        await publisher.publish(conn, "user.created", {"id": 123, "username": "johndoe"})
finally:
    await conn.close()

# Or using a connection pool
pool = await asyncpg.create_pool("postgresql://user:password@localhost/dbname")
async with pool.acquire() as conn:
    async with conn.transaction():
        await conn.execute("INSERT INTO users (id, username) VALUES ($1, $2)", 456, "janedoe")
        await publisher.publish(conn, "user.created", {"id": 456, "username": "janedoe"})
```

**Sync with psycopg2 connection:**

```python
import psycopg2
from outbox import Publisher

publisher = Publisher()

# Using psycopg2 connection
with psycopg2.connect("postgresql://user:password@localhost/dbname") as conn:
    with conn.cursor() as cursor:
        cursor.execute("INSERT INTO users (id, username) VALUES (%s, %s)", (123, "johndoe"))
    publisher.publish_sync(conn, "user.created", {"id": 123, "username": "johndoe"})
    conn.commit()
```

**Sync with psycopg2 cursor:**

```python
with psycopg2.connect("postgresql://user:password@localhost/dbname") as conn:
    with conn.cursor() as cursor:
        cursor.execute("INSERT INTO users (id, username) VALUES (%s, %s)", (123, "johndoe"))
        publisher.publish_sync(cursor, "user.created", {"id": 123, "username": "johndoe"})
    conn.commit()
```

The publisher works identically with raw connections and SQLAlchemy sessions - choose whichever fits your application architecture.

</details>

<details>
    <summary><h4>Delayed execution</h4></summary>

You can cause an event to be sent some time in the future by setting the `eta` argument during `publisher.publish()`:

```python
async with AsyncSession(db_engine) as session:
    await publisher.publish(
        session,
        "user.created",
        {"id": 123, "username": "johndoe"},
        eta=datetime.datetime.now() + datetime.timedelta(minutes=5),
    )
    await session.commit()
```

</details>

### Message relay

<details>
    <summary><h4>API</h4></summary>

##### `MessageRelay`

Class for relaying messages from the outbox table to RabbitMQ.

**Constructor parameters:**

- `db_engine_url`: PostgreSQL connection string in the format `postgresql://username:password@host:port/database`. Example: `postgresql://postgres:postgres@localhost:5432/mydb`
- `db_pool`: A `asyncpg.ConnectionPool` instance. Can be used in place of `db_engine_url` in case you already have one create or you want to fine-tune its parameters. Create with `await asyncpg.create_pool(self.db_engine_url, min_size=1, max_size=10)`
- `rmq_connection_url`: A string that indicates RabbitMQ connection parameters. Follows the pattern `amqp[s]://<username>:<password>@<host>:(<port>)/(virtualhost)`. Example: `amqp://guest:guest@localhost:5672/`
- `rmq_connection`: If you already have a aio-pika connection, you can pass it here instead of `rmq_connection_url` (you must pass either one or the other)
- `exchange_name`: Name of the RabbitMQ exchange to use. Defaults to `outbox`
- `notification_timeout`: Maximum time (in seconds) to wait for a PostgreSQL NOTIFY before checking for scheduled messages. Acts as a safety timeout. Defaults to `60` seconds
- `expiration`: Default expiration time for messages. Defaults to `None` (no expiration)
- `clean_up_after`: How long to keep messages in the outbox table after they are sent. Can be `IMMEDIATELY`, `NEVER`, or a `timedelta`. Defaults to `IMMEDIATELY`
- `table_name`: Name of the outbox table to use. Defaults to `outbox_table`
- `batch_size`: Number of messages to fetch and publish concurrently in each batch from the outbox table. Defaults to `50`. Higher values improve throughput but increase transaction duration and memory usage. Set to `1` for sequential processing.
- `enable_metrics`: Enable Prometheus metrics. Defaults to `True`

**Methods:**

- `run()`: Start the message relay loop. Blocks until interrupted.

</details>

<details>
    <summary><h4>Outbox table cleanup</h4></summary>

You can choose a strategy for when already sent messages from the outbox table should be cleaned up by passing the `clean_up_after` argument to the MessageRelay constructor:

```python
message_relay = MessageRelay(
    db_engine_url="...",
    rmq_connection_url="...",
    clean_up_after=datetime.timedelta(days=7)
)
```

The options are:

- **`IMMEDIATELY` (the default)**: messages are cleaned up immediately after being sent to RabbitMQ.
- **`NEVER`**: messages are never cleaned up, you will have to do it manually.
- **Any `datetime.timedelta` instance**.

</details>

### Worker

<details>
    <summary><h4>API</h4></summary>

##### `Worker`

Class for consuming messages from RabbitMQ and dispatching them to consumers.

**Constructor parameters:**

- `rmq_connection_url`: RabbitMQ connection string (same as MessageRelay)
- `rmq_connection`: aio-pika connection object (same as MessageRelay)
- `exchange_name`: Name of the RabbitMQ exchange (same as MessageRelay)
- `retry_delays`: Default retry delays (in seconds) for all consumers. A sequence of delay times for exponential backoff. Defaults to `(1, 10, 60, 300)` (1s, 10s, 1m, 5m). Set to `()` for no retries.
- `prefetch_count`: Number of messages to prefetch from RabbitMQ for each consumer. Defaults to `10`
- `enable_metrics`: Enable Prometheus metrics. Defaults to `True`
- `consumers`: A sequence of `Consumer` instances to consume messages for. Defaults to `()`

**Methods:**

- `run()`: Start the worker consuming messages. Blocks until SIGINT/SIGTERM is received.

##### `consume()`

A decorator that creates a `Consumer` instance from a function.

**Positional arguments:**

- `binding_key`: The binding key to use for the consumer. Supports wildcards, e.g. `user.*` will match `user.created`, `user.updated`, etc, according to RabbitMQ's topic exchange rules

**Keyword-only arguments:**

- `queue`: The name of the queue to use for the consumer. If not provided (empty string), a name based on the callback's module and qualname will be auto-generated
- `retry_delays`: Retry delays (in seconds) for this consumer. Overrides the Worker's default retry_delays. Set to `()` for no retries.

Returns a `Consumer` instance that is also callable (delegates to the original function).

##### `Consumer`

A dataclass representing a message handler.

**Fields:**

- `binding_key`: The binding key pattern to match messages against
- `callback`: The async function to call when a message is received
- `queue`: The queue name (auto-generated from callback if empty string)
- `retry_delays`: Retry delays in seconds for this consumer (None means use Worker's default)

The `Consumer` instance is callable and will delegate to the `callback` function, making it transparent for testing.

</details>

<details>
    <summary><h4>Retries and dead-lettering</h4></summary>

When a consumer raises an exception, the library implements delayed retries using RabbitMQ's TTL-based delay queues. Messages that fail are sent to a delay queue, and after the configured delay expires, they are automatically routed back to the original queue for retry.

**Configuring retry delays:**

You can control retry behavior by configuring `retry_delays` - a sequence of delay duration strings. For example, `retry_delays=("1s", "10s", "1m", "5m")` means:

- First failure: wait 1 second before retry (attempt 2)
- Second failure: wait 10 seconds before retry (attempt 3)
- Third failure: wait 1 minute before retry (attempt 4)
- Fourth failure: wait 5 minutes before retry (attempt 5)
- Fifth failure: send to dead-letter queue

**Duration format** (from `parse_duration` function):

Valid inputs: `0`, `0m`, `0s`, `0ms`, `1m`, `30s`, `500ms`, `1m30s`, `1m500ms`, `30s500ms`, `1m30s500ms`

Invalid inputs: `1h`, `60m`, `61m`, `60s`, `61s`, `1000ms`, `1001ms`, `01m`, `01s`, `01ms`, combinations of these and negative values

Configure retry delays at two levels, with per-consumer overriding global:

```python
from outbox import Worker, consume, Reject

# Per-consumer override
@consume(binding_key="user.created", queue="on_user_created", retry_delays=("5s", "30s"))  # Only 2 retries with these delays
async def on_user_created(user):
    if some_transient_error:
        raise Exception("Will retry with configured delays")
    if some_permanent_error:
        raise Reject()  # Skip retries, send directly to DLQ

# Fast retries for time-sensitive operations
@consume(binding_key="order.created", queue="on_order_created", retry_delays=("500ms", "2s", "5s"))
async def on_order_created(order):
    pass

# Disable retries for a specific consumer
@consume(binding_key="user.deleted", queue="on_user_deleted", retry_delays=())  # No retries - straight to DLQ on failure
async def on_user_deleted(user):
    pass  # Failures go directly to DLQ

# Global default for all consumers
worker = Worker(retry_delays=("1s", "10s", "1m", "5m"), ...)
```

**Special cases:**

- **Empty retry_delays (`()`)**: No retries - failures send message directly to dead-letter queue. Useful when you want failures to be handled manually.
- **`Reject` exception**: Sends message directly to dead-letter queue, bypassing all retries. Same effect as raising any exception with `retry_delays=()`.
- **Message expiration**: You can still set `expiration` during `publisher.publish()` to limit total processing time regardless of retries.

**Dead-letter queues:**

If a message fails after exhausting all retry attempts (or is explicitly rejected), it's sent to a dead-letter exchange and then to a dead-letter queue. One dead-letter queue is created for each regular queue with the suffix `.dlq`. When you encounter messages in dead-letter queues, you can:

1. Inspect logs to understand what went wrong
2. Fix the code and restart the worker
3. Use RabbitMQ's shovel interface to move messages back to their respective queues for reprocessing

**Important - Idempotency:**

The library implements **at-least-once delivery semantics**, meaning messages may be delivered multiple times. Your handlers **must be idempotent** to handle duplicate deliveries correctly. This can happen due to:

- Retries after failures
- Network issues or worker restarts
- RabbitMQ redeliveries

```python
@consume(binding_key="order.created", queue="process_order")
async def process_order(order_id: int):
    # ✅ Good: Check if already processed
    if await is_order_processed(order_id):
        return  # Skip duplicate

    await process_order_logic(order_id)
    await mark_order_processed(order_id)
```

</details>

<details>
    <summary><h4>Graceful shutdown</h4></summary>

When the worker receives a SIGINT or SIGTERM, it will request a disconnect from all the queues. Any messages that are sent before the disconnect request is processed will be rejected by the worker with `requeue=True` (so they will be consumed by other workers, immediately or later). In the meantime, any messages that have already started being processed will keep being processed until the consumer function terminates. When all pending tasks have finished, the worker will exit.

Example sequence of events:

```mermaid
sequenceDiagram
    participant Pub as Publisher
    participant Q as RabbitMQ Queue
    participant W as Worker
    participant OW as Other Worker

    W->>Q: Start and connect
    Pub->>Q: Publish message 1
    Q-->>W: Send message 1
    W->>W: Start processing message 1

    Note right of W: SIGINT or SIGTERM received
    W->>Q: Request disconnect from all queues

    Pub->>Q: Publish message 2
    Q-->>W: Send message 2
    W->>Q: Reject message 2 (requeue=True)
    Note left of Q: message 2 held in the queue

    Q-->>W: Acknowledge disconnect request

    Pub->>Q: Publish message 3
    Note right of Q: Message 3 not sent to W

    W->>W: Finish processing message 1
    W->>Q: Ack message 1
    W->>W: Exit

    OW->>Q: Start and connect
    Q-->>OW: Send message 2
    Q-->>OW: Send message 3
    OW->>OW: Process message 2
    OW->>OW: Process message 3
    OW->>Q: Ack message 2
    OW->>Q: Ack message 3
```

</details>

<details>
    <summary><h4>Consumer callback arguments</h4></summary>

Given everything we have discussed so far, the worker will populate the arguments of your consumer functions based on their names and/or type annotations.

If you arguments are named:

- `routing_key`: it will be populated with the routing key of the message (this may be useful if the binding key of the queue uses wildcards)
- `message`: it will be populated with the raw aio-pika message object
- `tracking_ids`: it will be populated with the tracking IDs of the message
- `attempt_count`: it will be populated with the number of attempts that have been made to process the message (starting from 1)

You must have exactly **one** argument that doesn't meet the above criteria, which will be populated with the body of the message. If you supply a type annotation that is a subclass of `pydantic.BaseModel`, the library will automatically deserialize the body into an instance of that class. If you don't supply a type annotation, the library will attempt to deserialize it with `json.loads`. If that fails, you will receive the contents of the message as bytes.

</details>

<details>
    <summary><h4>Blocking I/O and CPU-bound work</h4></summary>

The outbox pattern is designed for **async I/O-bound operations** like sending emails, calling external APIs, or writing to databases. For these tasks, the library's async approach using `asyncio.create_task()` provides excellent concurrency without blocking.

However, there are two scenarios where you might need different handling:

##### Blocking I/O (Legacy Codebases)

If you're integrating the outbox pattern into an existing codebase with **synchronous/blocking I/O** code, the `@consume` decorator automatically detects and handles this. You can use regular (non-async) functions as callbacks:

```python
from outbox import consume

# Async callback (preferred for new code)
@consume(binding_key="user.created", queue="async_handler")
async def async_handler(user):
    await send_email_async(user)  # Non-blocking async I/O

# Sync callback (for legacy code with blocking I/O)
@consume(binding_key="order.created", queue="sync_handler")
def sync_handler(order):
    send_email_blocking(order)  # Blocking I/O - automatically runs in thread pool
    update_crm_blocking(order)   # Blocking I/O - automatically runs in thread pool
```

**How it works:**

- The library detects if your callback is sync or async using `asyncio.iscoroutinefunction()`
- Sync callbacks are automatically wrapped with `asyncio.to_thread()` to run in a thread pool
- This prevents blocking the event loop while maintaining compatibility with legacy code
- Concurrency is controlled by `prefetch_count` - at most N callbacks (sync or async) run simultaneously

**Performance notes:**

- Thread pool overhead exists but is minimal for I/O-bound work
- Prefer async callbacks for new code - they're more efficient
- This feature enables **gradual migration** from sync to async code

For **CPU-bound work**, see Best Practices / Worker.

</details>

### Utils

<details>
    <summary><h4>API</h4></summary>

##### `ensure_outbox_table_async(db, table_name="outbox_table")`

Ensure the outbox table exists with proper schema, indexes, and triggers (async version).

**Parameters:**
- `db`: PostgreSQL connection URL string or `asyncpg.Connection` object
- `table_name`: Table name. Defaults to `"outbox_table"`

**Returns:** None. Idempotent - safe to call multiple times.

##### `ensure_outbox_table_sync(db, table_name="outbox_table")`

Ensure the outbox table exists with proper schema, indexes, and triggers (sync version).

**Parameters:**
- `db`: PostgreSQL connection URL string or `psycopg2.Connection` object
- `table_name`: Table name. Defaults to `"outbox_table"`

**Returns:** None. Idempotent - safe to call multiple times.

##### `get_tracking_ids()`

Get the current tracking IDs from context.

**Returns:** Tuple of UUID strings representing the tracking chain.

##### `tracking()`

Context manager that adds a UUID to the tracking ID chain. Use at entrypoints (API handlers, etc.) to include an identifier for the originating operation.

**Returns:** Context manager.

</details>

## Best Practices

### Overall

<details>
    <summary><h4>Logging</h4></summary>

The library logs important events (published messages, processing results, retries, errors) using Python's standard logging module under the logger name `"outbox"`. Since the outbox pattern handles critical infrastructure (message processing, retries, dead-letter queues), **logs are enabled by default** to ensure you're aware of any issues.

If you haven't configured logging in your application, you'll see log output automatically. To control the log level or disable logs entirely:

```python
import logging

# Configure logging for your entire application
logging.basicConfig(level=logging.INFO)

# Or control just the outbox logger
logging.getLogger("outbox").setLevel(logging.WARNING)  # Only warnings and errors

# Or disable outbox logs entirely
logging.getLogger("outbox").setLevel(logging.CRITICAL)

# Or disable propagation to root logger
logging.getLogger("outbox").propagate = False
```

The library logs at these levels:

- **DEBUG**: Detailed information about queue bindings, message polling
- **INFO**: Normal operations (messages published, processed successfully)
- **WARNING**: Retries, rejections, messages sent to dead-letter queues
- **ERROR**: Failures during deserialization or unexpected errors

For production, `logging.INFO` is recommended so you can track message flow without excessive noise.

</details>

<details>
    <summary><h4>Database setup</h4></summary>

The outbox pattern requires a database table to store messages before they're published to RabbitMQ. You can set this up in different ways depending on your environment:

##### Quick Setup (Development)

For development environments, you can use the provided utility functions to create the outbox table at application startup:

```python
import asyncio
from outbox.utils import ensure_outbox_table_async, ensure_outbox_table_sync

# Async version (recommended)
async def setup():
    await ensure_outbox_table_async("postgresql://user:password@localhost/dbname")

asyncio.run(setup())

# Or sync version
from outbox.utils import ensure_outbox_table_sync
ensure_outbox_table_sync("postgresql://user:password@localhost/dbname")
```

These functions are **idempotent** (get-or-create) - they're safe to call multiple times and will only create the table if it doesn't already exist. They create:

- The `outbox_table` with all necessary columns
- Indexes for efficient querying
- PostgreSQL NOTIFY trigger for instant message delivery

**Note:** This approach is convenient for development but for production environments, you should use a proper migration tool (see below) to have better control over your database schema.

##### Using Alembic (Production)

If you manage database migrations using Alembic, before deploying the code that uses this library, you should create a new empty migration:

```sh
alembic revision -m "Create outbox table"
```

Then, edit the generated migration file to include the following code:

```python
from alembic import op
import sqlalchemy as sa

def upgrade():
    op.create_table(
        "outbox_table",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("routing_key", sa.Text, nullable=False),
        sa.Column("body", sa.LargeBinary, nullable=False),
        sa.Column("tracking_ids", sa.JSON, nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        sa.Column("expiration", sa.Interval),
        sa.Column("send_after", sa.DateTime(timezone=True), nullable=False),
        sa.Column("sent_at", sa.DateTime(timezone=True)),
    )

    # Partial index: pending messages
    op.create_index(
        "outbox_pending_idx",
        "outbox_table",
        ["send_after", "created_at"],
        postgresql_where=sa.text("sent_at IS NULL"),
    )

    # Partial index: cleanup
    op.create_index(
        "outbox_cleanup_idx",
        "outbox_table",
        ["sent_at"],
        postgresql_where=sa.text("sent_at IS NOT NULL"),
    )

    # Create NOTIFY trigger for instant message delivery
    op.execute("""
        CREATE OR REPLACE FUNCTION notify_outbox_insert() RETURNS TRIGGER AS $$
        BEGIN
            IF NEW.send_after <= NOW() THEN
                PERFORM pg_notify('outbox_channel', '');
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
    """)

    op.execute("""
        CREATE TRIGGER outbox_notify_trigger
            AFTER INSERT ON outbox_table
            FOR EACH ROW
            EXECUTE FUNCTION notify_outbox_insert()
    """)

def downgrade():
    op.execute("DROP TRIGGER IF EXISTS outbox_notify_trigger ON outbox_table")
    op.execute("DROP FUNCTION IF EXISTS notify_outbox_insert()")
    op.drop_index("outbox_pending_idx", table_name="outbox_table")
    op.drop_index("outbox_cleanup_idx", table_name="outbox_table")
    op.drop_table("outbox_table")
```

##### Using raw SQL

If you manage your database schema with SQL, run the following commands:

```sql
CREATE TABLE outbox_table (
    id SERIAL PRIMARY KEY,
    routing_key TEXT NOT NULL,
    body BYTEA NOT NULL,
    tracking_ids JSON NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expiration INTERVAL,
    send_after TIMESTAMPTZ NOT NULL,
    sent_at TIMESTAMPTZ
);

-- Partial index for pending messages
CREATE INDEX outbox_pending_idx
ON outbox_table (send_after, created_at)
WHERE sent_at IS NULL;

-- Partial index for cleanup
CREATE INDEX outbox_cleanup_idx
ON outbox_table (sent_at)
WHERE sent_at IS NOT NULL;

-- Create NOTIFY trigger for instant message delivery
CREATE OR REPLACE FUNCTION notify_outbox_insert() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.send_after <= NOW() THEN
        PERFORM pg_notify('outbox_channel', '');
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER outbox_notify_trigger
    AFTER INSERT ON outbox_table
    FOR EACH ROW
    EXECUTE FUNCTION notify_outbox_insert();
```

</details>

<details>
    <summary><h4>Pre-provisioning RabbitMQ Resources</h4></summary>

## The Problem

By default, the outbox library automatically creates all required RabbitMQ exchanges, queues, and bindings when the worker starts. While convenient for development, this can cause issues in production environments:

- **Orphaned resources**: When code is removed or refactored, queues and exchanges may remain in RabbitMQ, potentially accumulating messages
- **Lack of oversight**: No central visibility into what RabbitMQ resources exist across your infrastructure
- **Configuration drift**: Resources created ad-hoc by applications may have inconsistent settings
- **Security concerns**: Applications shouldn't have permission to create/delete infrastructure

**Solution**: Pre-provision all RabbitMQ resources using infrastructure-as-code (Terraform, Ansible, etc.) and configure RabbitMQ permissions to prevent applications from creating resources.

## RabbitMQ Resources Created by Outbox

The outbox library creates the following RabbitMQ resources:

### Exchanges

| Name Pattern | Type | Durable | Purpose |
|--------------|------|---------|---------|
| `{exchange_name}` | TOPIC | Yes | Main exchange for routing messages from relay to consumers |
| `{exchange_name}.dlx` | DIRECT | Yes | Dead letter exchange for failed messages |
| `{exchange_name}.delay_{N}s` | FANOUT | Yes | Delay exchange for retry backoff (one per unique delay value) |

Default value for `exchange_name` is `"outbox"`.

### Queues

| Name Pattern | Durable | Arguments | Purpose |
|--------------|---------|-----------|---------|
| `{consumer.queue}` | Yes | `x-dead-letter-exchange`: `{exchange_name}.dlx`<br>`x-dead-letter-routing-key`: `{consumer.queue}`<br>`x-queue-type`: `quorum` | Consumer's main queue |
| `{consumer.queue}.dlq` | Yes | `x-queue-type`: `quorum` | Dead letter queue for messages that exhausted retries |
| `{exchange_name}.delay_{N}s` | Yes | `x-message-ttl`: `{N * 1000}` ms<br>`x-dead-letter-exchange`: `""` (default exchange)<br>`x-queue-type`: `quorum` | Delay queue for retry backoff (one per unique delay value). Routes back to specific queue via default exchange. |

### Bindings

| Exchange | Binding | Queue | Purpose |
|----------|---------|-------|---------|
| `{exchange_name}` | `{consumer.binding_key}` | `{consumer.queue}` | Routes messages to consumer (supports wildcards like `user.*`) |
| `{exchange_name}.dlx` | `{consumer.queue}` | `{consumer.queue}.dlq` | Routes failed messages to DLQ |
| `{exchange_name}.delay_{N}s` | (none - fanout) | `{exchange_name}.delay_{N}s` | Routes messages to delay queue |

**Key insight**: Delay exchanges/queues are **shared** across all consumers. The number created depends on the **unique set** of delay values across global `setup(retry_delays=...)` and all per-consumer `@consume(retry_delays=...)` overrides.

<details>
    <summary><h4>Terraform: Configuration Variables</h4></summary>

```hcl
terraform {
  required_providers {
    rabbitmq = {
      source  = "cyrilgdn/rabbitmq"
      version = "~> 1.8"
    }
  }
}

provider "rabbitmq" {
  endpoint = "http://localhost:15672"
  username = "admin"
  password = "admin"
}

# Variables for configuration
locals {
  exchange_name = "outbox"
  retry_delays  = [1, 10, 60, 300]  # Must match your Worker(retry_delays=...)

  consumers = [
    {
      queue       = "orders.user_handler"
      binding_key = "user.created"
    }
  ]
}
```

**Important notes:**

1. **Sync retry_delays**: The `local.retry_delays` list in Terraform **must match** your `Worker(retry_delays=...)` as well as all your retry_delays overrides in `@consume()` decorators
2. **Sync consumer queues**: The `local.consumers` list must include all your `@consume()` decorators

</details>

<details>
    <summary><h4>Terraform: Exchanges, Queues, and Bindings</h4></summary>

```hcl
# Main topic exchange
resource "rabbitmq_exchange" "main" {
  name  = local.exchange_name
  vhost = "/"

  settings {
    type    = "topic"
    durable = true
  }
}

# Dead letter exchange
resource "rabbitmq_exchange" "dlx" {
  name  = "${local.exchange_name}.dlx"
  vhost = "/"

  settings {
    type    = "direct"
    durable = true
  }
}

# Delay exchanges (one per unique delay value)
resource "rabbitmq_exchange" "delay" {
  for_each = toset([for d in local.retry_delays : tostring(d)])

  name  = "${local.exchange_name}.delay_${each.key}s"
  vhost = "/"

  settings {
    type    = "fanout"
    durable = true
  }
}

# Delay queues (one per unique delay value)
resource "rabbitmq_queue" "delay" {
  for_each = toset([for d in local.retry_delays : tostring(d)])

  name  = "${local.exchange_name}.delay_${each.key}s"
  vhost = "/"

  settings {
    durable = true
    arguments = {
      "x-message-ttl"           = tonumber(each.key) * 1000
      "x-dead-letter-exchange"  = ""  # Default exchange (routes by queue name)
      "x-queue-type"            = "quorum"
    }
  }
}

# Bind delay queues to their delay exchanges
resource "rabbitmq_binding" "delay" {
  for_each = toset([for d in local.retry_delays : tostring(d)])

  source      = "${local.exchange_name}.delay_${each.key}s"
  vhost       = "/"
  destination = "${local.exchange_name}.delay_${each.key}s"
  destination_type = "queue"
  routing_key = ""  # Fanout exchange ignores routing key

  depends_on = [
    rabbitmq_exchange.delay,
    rabbitmq_queue.delay
  ]
}

# Consumer queues
resource "rabbitmq_queue" "consumer" {
  for_each = { for idx, l in local.consumers : l.queue => l }

  name  = each.value.queue
  vhost = "/"

  settings {
    durable = true
    arguments = {
      "x-dead-letter-exchange"    = "${local.exchange_name}.dlx"
      "x-dead-letter-routing-key" = each.value.queue
      "x-queue-type"              = "quorum"
    }
  }
}

# Bind consumer queues to main exchange
resource "rabbitmq_binding" "consumer" {
  for_each = { for idx, l in local.consumers : l.queue => l }

  source      = local.exchange_name
  vhost       = "/"
  destination = each.value.queue
  destination_type = "queue"
  routing_key = each.value.binding_key

  depends_on = [
    rabbitmq_exchange.main,
    rabbitmq_queue.consumer
  ]
}

# Dead letter queues
resource "rabbitmq_queue" "dlq" {
  for_each = { for idx, l in local.consumers : l.queue => l }

  name  = "${each.value.queue}.dlq"
  vhost = "/"

  settings {
    durable = true
    arguments = {
      "x-queue-type" = "quorum"
    }
  }
}

# Bind DLQs to dead letter exchange
resource "rabbitmq_binding" "dlq" {
  for_each = { for idx, l in local.consumers : l.queue => l }

  source      = "${local.exchange_name}.dlx"
  vhost       = "/"
  destination = "${each.value.queue}.dlq"
  destination_type = "queue"
  routing_key = each.value.queue

  depends_on = [
    rabbitmq_exchange.dlx,
    rabbitmq_queue.dlq
  ]
}
```

</details>

<details>
    <summary><h4>Terraform: User and Permissions</h4></summary>

```hcl
# Create restricted application user
resource "rabbitmq_user" "app" {
  name     = "myapp"
  password = "secure_password_here"
  tags     = []  # No admin tags
}

# Grant permissions: no configure, limited write, full read
resource "rabbitmq_permissions" "app" {
  user  = rabbitmq_user.app.name
  vhost = "/"

  permissions {
    configure = "^$"  # Cannot create/delete any resources
    write     = "^({exchange_name}|{exchange_name}\\.delay_.*)$"  # Can publish to main exchange and delay exchanges
    read      = ".*"  # Can consume from any queue
  }
}
```

The permissions configured above:

```hcl
configure = "^$"  # Regex matches nothing - no resource creation allowed
write     = "^({exchange_name}|{exchange_name}\\.delay_.*)$"  # Can publish to main and delay exchanges
read      = ".*"  # Can consume from all queues
```

**Permission types:**

- **Configure**: Create and delete resources (exchanges, queues, bindings)
- **Write**: Publish messages to exchanges
- **Read**: Consume messages from queues

**Why these settings:**

- **`configure = "^$"`**: Prevents application from creating/deleting any RabbitMQ resources
- **`write = "^({exchange_name}|{exchange_name}\\.delay_.*)$"`**: Allows publishing to main exchange (for message relay) and delay exchanges (for retries)
- **`read = ".*"`**: Allows consuming from all queues

**Note**: Consumers need write access to delay exchanges because when a message fails and needs to retry, the consumer publishes it to a delay exchange (e.g., `outbox.delay_5s`).

</details>

<details>
    <summary><h4>What Happens Without Pre-provisioning</h4></summary>

If you configure RabbitMQ permissions to prevent resource creation but don't pre-create resources, you'll see errors **when the worker starts up** (during `_set_up_queues()`), not during task execution:

### Missing Exchange Error

```python
aio_pika.exceptions.ChannelPreconditionFailed: ACCESS_REFUSED - access to exchange '{exchange_name}.delay_10s'
in vhost '/' refused for user 'myapp'
```

**Cause**: Worker tried to create delay exchange at startup but lacks configure permission

**Solution**: Create the exchange in Terraform

### Missing Queue Error

```python
aio_pika.exceptions.ChannelPreconditionFailed: ACCESS_REFUSED - access to queue 'myapp.user_handler'
in vhost '/' refused for user 'myapp'
```

**Cause**: Worker tried to create consumer queue at startup but lacks configure permission

**Solution**: Create the queue in Terraform

### Retry Failure (Missing Delay Exchange)

```python
aio_pika.exceptions.ChannelPreconditionFailed: NOT_FOUND - no exchange '{exchange_name}.delay_5s' in vhost '/'
```

**Cause**: Added `retry_delays=(5, 15)` to a consumer but didn't create corresponding delay exchanges/queues in Terraform

**Solution**: Update `local.retry_delays` in Terraform to include new delay values, run `terraform apply`

### Publishing Failure (Missing Write Permission)

```python
aio_pika.exceptions.ChannelPreconditionFailed: ACCESS_REFUSED - access to exchange '{exchange_name}.delay_10s'
in vhost '/' refused for user 'myapp'
```

**Cause**: Consumer tried to publish to delay exchange during retry but lacks write permission

**Solution**: Update write regex in `rabbitmq_permissions` to include delay exchanges: `"^({exchange_name}|{exchange_name}\\.delay_.*)$"`

**All these errors occur at worker startup**, making it easy to catch configuration issues before processing any messages.

</details>

## Development vs Production

**Recommended approach:**

**Development/Local:**

- Use RabbitMQ with default permissions (guest/guest with full access)
- Let outbox auto-create resources for fast iteration
- No Terraform needed

**Production:**

- Pre-create all resources via Terraform
- Use restricted RabbitMQ user with `configure = "^$"`
- Enforce infrastructure-as-code discipline

The library's declarative approach (`declare_exchange`/`declare_queue`) means:

- If resources exist with correct config → succeeds silently
- If resources don't exist and user has permission → creates them
- If resources don't exist and user lacks permission → fails with error at worker startup

</details>

### Publisher

<details>
    <summary><h4>Publish inside database transaction</h4></summary>

You can (and should) call `publisher.publish` inside a database transaction. This way, data creation and triggering of side-effects will either succeed together or fail together. This is the main goal of the outbox pattern.

```python
async with AsyncSession(db_engine) as session, session.begin():
    session.add(User(id=123, username="johndoe"))
    await publisher.publish(session, "user.created", {"id": 123, "username": "johndoe"})
    # commit not needed because of `session.begin()`
```

</details>

### Worker

<details>
    <summary><h4>Queue Naming for Multi-Service Deployments</h4></summary>

When running multiple services, prefix queue names with your service identifier for clear ownership:

```python
# orders-service
worker = Worker(
    exchange_name="events",
    consumers=[
        consume(binding_key="payment.completed", queue="orders.on_payment_completed")(handler),
        consume(binding_key="user.created", queue="orders.on_user_created")(handler),
    ]
)

# payments-service
worker = Worker(
    exchange_name="events",
    consumers=[
        consume(binding_key="order.created", queue="payments.on_order_created")(handler),
    ]
)
```

Benefits: clear ownership in RabbitMQ UI, simplified infrastructure-as-code, easy filtering by service.

</details>

<details>
    <summary><h4>CPU-Bound Work</h4></summary>

If your consumer needs to perform **CPU-intensive work** (image processing, data transformations, heavy computations), you should offload it to a **process pool** (not thread pool). This is **not built into the library** because:

1. Most outbox use cases are I/O-bound, not CPU-bound
2. Users have different needs (process pools, thread pools, custom executors)
3. Python's standard library already makes this straightforward

Here's how to handle CPU-bound work:

```python
import asyncio
from concurrent.futures import ProcessPoolExecutor
from outbox import consume

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

@consume(binding_key="image.uploaded", queue="process_image")
async def process_image(image_data: bytes):
    """Consumer remains async and non-blocking"""
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

**Why process pool instead of thread pool for CPU work?**

- Python's GIL (Global Interpreter Lock) prevents true parallelism in threads for CPU-bound tasks
- Process pools bypass the GIL by using separate processes
- For blocking I/O, threads are sufficient (and more efficient) because I/O releases the GIL

This pattern gives you complete control over parallelism while keeping the library focused and simple.

</details>

<details>
    <summary><h4>Connection resilience</h4></summary>

The library uses `aio_pika.connect_robust()` for RabbitMQ connections, which provides automatic reconnection with full state recovery (queues, exchanges, bindings, and consumers). If RabbitMQ restarts or network issues occur, connections automatically recover without manual intervention.

**PostgreSQL - Connection Pooling:**

If you're using SQLAlchemy, configure your engine with connection pooling:

```python
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from outbox import Publisher

# Configure engine with pooling
db_engine = create_async_engine(
    "postgresql+asyncpg://user:password@localhost/dbname",
    pool_pre_ping=True,        # Test connections before use (recommended)
    pool_recycle=3600,         # Recycle connections after 1 hour
    pool_size=5,               # Connection pool size
    max_overflow=10,           # Max connections beyond pool_size
)

publisher = Publisher()

# Use engine to create sessions
async with AsyncSession(db_engine) as session:
    await publisher.publish(session, ...)
```

**Key parameters:**

- `pool_pre_ping=True` - Most important! Tests if a connection is alive before use, prevents "connection closed" errors
- `pool_recycle=3600` - Closes and recreates connections after specified seconds to prevent stale connections
- `pool_size` and `max_overflow` - Control connection pool sizing for your workload

If you're using asyncpg directly, you can create a connection pool:

```python
import asyncpg
from outbox import Publisher

# Create connection pool
pool = await asyncpg.create_pool(
    "postgresql://user:password@localhost/dbname",
    min_size=5,
    max_size=10,
)

publisher = Publisher()

# Use connection from pool
async with pool.acquire() as conn:
    await publisher.publish(conn, ...)
```

For `MessageRelay`, the `db_engine_url` parameter is used, and asyncpg handles connection pooling automatically. You can tune pool parameters in the connection URL if needed, or create your own pool for more control. Or you can pass the `db_pool` parameter in place of `db_engine_url` to create and configure the connection pool yourself.

**RabbitMQ - Configure heartbeat and other connection parameters:**

```python
import aio_pika

rmq_connection = await aio_pika.connect_robust(
    "amqp://guest:guest@localhost:5672/",
    heartbeat=30,              # Send heartbeats every 30 seconds (default: 60)
)

message_relay = MessageRelay(db_engine_url="...", rmq_connection=rmq_connection)
worker = Worker(rmq_connection=rmq_connection, ...)
```

Heartbeats detect dead connections and trigger automatic reconnection. The default timeout (60s) works for most cases, but you can adjust it based on your network reliability

</details>

### Benchmarks

The outbox pattern scales according to your needs - from ~3,500 msgs/sec with a single worker/relay to higher throughput with additional workers and relays. Performance is comparable to pure Celery, with the relay introducing minimal overhead despite providing transactional guarantees.

See [detailed benchmark results](src/benchmarks/README.md) for throughput scaling, parameter tuning recommendations, and Celery comparison.

## TODOs

### High priority

- [ ] Graceful shutdown for message relay

### Medium priority

- [ ] Terraform examples to use string delays like "5s", "1m"

### Low priority

- [ ] Use msgpack (optionally) to reduce size
