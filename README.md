# Outbox pattern for Python, SQLAlchemy, RabbitMQ and Pydantic

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
        await emit(
            session, "user.created", {"id": 123, "username": "johndoe"}, commit=True
        )

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
    # `commit=True` not needed because of `session.begin()`
    await emit(session, "user.created", {"id": 123, "username": "johndoe"})
```

### Topic exchange and wildcard matching

```python
# Main application
async with AsyncSession(db_engine) as session:
    await emit(
        session, "user.created", {"id": 123, "username": "johndoe"}, commit=True
    )

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
    await emit(
        session, "user.created", User(id=123, username="johndoe"), commit=True
    )

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

## TODOs

- Clean up outbox table
- Use pg notify/listen to avoid polling the database
- Use msgpack (optionally) to reduce size
- Support binary payloads (without base64)
- Dependency injection on listen
