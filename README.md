# Outbox pattern for Python, SQLAlchemy, RabbitMQ and Pydantic

## Usage

### Main application

```python
import asyncio

from outbox import setup_async, emit
from sqlalchemy.ext.asyncio import create_async_engine

db_engine = create_async_engine("postgresql+asyncpg://user:password@localhost/dbname")

async def main():
    await setup_async(db_engine=db_engine)

    async with AsyncSession(db_engine) as session:
        await emit(
            session, "user.created", {"user_id": 123, "username": "johndoe"}, commit=True
        )

asyncio.run(main())
```

No need for migrations, `setup_async` will get-or-create the outbox table automatically.

### Message relay process

```python
import asyncio

from outbox import setup_async, message_relay

async def main():
    await setup_async(
        db_engine_url="postgresql+asyncpg://user:password@localhost/dbname",
        rabbitmq_url="amqp://user:password@localhost:5672/",
    )
    await message_relay()

asyncio.run(main())
```

### Worker process

```python
import asyncio

from outbox import setup_async, listen, worker

@listen("user.created")
async def on_user_created(user):
    print(user)
    # <<< {"user_id": 123, "username": "johndoe"}

async def main():
    await setup_async(rabbitmq_url="amqp://user:password@localhost:5672/")
    await worker()

asyncio.run(main())
```

## Features

### Emit inside database transaction

You can (should) call `emit` inside a database transaction. This way, data creation and triggering of side-effects will either succeed together or fail together.

```python
async with AsyncSession(db_engine) as session, session.begin():
    session.add(User(user_id=123, username="johndoe"))
    # `commit=True` not needed because of `session.begin()`
    await emit(session, "user.created", {"user_id": 123, "username": "johndoe"})
```

### Topic exchange and wildcard matching

```python
# Main application
async with AsyncSession(db_engine) as session:
    await emit(
        session, "user.created", {"user_id": 123, "username": "johndoe"}, commit=True
    )

# Worker process
@listen("user.*")
async def on_user_event(user):
    print(user)
    # <<< {"user_id": 123, "username": "johndoe"}
```

### Automatic (de)serialization of Pydantic models

```python
class User(BaseModel):
    user_id: int
    username: str

# Main application
async with AsyncSession(db_engine) as session:
    await emit(
        session, "user.created", User(user_id=123, username="johndoe"), commit=True
    )

# Worker process
@listen("user.created")
async def on_user_created(user: User):  # inspects type annotation
    print(user)
    # <<< User(user_id=123, username="johndoe")
```

# TODOs

- Add name parameter to listen
- Dependency injection on listen
- Improve message relay polling
- Use pg notify/listen to avoid polling the database
- Only async constructor
- Pass routing key to handler
- Verify that retries work
- Figure out a way to not retry on some exceptions
- Clean up outbox table
- Use msgpack (optionally) to reduce size
- Support binary payloads (without base64)
