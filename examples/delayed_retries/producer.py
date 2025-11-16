"""
Producer example - Emits messages to demonstrate delayed retries.

This script emits various types of messages that will be processed by the consumer
with different retry behaviors.
"""
import asyncio
import logging
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from outbox import setup, emit

# Configure logging to see what's happening
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Database and RabbitMQ connection strings
DB_URL = "sqlite+aiosqlite:///outbox_delayed_retries_demo.db"
RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"


async def main():
    # Create database engine
    db_engine = create_async_engine(DB_URL, echo=False)

    # Setup outbox with exponential backoff delays: 2s, 5s, 15s
    setup(
        db_engine=db_engine,
        rmq_connection_url=RABBITMQ_URL,
        retry_delays=(2, 5, 15)  # 3 retry attempts with increasing delays
    )

    print("\n" + "="*70)
    print("Producer - Emitting test messages")
    print("="*70 + "\n")

    async with AsyncSession(db_engine) as session:
        # Message 1: Will fail and retry with exponential backoff
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Emitting 'order.process' - will fail and retry")
        await emit(session, "order.process", {
            "order_id": 123,
            "customer": "John Doe",
            "total": 99.99
        })

        # Message 2: Will succeed on first attempt
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Emitting 'order.notify' - will succeed immediately")
        await emit(session, "order.notify", {
            "order_id": 456,
            "customer": "Jane Smith",
            "email": "jane@example.com"
        })

        # Message 3: Will be rejected immediately
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Emitting 'order.invalid' - will be rejected to DLQ")
        await emit(session, "order.invalid", {
            "order_id": 999,
            "bad_data": True
        })

        await session.commit()

    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] All messages emitted successfully!")
    print("Check the consumer logs to see the retry behavior.\n")
    print("Tip: Watch the RabbitMQ management UI at http://localhost:15672")
    print("     (username: guest, password: guest)")
    print("     You'll see delay queues being created and messages moving between them.\n")

    await db_engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
