"""
Message Relay - Transfers messages from the database outbox to RabbitMQ.

This process continuously polls the outbox table and publishes messages to RabbitMQ.
It's a separate process from the producer and consumer.
"""

import asyncio
import logging

from outbox import message_relay, setup

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# Database and RabbitMQ connection strings
DB_URL = "sqlite+aiosqlite:///outbox_delayed_retries_demo.db"
RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"


async def main() -> None:
    print("\n" + "=" * 70)
    print("Message Relay - Transferring messages from database to RabbitMQ")
    print("=" * 70 + "\n")

    # Setup outbox
    setup(
        db_engine_url=DB_URL,
        rmq_connection_url=RABBITMQ_URL,
        poll_interval=1.0,  # Check for new messages every second
        auto_create_table=True,
    )

    print("🔄 Relay started. Polling outbox table every second...\n")
    print("💡 This process ensures transactional guarantees:")
    print("   1. Producer writes to database (atomic with business logic)")
    print("   2. Relay publishes to RabbitMQ (separate process)")
    print("   3. Consumer processes messages with retry logic\n")

    # Start the message relay
    await message_relay()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n👋 Relay shutting down...")
