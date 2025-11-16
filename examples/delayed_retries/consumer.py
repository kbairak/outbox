"""
Consumer example - Demonstrates delayed retries with exponential backoff.

This script sets up workers that handle messages with different retry behaviors:
1. Transient errors that retry with exponential backoff
2. Successful processing
3. Permanent errors that go directly to DLQ
"""

import asyncio
import logging
from collections.abc import Mapping
from datetime import datetime

from outbox import Reject, listen, setup, worker

# Configure logging to see retry behavior
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# Database and RabbitMQ connection strings
RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"

# Track processing attempts for demonstration
attempt_counts = {}


@listen("order.process", queue="demo.order_processor", retry_delays=(2, 5, 15))
async def process_order(order: Mapping[str, object], attempt_count: int) -> None:
    """
    Simulates a flaky service that fails a few times before succeeding.

    This demonstrates exponential backoff:
    - Attempt 1 (initial): Fails immediately
    - Attempt 2 (after 2s delay): Fails again
    - Attempt 3 (after 5s delay): Fails again
    - Attempt 4 (after 15s delay): Succeeds!
    """
    order_id = order["order_id"]

    # Track attempts for this order
    if order_id not in attempt_counts:
        attempt_counts[order_id] = 0
    attempt_counts[order_id] += 1

    current_time = datetime.now().strftime("%H:%M:%S")

    logger.info(
        f"[{current_time}] 📦 Processing order {order_id} "
        f"(attempt {attempt_count}/{attempt_counts[order_id]})"
    )

    # Simulate transient failure for first 3 attempts
    if attempt_counts[order_id] < 4:
        logger.warning(
            f"[{current_time}] ⚠️  Order {order_id} failed (transient error). "
            f"Will retry after delay..."
        )
        raise Exception("Simulated transient failure (e.g., external API timeout)")

    # Success on 4th attempt
    logger.info(
        f"[{current_time}] ✅ Order {order_id} processed successfully "
        f"after {attempt_counts[order_id]} attempts!"
    )


@listen("order.notify", queue="demo.order_notifier")
async def notify_customer(order: Mapping[str, object]) -> None:
    """
    Simulates a reliable service that always succeeds.
    """
    current_time = datetime.now().strftime("%H:%M:%S")
    logger.info(
        f"[{current_time}] 📧 Sending notification to {order.get('customer')} "
        f"for order {order['order_id']}"
    )
    # Simulate some work
    await asyncio.sleep(0.1)
    logger.info(f"[{current_time}] ✅ Notification sent successfully!")


@listen("order.invalid", queue="demo.order_validator", retry_delays=(1, 2))
async def validate_order(order: Mapping[str, object], attempt_count: int) -> None:
    """
    Simulates a validator that rejects invalid data immediately.

    Uses the Reject exception to bypass retries and send directly to DLQ.
    """
    current_time = datetime.now().strftime("%H:%M:%S")
    logger.warning(
        f"[{current_time}] ❌ Order {order['order_id']} has invalid data. "
        f"Rejecting to DLQ (no retries)."
    )
    # Permanent error - no point retrying
    raise Reject("Invalid order data - cannot be processed")


async def main() -> None:
    print("\n" + "=" * 70)
    print("Consumer - Waiting for messages with delayed retry demonstration")
    print("=" * 70 + "\n")

    # Setup outbox with default retry delays
    setup(
        rmq_connection_url=RABBITMQ_URL,
        retry_delays=(2, 5, 15),  # Default: 2s, 5s, 15s delays
        auto_create_table=True,
    )

    print("🎧 Listening for messages...")
    print("   - order.process: Will fail 3 times with 2s, 5s, 15s delays")
    print("   - order.notify: Will succeed immediately")
    print("   - order.invalid: Will be rejected to DLQ immediately\n")

    print("💡 Tip: Run producer.py in another terminal to send messages\n")
    print("📊 Watch the delays in action:")
    print("   Initial attempt → fails → wait 2s → retry → fails → wait 5s → retry")
    print("   → fails → wait 15s → retry → succeeds!\n")

    # Start consuming messages
    await worker([process_order, notify_customer, validate_order])


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n👋 Shutting down gracefully...")
