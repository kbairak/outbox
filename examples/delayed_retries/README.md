# Delayed Retries with Exponential Backoff Example

This example demonstrates the **delayed retry** feature of the outbox library, which implements exponential backoff using RabbitMQ's TTL-based delay queues.

## What This Example Demonstrates

1. **Exponential Backoff**: Messages that fail are retried with increasing delays (e.g., 2s, 5s, 15s)
2. **Per-Listener Configuration**: Different listeners can have different retry policies
3. **Transient vs Permanent Errors**: How to handle temporary failures vs permanent rejection
4. **Dead Letter Queues**: Where messages go after exhausting all retries
5. **At-Least-Once Delivery**: Why handlers need to be idempotent

## Architecture

```
Producer → Database (Outbox Table) → Relay → RabbitMQ → Consumer
                                                ↓
                                          Delay Queues
                                          (TTL-based)
                                                ↓
                                       Back to Main Queue
                                          (after delay)
```

## Prerequisites

- Docker and Docker Compose (for RabbitMQ)
- Python 3.10+ with the outbox library installed

## Quick Start

### 1. Start the Infrastructure

Start RabbitMQ:

```bash
docker-compose up -d
```

Wait a few seconds for RabbitMQ to be ready. You can check its status with:

```bash
docker-compose ps
```

The service should show as "healthy".

### 2. Start the Message Relay

The relay transfers messages from the database to RabbitMQ:

```bash
python relay.py
```

Keep this running in its own terminal. You should see:
```
🔄 Relay started. Polling outbox table every second...
```

### 3. Start the Consumer

The consumer processes messages with retry logic:

```bash
python consumer.py
```

Keep this running in a second terminal. You should see:
```
🎧 Listening for messages...
```

### 4. Run the Producer

In a third terminal, emit some test messages:

```bash
python producer.py
```

This will emit three different types of messages to demonstrate different retry behaviors.

## What You'll See

### Message 1: `order.process` (Transient Failures)

This message will fail 3 times before succeeding:

```
[14:23:01] 📦 Processing order 123 (attempt 1/1)
[14:23:01] ⚠️  Order 123 failed (transient error). Will retry after delay...

[14:23:03] 📦 Processing order 123 (attempt 2/2)  # After 2s delay
[14:23:03] ⚠️  Order 123 failed (transient error). Will retry after delay...

[14:23:08] 📦 Processing order 123 (attempt 3/3)  # After 5s delay
[14:23:08] ⚠️  Order 123 failed (transient error). Will retry after delay...

[14:23:23] 📦 Processing order 123 (attempt 4/4)  # After 15s delay
[14:23:23] ✅ Order 123 processed successfully after 4 attempts!
```

**Total time**: ~22 seconds (2s + 5s + 15s delays)

### Message 2: `order.notify` (Immediate Success)

This message succeeds on the first attempt:

```
[14:23:01] 📧 Sending notification to Jane Smith for order 456
[14:23:01] ✅ Notification sent successfully!
```

### Message 3: `order.invalid` (Permanent Rejection)

This message is rejected immediately (no retries):

```
[14:23:01] ❌ Order 999 has invalid data. Rejecting to DLQ (no retries).
```

## Inspecting the Results

### RabbitMQ Management UI

Open http://localhost:15672 in your browser (username: `guest`, password: `guest`).

**Queues Tab**: You'll see:
- `demo.order_processor` - Main queue for order processing
- `outbox_exchange.delay_2s` - Delay queue for 2-second retry
- `outbox_exchange.delay_5s` - Delay queue for 5-second retry
- `outbox_exchange.delay_15s` - Delay queue for 15-second retry
- `demo.order_processor.dlq` - Dead letter queue for failed messages
- `demo.order_notifier` - Queue for notifications
- `demo.order_validator.dlq` - DLQ containing the rejected invalid order

**Exchanges Tab**: You'll see:
- `outbox_exchange` - Main topic exchange
- `outbox_exchange.dlx` - Dead letter exchange
- `outbox_exchange.delay_2s` - Delay exchange for 2-second retry (fanout)
- `outbox_exchange.delay_5s` - Delay exchange for 5-second retry (fanout)
- `outbox_exchange.delay_15s` - Delay exchange for 15-second retry (fanout)

### Database

Check the outbox table to see sent messages:

```bash
sqlite3 outbox_delayed_retries_demo.db "SELECT id, routing_key, sent_at FROM outbox_table;"
```

## How It Works

### Delay Queue Architecture

When a message fails:

1. **Handler raises exception** → Message is acknowledged
2. **Publish to delay queue** → Message sent to `queue_name.delay_Xs`
3. **TTL expires** → After X seconds, message expires
4. **Dead-letter routing** → Expired message routes back to main queue via default exchange
5. **Retry attempt** → Handler processes the message again

### Retry Limits

With `retry_delays=(2, 5, 15)`:
- **Attempt 1** (initial): Immediate processing
- **Attempt 2**: After 2-second delay
- **Attempt 3**: After 5-second delay
- **Attempt 4**: After 15-second delay
- **Attempt 5+**: Message goes to dead-letter queue (`.dlq`)

Total attempts = `len(retry_delays) + 1` = 4

### Special Cases

**Empty retry_delays `()`**:
```python
@listen("critical.task", retry_delays=())
async def handler(data):
    pass  # No retries - failures go straight to DLQ
```

**Permanent rejection with `Reject` exception**:
```python
from outbox import Reject

@listen("task", retry_delays=(1, 5))
async def handler(data):
    if is_invalid:
        raise Reject()  # Skip all retries, go directly to DLQ
```

## Configuration Examples

### Global Default

```python
setup(
    rmq_connection_url="amqp://...",
    retry_delays=(1, 10, 60, 300)  # All listeners use this by default
)
```

### Per-Listener Override

```python
# Fast retries for transient network errors
@listen("api.call", retry_delays=(1, 2, 5))
async def call_api(data):
    pass

# Slower retries for rate-limited APIs
@listen("external.api", retry_delays=(60, 300, 900))
async def call_rate_limited_api(data):
    pass

# No delayed retries, immediate retry only
@listen("idempotent.task", retry_delays=())
async def idempotent_handler(data):
    pass
```

## Idempotency Warning ⚠️

The outbox library implements **at-least-once delivery**. Messages may be delivered multiple times due to:
- Retries after failures
- Network issues or worker restarts
- RabbitMQ redeliveries

**Your handlers MUST be idempotent!**

### Bad Example ❌

```python
@listen("payment.process")
async def process_payment(payment):
    # BAD: This could charge the customer multiple times!
    await charge_credit_card(payment['amount'])
```

### Good Example ✅

```python
@listen("payment.process")
async def process_payment(payment):
    # GOOD: Check if already processed
    payment_id = payment['id']

    if await is_payment_processed(payment_id):
        logger.info(f"Payment {payment_id} already processed, skipping")
        return

    await charge_credit_card(payment['amount'])
    await mark_payment_processed(payment_id)
```

## Cleanup

Stop and remove all containers:

```bash
docker-compose down -v
```

The `-v` flag also removes the volumes (database data).

## Troubleshooting

**Consumer not receiving messages?**
- Make sure the relay is running
- Check RabbitMQ management UI to verify queues exist
- Check that messages were emitted (query the database)

**Messages stuck in delay queues?**
- This is normal! They're waiting for the TTL to expire
- Check the "Message TTL" on the delay queue in RabbitMQ UI

**Database file not found?**
- Make sure the producer ran successfully (it creates the database file)
- Check that `outbox_delayed_retries_demo.db` exists in the current directory

**Want to see more details?**
- Set logging level to DEBUG in the scripts
- Watch the RabbitMQ management UI in real-time
- Check queue depths and message rates

## Further Reading

- [Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)
- [RabbitMQ TTL and Dead Lettering](https://www.rabbitmq.com/ttl.html)
- [Idempotency in Distributed Systems](https://en.wikipedia.org/wiki/Idempotence)
