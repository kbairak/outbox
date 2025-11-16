# Outbox Examples

This directory contains practical examples demonstrating various features of the outbox library.

## Available Examples

### [Delayed Retries](./delayed_retries/)

Demonstrates the exponential backoff retry mechanism with TTL-based delay queues.

**What you'll learn:**
- How to configure retry delays globally and per-listener
- Observing exponential backoff in action (2s, 5s, 15s delays)
- Handling transient vs permanent errors
- Using the `Reject` exception for permanent failures
- Inspecting delay queues in RabbitMQ
- Why handlers need to be idempotent

**Quick start:**
```bash
cd delayed_retries
docker-compose up -d
python relay.py  # Terminal 1
python consumer.py  # Terminal 2
python producer.py  # Terminal 3
```

Or use the automated demo script:
```bash
cd delayed_retries
./run_demo.sh
```

## Contributing Examples

Have a useful example? Feel free to contribute! Examples should:
- Be self-contained with their own README
- Include a docker-compose.yml for infrastructure
- Demonstrate a specific feature or use case
- Include clear comments and logging
- Show both success and failure cases

## Getting Help

- Check the main [README](../README.md) for library documentation
- Visit the [GitHub repository](https://github.com/kbairak/outbox) for issues and discussions
