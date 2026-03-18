"""Prometheus metrics for outbox pattern.

Usage:
    from . import metrics

    metrics.messages_published.labels(exchange_name="outbox").inc()
    metrics.publish_failures.labels(
        exchange_name="outbox",
        failure_type="main",
        error_type="ConnectionError"
    ).inc()
"""

import prometheus_client

messages_published = prometheus_client.Counter(
    "outbox_messages_published_total",
    "Messages successfully published from outbox table to RabbitMQ",
    ["exchange_name"],
)

publish_failures = prometheus_client.Counter(
    "outbox_publish_failures_total",
    "Failed attempts to publish messages to RabbitMQ",
    ["exchange_name", "failure_type", "error_type"],
)

message_age = prometheus_client.Histogram(
    "outbox_message_age_seconds",
    "Time message spent in outbox table before publishing",
    ["exchange_name"],
)

poll_duration = prometheus_client.Histogram(
    "outbox_poll_duration_seconds",
    "Time to poll DB and publish one message",
    ["exchange_name"],
)

table_backlog = prometheus_client.Gauge(
    "outbox_table_backlog",
    "Current unsent messages in outbox table",
    ["exchange_name"],
)

messages_received = prometheus_client.Counter(
    "outbox_messages_received_total",
    "Messages received from RabbitMQ queue",
    ["queue", "exchange_name"],
)

messages_processed = prometheus_client.Counter(
    "outbox_messages_processed_total",
    "Messages processed with outcome",
    ["queue", "exchange_name", "status"],
)

retry_attempts = prometheus_client.Counter(
    "outbox_retry_attempts_total",
    "Retry attempts by delay tier",
    ["queue", "delay_seconds"],
)

message_processing_duration = prometheus_client.Histogram(
    "outbox_message_processing_duration_seconds",
    "Handler execution time",
    ["queue", "exchange_name"],
)

dlq_messages = prometheus_client.Gauge(
    "outbox_dlq_messages",
    "Current messages in dead letter queue",
    ["queue"],
)

active_consumers = prometheus_client.Gauge(
    "outbox_active_consumers",
    "Active consumer connections",
    ["queue", "exchange_name"],
)
