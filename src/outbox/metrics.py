"""Prometheus metrics for outbox pattern.

This module provides optional Prometheus metrics collection. If metrics are disabled, no-op
implementations are used instead.

Usage:
    from .metrics import metrics

    metrics.enable_metrics(True)  # Enable metrics collection
    metrics.messages_published.labels(exchange_name="outbox").inc()
    metrics.publish_failures.labels(
        exchange_name="outbox",
        failure_type="main",
        error_type="ConnectionError"
    ).inc()
"""

from __future__ import annotations

import prometheus_client


class NoopCounter:
    """No-op counter that duplicates Counter interface."""

    def labels(self, **labels: str) -> NoopCounter:
        """Return self for method chaining."""
        return self

    def inc(self, amount: int = 1) -> None:
        """No-op increment."""
        pass


class NoopHistogram:
    """No-op histogram that duplicates Histogram interface."""

    def labels(self, **labels: str) -> NoopHistogram:
        """Return self for method chaining."""
        return self

    def observe(self, amount: float) -> None:
        """No-op observe."""
        pass


class NoopGauge:
    """No-op gauge that duplicates Gauge interface."""

    def labels(self, **labels: str) -> NoopGauge:
        """Return self for method chaining."""
        return self

    def inc(self, amount: float = 1.0) -> None:
        """No-op increment."""
        pass

    def dec(self, amount: float = 1.0) -> None:
        """No-op decrement."""
        pass

    def set(self, value: float) -> None:
        """No-op set."""
        pass


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


class Metrics:
    """Global metrics registry."""

    messages_published: NoopCounter | prometheus_client.Counter
    publish_failures: NoopCounter | prometheus_client.Counter
    message_age: NoopHistogram | prometheus_client.Histogram
    poll_duration: NoopHistogram | prometheus_client.Histogram
    table_backlog: NoopGauge | prometheus_client.Gauge
    messages_received: NoopCounter | prometheus_client.Counter
    messages_processed: NoopCounter | prometheus_client.Counter
    retry_attempts: NoopCounter | prometheus_client.Counter
    message_processing_duration: NoopHistogram | prometheus_client.Histogram
    dlq_messages: NoopGauge | prometheus_client.Gauge
    active_consumers: NoopGauge | prometheus_client.Gauge

    def __init__(self) -> None:
        """Initialize metrics in disabled state."""
        self._disable()

    def enable_metrics(self, enable: bool) -> None:
        """Enable or disable metrics collection.

        Args:
            enable: Whether to enable metrics collection
        """
        if enable:
            self._enable()
        else:
            self._disable()

    def _enable(self) -> None:
        """Enable metrics collection."""
        self.messages_published = messages_published
        self.publish_failures = publish_failures
        self.message_age = message_age
        self.poll_duration = poll_duration
        self.table_backlog = table_backlog
        self.messages_received = messages_received
        self.messages_processed = messages_processed
        self.retry_attempts = retry_attempts
        self.message_processing_duration = message_processing_duration
        self.dlq_messages = dlq_messages
        self.active_consumers = active_consumers

    def _disable(self) -> None:
        self.messages_published = NoopCounter()
        self.publish_failures = NoopCounter()
        self.message_age = NoopHistogram()
        self.poll_duration = NoopHistogram()
        self.table_backlog = NoopGauge()
        self.messages_received = NoopCounter()
        self.messages_processed = NoopCounter()
        self.retry_attempts = NoopCounter()
        self.message_processing_duration = NoopHistogram()
        self.dlq_messages = NoopGauge()
        self.active_consumers = NoopGauge()


# Module-level singleton
metrics = Metrics()
