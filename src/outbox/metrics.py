"""Prometheus metrics for outbox pattern.

This module provides optional Prometheus metrics collection. If metrics are disabled, no-op
implementations are used instead.

Usage:
    from outbox.metrics import metrics

    metrics.enable_metrics(True)  # Enable metrics collection
    metrics.messages_published.labels(exchange_name="outbox").inc()
"""

from typing import Union

import prometheus_client


class NoopCounter:
    """No-op counter that duplicates Counter interface."""

    def labels(self, **labels: str) -> "NoopCounter":
        """Return self for method chaining."""
        return self

    def inc(self, amount: int = 1) -> None:
        """No-op increment."""
        pass


messages_published = prometheus_client.Counter(
    "outbox_messages_published_total",
    "Messages successfully published from outbox table to RabbitMQ",
    ["exchange_name"],
)


class Metrics:
    """Global metrics registry."""

    messages_published: Union[NoopCounter, prometheus_client.Counter]

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

    def _disable(self) -> None:
        self.messages_published = NoopCounter()


# Module-level singleton
metrics = Metrics()
