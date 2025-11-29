import asyncio
import inspect
import json
import signal
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Optional, Union, cast

import aio_pika
from aio_pika.abc import (
    AbstractConnection,
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractQueue,
    ConsumerTag,
)
from pydantic import BaseModel

from .log import logger
from .metrics import metrics
from .utils import (
    Reject,
    get_rmq_connection,
    parse_duration,
    tracking_ids_contextvar,
    truncate_body,
)


@dataclass
class Consumer:
    binding_key: str
    queue: str
    callback: Union[Callable[..., Any], Callable[..., Coroutine[Any, Any, None]]]
    retry_delays: Optional[Sequence[str]] = None
    _queue_obj: Optional[AbstractQueue] = None
    _consumer_tag: Optional[ConsumerTag] = None
    _delay_exchanges: dict[str, AbstractExchange] = field(default_factory=dict)
    _exchange_name: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.queue:
            # callback is always a function (decorated), safe to access __module__ and __qualname__
            callback_func = cast(Any, self.callback)
            self.queue = f"{callback_func.__module__}.{callback_func.__qualname__}".replace(
                "<", ""
            ).replace(">", "")

        # Auto-wrap sync callbacks to async using asyncio.to_thread
        if not asyncio.iscoroutinefunction(self.callback):
            sync_callback = self.callback

            async def async_wrapper(
                *args: Any, _sync_callback: Callable[..., Any] = sync_callback, **kwargs: Any
            ) -> None:
                return await asyncio.to_thread(_sync_callback, *args, **kwargs)

            # Preserve function signature for introspection
            async_wrapper.__signature__ = (  # type: ignore[attr-defined]
                inspect.signature(sync_callback)
            )

            self.callback = async_wrapper

    def __call__(self, *args: Any, **kwargs: Any) -> Coroutine[Any, Any, None]:
        return self.callback(*args, **kwargs)

    async def _handle(self, message: AbstractIncomingMessage) -> None:
        assert self._exchange_name is not None
        metrics.messages_received.labels(queue=self.queue, exchange_name=self._exchange_name).inc()

        parameters = inspect.signature(self.callback).parameters
        parameter_keys = set(parameters.keys()) - {
            "routing_key",
            "message",
            "tracking_ids",
            "attempt_count",
        }
        if len(parameter_keys) != 1:
            raise ValueError("Worker functions must accept exactly one argument")
        body_param_key = parameter_keys.pop()
        body_param = parameters[body_param_key]

        tracking_ids = tuple(
            json.loads(cast(str, message.headers.get("x-outbox-tracking-ids", "[]")))
        )
        token = tracking_ids_contextvar.set(tracking_ids)

        attempt_count_header = cast(Optional[str], message.headers.get("x-delivery-count"))
        retry_delays = self.retry_delays or ()

        if attempt_count_header is not None:
            attempt_count = int(attempt_count_header)

            # TODO: check if this is really needed
            # Check if retries are exhausted (only check if retry_delays is configured)
            # If retry_delays is empty, messages go to DLQ (see exception handler below)
            if retry_delays and attempt_count > len(retry_delays) + 1:
                logger.warning(
                    f"Message {message.routing_key} with tracking IDs {tracking_ids} exceeded "
                    f"retry attempts ({attempt_count} > {len(retry_delays) + 1}), sending to DLQ"
                )
                await message.nack(requeue=False)
                tracking_ids_contextvar.reset(token)
                return
        else:
            attempt_count = 1

        # Get routing key - use original routing key if preserved in header (for retries)
        # otherwise use message routing key (for initial delivery)
        routing_key = (
            cast(Optional[str], message.headers.get("x-original-routing-key"))
            or message.routing_key
        )
        assert routing_key is not None
        body = message.body
        try:
            if issubclass(body_param.annotation, BaseModel):
                body = body_param.annotation.model_validate_json(message.body)
            elif issubclass(body_param.annotation, bytes):
                body = message.body
            else:
                body = json.loads(message.body)
        except Exception as exc:
            logger.error(
                f"Failed to deserialize message body {routing_key=}, {tracking_ids=}, "
                f"error={type(exc).__name__}: {exc}",
                exc_info=True,
            )
            await message.nack(requeue=False)  # Send to DLQ instead of crashing
            metrics.messages_processed.labels(
                queue=self.queue,
                exchange_name=self._exchange_name,
                status="deserialization_failed",
            ).inc()
            tracking_ids_contextvar.reset(token)
            return  # Don't crash worker
        else:
            logger.debug(
                f"Processing message {routing_key=}, {tracking_ids=}, body={truncate_body(body)}"
            )

        kwargs: dict[str, Any] = {body_param_key: body}
        if "routing_key" in parameters:
            kwargs["routing_key"] = routing_key
        if "message" in parameters:
            kwargs["message"] = message
        if "tracking_ids" in parameters:
            kwargs["tracking_ids"] = tracking_ids
        if "attempt_count" in parameters:
            kwargs["attempt_count"] = attempt_count

        processing_start_time = time.perf_counter()

        try:
            await self.callback(**kwargs)
        except Reject:
            logger.warning(
                f"Rejecting, this message will end up in DLQ {routing_key=}, "
                f"{tracking_ids=}, body={truncate_body(body)}"
            )
            await message.nack(requeue=False)
            metrics.messages_processed.labels(
                queue=self.queue, exchange_name=self._exchange_name, status="rejected"
            ).inc()
        except Exception as exc:
            # Catch everything - programming errors will pile up in DLQ, forcing fix
            logger.warning(
                f"Handler failed with {type(exc).__name__}: {exc}, retrying "
                f"{routing_key=}, {tracking_ids=}, body={truncate_body(body)}, "
                f"attempt={attempt_count}/{len(retry_delays)}",
                exc_info=True,
            )
            await self._delayed_retry(message, attempt_count, tracking_ids)
            metrics.messages_processed.labels(
                queue=self.queue, exchange_name=self._exchange_name, status="failed"
            ).inc()
        else:
            logger.debug(f"Successfully processed {routing_key=}, {tracking_ids=}")
            await message.ack()
            metrics.messages_processed.labels(
                queue=self.queue, exchange_name=self._exchange_name, status="success"
            ).inc()
        finally:
            processing_duration_seconds = time.perf_counter() - processing_start_time
            metrics.message_processing_duration.labels(
                queue=self.queue, exchange_name=self._exchange_name
            ).observe(processing_duration_seconds)

            tracking_ids_contextvar.reset(token)

    async def _delayed_retry(
        self,
        message: AbstractIncomingMessage,
        attempt_count: int,
        tracking_ids: tuple[str, ...],
    ) -> None:
        """Publish message to delay queue for retry after backoff period."""
        retry_delays = self.retry_delays or ()

        if attempt_count > len(retry_delays):
            logger.warning(
                f"Exceeded retry attempts ({attempt_count} > {len(retry_delays)}), sending to DLQ"
            )
            await message.nack(requeue=False)
            return

        # Get delay for this attempt
        delay_str = retry_delays[attempt_count - 1]
        delay_ms = parse_duration(delay_str)

        # For instant retries (delay=0), use nack(requeue=True) to avoid unnecessary exchange/queue
        # overhead
        if delay_ms == 0:
            await message.nack(requeue=True)
            metrics.retry_attempts.labels(queue=self.queue, delay_seconds=delay_str).inc()
            logger.info(
                f"Message requeued for instant retry (attempt {attempt_count}/{len(retry_delays)}) "
                f"routing_key={message.routing_key}, {tracking_ids=}"
            )
            return

        delay_exchange = self._delay_exchanges[delay_str]

        # Prepare new headers with incremented delivery count and preserve original routing key
        new_headers = dict(message.headers) if message.headers else {}
        new_headers["x-delivery-count"] = str(attempt_count + 1)

        # Preserve original routing key in header (only set on first retry)
        assert message.routing_key is not None
        if "x-original-routing-key" not in new_headers:
            new_headers["x-original-routing-key"] = message.routing_key

        # Publish to delay exchange using queue name as routing key
        # This ensures retry routes back to the SAME queue (via default exchange DLX)
        # rather than re-routing through topic exchange to multiple queues
        try:
            await delay_exchange.publish(
                aio_pika.Message(
                    body=message.body,
                    content_type=message.content_type,
                    headers=new_headers,
                ),
                routing_key=self.queue,  # Use queue name, not original routing key
            )
        except Exception as exc:
            metrics.publish_failures.labels(
                exchange_name=delay_exchange.name,
                failure_type="retry",
                error_type=type(exc).__name__,
            ).inc()
            logger.error(
                f"Failed to publish message to delay exchange: routing_key={self.queue}, "
                f"original_routing_key={message.routing_key}, {tracking_ids=}, "
                f"delay_exchange={delay_exchange.name}, error={type(exc).__name__}, {exc=}"
            )
            raise

        # Ack original message
        await message.ack()

        metrics.retry_attempts.labels(queue=self.queue, delay_seconds=delay_str).inc()

        logger.info(
            f"Message sent to delay exchange (attempt {attempt_count}/{len(retry_delays)}, "
            f"delay {delay_str}) routing_key={self.queue}, "
            f"original_routing_key={message.routing_key}, {tracking_ids=}"
        )


def consume(
    binding_key: str,
    queue: str,
    retry_delays: Optional[Sequence[str]] = None,
) -> Callable[[Union[Callable[..., Any], Callable[..., Coroutine[Any, Any, None]]]], Consumer]:
    def decorator(
        func: Union[Callable[..., Any], Callable[..., Coroutine[Any, Any, None]]],
    ) -> Consumer:
        return Consumer(binding_key, queue, func, retry_delays)

    return decorator


@dataclass
class Worker:
    rmq_connection: Optional[AbstractConnection] = None
    rmq_connection_url: Optional[str] = None
    consumers: Sequence[Consumer] = field(default_factory=list)
    exchange_name: str = "outbox"
    retry_delays: Sequence[str] = ("1s", "10s", "1m", "5m")
    prefetch_count: int = 10
    enable_metrics: bool = True
    # Instance attribute (not just local var) to allow tests to simulate shutdown signals
    _shutdown_future: Optional[asyncio.Future[None]] = None

    def __post_init__(self) -> None:
        metrics.enable_metrics(self.enable_metrics)
        if self.rmq_connection is not None and self.rmq_connection_url is not None:
            raise ValueError("You cannot set both rmq_connection and rmq_connection_url")

    async def run(self) -> None:
        if self.rmq_connection is None and self.rmq_connection_url is not None:
            self.rmq_connection = await get_rmq_connection(self.rmq_connection_url)
        if self.rmq_connection is None:
            logger.warning("Cannot update DLQ metrics: RabbitMQ connection not available")
            return

        await self._set_up_queues()

        self._shutdown_future = asyncio.Future()
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGINT, self._shutdown_future.set_result, None)
        loop.add_signal_handler(signal.SIGTERM, self._shutdown_future.set_result, None)

        tasks = set()

        dlq_metrics_task = (
            asyncio.create_task(self._update_dlq_metrics()) if self.enable_metrics else None
        )

        logger.info(
            f"Starting worker on exchange '{self.exchange_name}' with "
            f"{len(self.consumers)} consumers, prefetch_count={self.prefetch_count}, "
            f"retry_delays={self.retry_delays}"
        )
        for consumer in self.consumers:
            assert consumer._queue_obj is not None

            async def _task(
                message: AbstractIncomingMessage, consumer: Consumer = consumer
            ) -> None:
                if self._shutdown_future is not None and self._shutdown_future.done():
                    await message.nack(requeue=True)
                    return
                task = asyncio.create_task(consumer._handle(message))
                tasks.add(task)
                task.add_done_callback(tasks.discard)

            consumer._consumer_tag = await consumer._queue_obj.consume(_task)
            metrics.active_consumers.labels(
                queue=consumer.queue, exchange_name=self.exchange_name
            ).inc()

        await self._shutdown_future

        logger.info("Received shutdown signal, waiting for ongoing tasks and exiting...")

        for consumer in self.consumers:
            if consumer._consumer_tag is None:
                continue
            assert consumer._queue_obj is not None
            await consumer._queue_obj.cancel(consumer._consumer_tag)
            metrics.active_consumers.labels(
                queue=consumer.queue, exchange_name=self.exchange_name
            ).dec()

        if tasks:
            await asyncio.wait(tasks)

        if dlq_metrics_task is not None:
            dlq_metrics_task.cancel()

    async def _update_dlq_metrics(self) -> None:
        """Background task to periodically update DLQ message count metrics."""

        assert self.rmq_connection is not None

        while True:
            if self._shutdown_future is not None and self._shutdown_future.done():
                break

            try:
                channel = await self.rmq_connection.channel()
                for consumer in self.consumers:
                    dlq_name = f"{consumer.queue}.dlq"
                    try:
                        dlq = await channel.get_queue(dlq_name)
                        declaration_result = await dlq.declare()
                        metrics.dlq_messages.labels(queue=consumer.queue).set(
                            float(declaration_result.message_count or 0)
                        )
                    except Exception as exc:
                        logger.debug(f"Failed to get DLQ message count for {dlq_name}: {exc}")
            except Exception as exc:
                logger.warning(f"Error updating DLQ metrics: {exc}")

            # Update every 30 seconds
            await asyncio.sleep(30)

    async def _set_up_queues(self) -> None:
        if self.rmq_connection is None:
            raise ValueError("RabbitMQ connection is not set up.")

        channel = await self.rmq_connection.channel()
        await channel.set_qos(prefetch_count=self.prefetch_count)

        try:
            exchange = await channel.declare_exchange(
                self.exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
            )
        except Exception as exc:
            logger.error(
                f"Failed to declare exchange '{self.exchange_name}': {exc}. "
                f"Check RabbitMQ permissions and connection.",
                exc_info=True,
            )
            raise  # Re-raise original aio-pika exception

        try:
            dead_letter_exchange = await channel.declare_exchange(
                f"{self.exchange_name}.dlx", aio_pika.ExchangeType.DIRECT, durable=True
            )
        except Exception as exc:
            logger.error(
                f"Failed to declare dead letter exchange '{self.exchange_name}.dlx': {exc}. "
                f"Check RabbitMQ permissions and connection.",
                exc_info=True,
            )
            raise  # Re-raise original aio-pika exception

        # Collect all unique delay strings from worker default and consumer overrides
        all_delay_strs: set[str] = set(self.retry_delays)
        for consumer in self.consumers:
            if consumer.retry_delays:
                all_delay_strs.update(consumer.retry_delays)

        # Parse and validate all delay strings upfront (before any RabbitMQ operations)
        # Create a delay_str -> delay_ms mapping for efficient lookup
        delay_map: dict[str, int] = {}
        for delay_str in all_delay_strs:
            delay_ms = parse_duration(delay_str)  # Let ValueError propagate naturally
            delay_map[delay_str] = delay_ms

        # Create delay exchanges (fanout type) and their queues
        # Skip delay=0 since instant retries use nack(requeue=True)
        delay_exchanges = {}
        for delay_str, delay_ms in delay_map.items():
            if delay_ms == 0:
                continue  # Instant retries handled by nack(requeue=True), no exchange needed

            # Use duration string directly in queue name (clean, no float precision issues)
            exchange_and_queue_name = f"{self.exchange_name}.delay_{delay_str}"
            delay_exchange = await channel.declare_exchange(
                exchange_and_queue_name, aio_pika.ExchangeType.FANOUT, durable=True
            )
            delay_exchanges[delay_str] = delay_exchange

            # Create delay queue and bind to delay exchange
            delay_queue = await channel.declare_queue(
                exchange_and_queue_name,
                durable=True,
                arguments={
                    "x-message-ttl": delay_ms,
                    "x-dead-letter-exchange": "",  # Default exchange (routes by queue name)
                    "x-queue-type": "quorum",
                },
            )
            # Bind to fanout exchange (no routing key needed)
            await delay_queue.bind(delay_exchange)

        # Set up consumer queues and inject configuration
        for consumer in self.consumers:
            # Inject default retry_delays, delay exchanges, and exchange_name
            if consumer.retry_delays is None:
                consumer.retry_delays = self.retry_delays
            consumer._delay_exchanges = delay_exchanges
            consumer._exchange_name = self.exchange_name

            dead_letter_queue_obj = await channel.declare_queue(
                f"{consumer.queue}.dlq", durable=True, arguments={"x-queue-type": "quorum"}
            )
            await dead_letter_queue_obj.bind(dead_letter_exchange, consumer.queue)

            logger.debug(
                f"Binding queue {consumer.queue} to exchange {self.exchange_name} with "
                f"binding key {consumer.binding_key}"
            )
            consumer._queue_obj = await channel.declare_queue(
                consumer.queue,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": f"{self.exchange_name}.dlx",
                    "x-dead-letter-routing-key": consumer.queue,
                    "x-queue-type": "quorum",
                },
            )
            await consumer._queue_obj.bind(exchange, consumer.binding_key)
