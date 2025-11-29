from unittest.mock import AsyncMock

import aio_pika
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from outbox import MessageRelay
from outbox.database import OutboxTable
from tests.utils import PublishType


@pytest.mark.asyncio(loop_scope="session")
async def test_message_relay(
    publish: PublishType, message_relay: MessageRelay, session: AsyncSession
) -> None:
    # arrange
    rmq_connection_mock = AsyncMock(name="rmq_connection")

    message_relay.rmq_connection = rmq_connection_mock
    rmq_connection_mock.channel.return_value = (channel_mock := AsyncMock(name="channel"))
    channel_mock.declare_exchange.return_value = (exchange_mock := AsyncMock(name="exchange"))

    await publish(session, "test_routing_key", "test_body")
    await session.commit()

    # test
    await message_relay._consume_outbox_table()

    # assert
    rmq_connection_mock.channel.assert_called_once_with()
    channel_mock.declare_exchange.assert_called_once_with(
        "outbox", aio_pika.ExchangeType.TOPIC, durable=True
    )
    assert exchange_mock.publish.call_count == 1
    actual_message = exchange_mock.publish.mock_calls[0].args[0]
    assert actual_message.body == b'"test_body"'
    assert actual_message.body_size == 11
    assert actual_message.content_type == "application/json"

    message = (await session.execute(select(OutboxTable))).scalars().one()
    assert message.sent_at is not None


@pytest.mark.asyncio(loop_scope="session")
async def test_message_relay_batch(
    publish: PublishType, message_relay: MessageRelay, session: AsyncSession
) -> None:
    # arrange
    prev_batch_size = message_relay.batch_size
    message_relay.batch_size = 2

    rmq_connection_mock = AsyncMock(name="rmq_connection")
    message_relay.rmq_connection = rmq_connection_mock
    rmq_connection_mock.channel.return_value = (channel_mock := AsyncMock(name="channel"))
    channel_mock.declare_exchange.return_value = (exchange_mock := AsyncMock(name="exchange"))

    await publish(session, "test_routing_key_1", "test_body_1")
    await publish(session, "test_routing_key_2", "test_body_2")
    await publish(session, "test_routing_key_3", "test_body_3")
    await session.commit()

    # act - process one batch
    count = await message_relay._consume_outbox_batch(exchange_mock, session)

    # assert - batch_size=2 means exactly 2 messages processed
    assert count == 2
    assert len(exchange_mock.publish.mock_calls) == 2
    assert exchange_mock.publish.mock_calls[0].args[0].body == b'"test_body_1"'
    assert exchange_mock.publish.mock_calls[0].args[1] == "test_routing_key_1"
    assert exchange_mock.publish.mock_calls[1].args[0].body == b'"test_body_2"'
    assert exchange_mock.publish.mock_calls[1].args[1] == "test_routing_key_2"

    await session.commit()

    # assert - 2 messages marked as sent, 1 still pending
    messages = (
        (await session.execute(select(OutboxTable).order_by(OutboxTable.id))).scalars().all()
    )
    assert len(messages) == 3
    assert messages[0].sent_at is not None  # First message sent
    assert messages[1].sent_at is not None  # Second message sent
    assert messages[2].sent_at is None  # Third message still pending

    # reset
    message_relay.batch_size = prev_batch_size
