import argparse
import asyncio
import logging

from outbox import message_relay, setup


def validate_clean_up_after(value: str) -> str | float:
    if value.upper() in ("IMMEDIATELY", "NEVER"):
        return value.upper()
    return float(value)


parser = argparse.ArgumentParser(
    "message_relay",
    description=(
        "Polls the outbox database table, consumes unsent messages, and sends them to RabbitMQ"
    ),
)
parser.add_argument(
    "--db-engine-url",
    required=True,
    help=(
        "A string that indicates database dialect and connection arguments. Will be passed to "
        "SQLAlchemy. Follows the pattern "
        "'<database_type>+<dbapi>://<username>:<password>@<host>:<port>/<db_name>'. Make sure you "
        "use a DBAPI that supports async operations, like `asyncpg` for PostgreSQL or `aiosqlite` "
        "for SQLite. Examples: 'postgresql+asyncpg://postgres:postgres@localhost:5432/postgres' "
        "or 'sqlite+aiosqlite:///:memory:'"
    ),
)
parser.add_argument(
    "--rmq-connection-url",
    required=True,
    help=(
        "A string that indicates RabbitMQ connection parameters. Follows the pattern "
        "'amqp[s]://<username>:<password>@<host>:(<port>)/(virtualhost)'. Example: "
        "'amqp://guest:guest@localhost:5672/'"
    ),
)
parser.add_argument("--exchange-name", default=None, help="Name of the RabbitMQ exchange to use")
parser.add_argument(
    "--poll-interval",
    type=float,
    default=None,
    help="Interval in seconds to poll the outbox table",
)
parser.add_argument(
    "--expiration",
    type=float,
    default=None,
    help="Expiration time in seconds for messages in RabbitMQ",
)
parser.add_argument(
    "--clean-up-after",
    type=validate_clean_up_after,
    default=None,
    help=(
        "How long to keep messages in the outbox table after they are sent. Can be 'IMMEDIATELY', "
        "'NEVER', or a float representing seconds."
    ),
)
parser.add_argument(
    "--table-name", default=None, help="Name of the outbox table to use. Defaults to 'outbox'."
)
parser.add_argument("-v", "--verbose", action="store_true", help="Increase verbosity")


def main() -> None:
    options = parser.parse_args()
    if options.verbose:
        logging.basicConfig(level=logging.INFO)
        logging.getLogger("outbox").setLevel(logging.DEBUG)
    kwargs = {
        "db_engine_url": options.db_engine_url,
        "rmq_connection_url": options.rmq_connection_url,
    }
    for option_name in (
        "exchange_name",
        "poll_interval",
        "expiration",
        "clean_up_after",
        "table_name",
    ):
        if getattr(options, option_name) is not None:
            kwargs[option_name] = getattr(options, option_name)
    setup(**kwargs)
    asyncio.run(message_relay())


if __name__ == "__main__":
    main()
