import argparse
import asyncio
import importlib.util
import logging
import sys

from outbox import setup, worker

parser = argparse.ArgumentParser(
    "worker", description="Worker process for processing outbox messages"
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
parser.add_argument(
    "--db-engine-url",
    help=(
        "A string that indicates database dialect and connection arguments. Will be passed to "
        "SQLAlchemy. Not necessary unless you plan to invoke `emit` from within the listeners "
        "(which is highly probable). Follows the pattern "
        "'<database_type>+<dbapi>://<username>:<password>@<host>:<port>/<db_name>'. Make sure you "
        "use a DBAPI that supports async operations, like `asyncpg` for PostgreSQL or `aiosqlite` "
        "for SQLite. Examples: 'postgresql+asyncpg://postgres:postgres@localhost:5432/postgres' "
        "or 'sqlite+aiosqlite:///:memory:'"
    ),
)
parser.add_argument("--exchange-name", default=None, help="Name of the RabbitMQ exchange to use")
parser.add_argument(
    "--no-retry-on-error",
    dest="retry_on_error",
    action="store_false",
    help=(
        "Disable retry on error. Exceptions raised within listeners will result in the messages "
        "being dead-lettered (unless an `outbox.Retry` is raised)"
    ),
)
parser.add_argument(
    "--retry-limit", type=int, default=None, help="Default maximum number of retries for a message"
)
parser.add_argument(
    "--tags",
    default=None,
    help="Comma-separated list of tags to limit which queues this worker will consume",
)
parser.add_argument("-v", "--verbose", action="store_true", help="Increase verbosity")
parser.add_argument("module", nargs="+", help="Import path of the module to load listeners from")


def main():
    options = parser.parse_args()
    if options.verbose:
        logging.basicConfig(level=logging.INFO)
        logging.getLogger("outbox").setLevel(logging.DEBUG)
    kwargs = {"rmq_connection_url": options.rmq_connection_url}
    for option_name in ("db_engine_url", "exchange_name", "retry_on_error", "retry_limit"):
        if getattr(options, option_name) is not None:
            kwargs[option_name] = getattr(options, option_name)
    setup(**kwargs)
    tags = options.tags
    if tags is not None:
        tags = set(tags.split(","))

    sys.path.append(".")
    for module in options.module:
        try:
            __import__(module)
        except ModuleNotFoundError:
            module_path = module
            module_filename = module_path.rsplit("/", 1)[-1]
            module_name = (
                module_filename[:-3] if module_filename.endswith(".py") else module_filename
            )
            spec = importlib.util.spec_from_file_location(module_name, module_path)
            assert spec is not None
            module = importlib.util.module_from_spec(spec)
            assert spec.loader is not None
            spec.loader.exec_module(module)

    asyncio.run(worker(tags=tags))


if __name__ == "__main__":
    main()
