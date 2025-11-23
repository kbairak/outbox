# /// script
# requires-python = ">=3.9.2"
# dependencies = [
#     "celery>=5.4.0",
#     "testcontainers[rabbitmq]>=4.13.3",
# ]
# ///

import argparse
import glob
import os
import subprocess
import time
from typing import Generator, Optional, Union

from celery import Celery, group, signals
from testcontainers.rabbitmq import RabbitMqContainer  # type: ignore[import-untyped]

TIMESTAMPS_PATTERN = "/tmp/celery_benchmark_timestamps_*.txt"

app = Celery("celery_benchmark")

min_timestamp: Optional[float] = None
max_timestamp: Optional[float] = None


@app.task(name="process_message", ignore_result=True)
def process_message(_: str) -> None:
    """Trivial task that records completion timestamp."""
    global min_timestamp, max_timestamp
    now = time.perf_counter()
    if min_timestamp is None:
        min_timestamp = now
    max_timestamp = now


@signals.worker_process_shutdown.connect
def close_timestamp_file(**_):
    """Flush and close timestamp file when worker shuts down."""
    with open(TIMESTAMPS_PATTERN.replace("*", str(os.getpid())), "w") as f:
        f.write(f"{min_timestamp}\n{max_timestamp}\n")


def _generate_batch_sizes(
    rate: Union[int, float], duration: Union[int, float]
) -> Generator[int, None, None]:
    """Generate batch sizes for steady message emission."""
    start = time.perf_counter()
    yield (sent := 1)
    while (now := time.perf_counter()) - start <= duration:
        target = int((now - start) * rate)
        if target - sent > 0:
            yield target - sent
            sent = target
    if sent < int(rate * duration):
        yield int(rate * duration) - sent


def benchmark(
    message_rate: int = 1000,
    worker_count: int = 1,
    message_size: int = 256,
) -> None:
    """Run Celery benchmark with configurable parameters."""
    duration = 5.0
    message_count = int(message_rate * duration)

    # Clean up timestamps files from previous runs
    for old_file in glob.glob(TIMESTAMPS_PATTERN):
        os.remove(old_file)

    print("Starting containers...")
    with RabbitMqContainer("rabbitmq:4.1-management") as rabbitmq:
        rabbitmq_url = (
            f"amqp://{rabbitmq.username}:{rabbitmq.password}@{rabbitmq.get_container_host_ip()}:"
            f"{rabbitmq.get_exposed_port(rabbitmq.port)}/"
        )
        print(f"RabbitMQ: {rabbitmq_url}")

        # Configure Celery app
        app.conf.update(
            broker_url=rabbitmq_url,
            task_serializer="json",
            accept_content=["json"],
            task_ignore_result=True,
        )

        # Start Celery worker with multiprocessing
        print(f"Starting {worker_count} worker(s) via Celery concurrency...")

        env = os.environ.copy()
        env["CELERY_BROKER_URL"] = rabbitmq_url
        env["CELERY_RESULT_BACKEND"] = "rpc://"

        # Add src directory to PYTHONPATH so celery_benchmark module can be imported
        script_dir = os.path.dirname(os.path.abspath(__file__))
        src_dir = os.path.dirname(script_dir)
        if "PYTHONPATH" in env:
            env["PYTHONPATH"] = f"{src_dir}:{env['PYTHONPATH']}"
        else:
            env["PYTHONPATH"] = src_dir

        worker = subprocess.Popen(
            [
                "celery",
                "-A",
                "celery_benchmark.celery_benchmark",
                "worker",
                "--loglevel=WARNING",
                f"--concurrency={worker_count}",
                "--pool=prefork",
            ],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # Wait for workers to be ready
        print("Waiting for workers to be ready...")
        time.sleep(5)

        # Emit tasks at controlled rate
        print(
            f"Emitting {message_count:,} tasks at {message_rate:,} msgs/sec for {duration} seconds..."
        )

        start_time = time.perf_counter()
        junk = "*" * message_size

        for batch_size in _generate_batch_sizes(message_rate, duration):
            group([process_message.s(junk) for _ in range(batch_size)]).apply_async()

        emit_duration = time.perf_counter() - start_time
        actual_rate = message_count / emit_duration
        print(
            f"Emitted {message_count:,} tasks in {emit_duration:.2f}s "
            f"(target: {message_rate:,} msgs/sec, actual: {actual_rate:.0f} msgs/sec)"
        )

        print("Waiting 5 seconds for workers to finish...")
        time.sleep(5)

        print("Terminating worker process...")
        worker.terminate()
        worker.wait(timeout=5)

        print("Reading timestamps from files...")
        all_timestamps = []
        for filepath in glob.glob(TIMESTAMPS_PATTERN):
            with open(filepath, "r") as f:
                for line in f:
                    all_timestamps.append(float(line.strip()))

        processing_rate = message_count / (max(all_timestamps) - min(all_timestamps))

        print("\nWorker Statistics:")
        print(f"  Completed: {message_count:,} tasks")

        print("\n" + "=" * 80)
        print("BENCHMARK RESULTS")
        print("=" * 80)
        print(f"Workers:           {worker_count}")
        print(f"Target Rate:       {message_rate:,} msgs/sec")
        print(f"Message Size:      {message_size} bytes")
        print(f"Duration:          {duration} seconds")
        print(f"Tasks Emitted:     {message_count:,}")
        print(f"Processing Rate:   {processing_rate:,.0f} msgs/sec")

        # Clean up timestamp files
        for timestamp_file in glob.glob(TIMESTAMPS_PATTERN):
            try:
                os.remove(timestamp_file)
            except:
                pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run Celery benchmark - emits tasks at specified rate for 5 seconds"
    )
    parser.add_argument(
        "--message-rate",
        type=int,
        default=1000,
        help="Task emission rate in msgs/sec (default: 1000)",
    )
    parser.add_argument(
        "--worker-count",
        type=int,
        default=1,
        help="Number of worker processes (Celery concurrency) (default: 1)",
    )
    parser.add_argument(
        "--message-size",
        type=int,
        default=256,
        help="Message payload size in bytes (default: 256)",
    )

    args = parser.parse_args()

    benchmark(
        message_rate=args.message_rate,
        worker_count=args.worker_count,
        message_size=args.message_size,
    )
