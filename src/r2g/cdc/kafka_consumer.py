"""Kafka consumer for CDC event ingestion.

Subscribes to one or more Kafka topics carrying Debezium (or flat JSON)
change events, parses them into ChangeEvents, and feeds them through the
existing CDCHandler pipeline for transformation and application to ArangoDB.

Requires ``confluent-kafka`` (install via ``pip install r2g[kafka]``).

Key design decisions:
- Uses ``confluent-kafka`` for robust, high-throughput consumption.
- Commits offsets only after successful processing to provide at-least-once
  delivery semantics.
- Supports both ``earliest`` and ``latest`` auto-offset-reset.
- Graceful shutdown via SIGINT/SIGTERM.
- Preserves Kafka partition ordering for transactional consistency.
"""

from __future__ import annotations

import signal
import time
from typing import Any

from r2g.cdc.handler import CDCHandler
from r2g.cdc.kafka_parser import DebeziumParser, FlatJsonParser
from r2g.cdc.models import ChangeEvent
from r2g.log import get_logger

logger = get_logger(__name__)

SUPPORTED_FORMATS = ("debezium", "flat")


def _check_confluent_kafka() -> None:
    try:
        import confluent_kafka  # noqa: F401
    except ImportError:
        raise ImportError(
            "confluent-kafka is required for Kafka integration. "
            "Install it with: pip install 'r2g[kafka]'"
        )


class KafkaConsumer:
    """Consumes CDC events from Kafka and applies them to ArangoDB.

    Usage::

        consumer = KafkaConsumer(
            handler=handler,
            brokers="localhost:9092",
            topics=["dbserver1.public.users", "dbserver1.public.orders"],
            group_id="r2g-cdc",
        )
        consumer.run()  # blocks until stop() or SIGINT

    The consumer preserves message ordering within each partition,
    which aligns with Debezium's per-table partitioning strategy.
    """

    def __init__(
        self,
        handler: CDCHandler,
        brokers: str = "localhost:9092",
        topics: list[str] | None = None,
        group_id: str = "r2g-cdc",
        auto_offset_reset: str = "earliest",
        message_format: str = "debezium",
        poll_timeout: float = 1.0,
        batch_size: int = 500,
        extra_config: dict[str, Any] | None = None,
    ) -> None:
        _check_confluent_kafka()

        if message_format not in SUPPORTED_FORMATS:
            raise ValueError(
                f"Unsupported message format '{message_format}'; "
                f"choose from {SUPPORTED_FORMATS}"
            )

        self.handler = handler
        self.brokers = brokers
        self.topics = topics or []
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.message_format = message_format
        self.poll_timeout = poll_timeout
        self.batch_size = batch_size
        self.extra_config = extra_config or {}

        self._running = False
        self._consumer: Any = None

        if message_format == "debezium":
            self._parser: DebeziumParser | FlatJsonParser = DebeziumParser()
        else:
            self._parser = FlatJsonParser()

    def _build_config(self) -> dict[str, Any]:
        """Build the confluent-kafka consumer configuration."""
        conf: dict[str, Any] = {
            "bootstrap.servers": self.brokers,
            "group.id": self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": False,
        }
        conf.update(self.extra_config)
        return conf

    def _create_consumer(self) -> Any:
        """Create and return a confluent_kafka.Consumer."""
        from confluent_kafka import Consumer

        conf = self._build_config()
        consumer = Consumer(conf)
        consumer.subscribe(self.topics)
        logger.info(
            "kafka_consumer_subscribed",
            brokers=self.brokers,
            topics=self.topics,
            group_id=self.group_id,
            format=self.message_format,
        )
        return consumer

    def _parse_message(self, msg: Any) -> ChangeEvent | None:
        """Extract and parse a Kafka message value."""
        value = msg.value()
        if value is None:
            return None
        return self._parser.parse(value)

    def _consume_batch(self) -> list[ChangeEvent]:
        """Poll up to batch_size messages and parse them."""
        events: list[ChangeEvent] = []
        messages = self._consumer.consume(
            num_messages=self.batch_size,
            timeout=self.poll_timeout,
        )

        for msg in messages:
            if msg.error():
                self._handle_error(msg)
                continue
            evt = self._parse_message(msg)
            if evt is not None:
                events.append(evt)

        return events

    def _handle_error(self, msg: Any) -> None:
        """Handle a Kafka consumer error message."""
        from confluent_kafka import KafkaError

        err = msg.error()
        if err.code() == KafkaError._PARTITION_EOF:
            logger.debug(
                "kafka_partition_eof",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )
        else:
            logger.error(
                "kafka_consumer_error",
                error=str(err),
                topic=msg.topic(),
            )

    def _commit(self) -> None:
        """Synchronously commit offsets."""
        try:
            self._consumer.commit(asynchronous=False)
        except Exception as exc:
            logger.warning("kafka_commit_failed", error=str(exc))

    def run(self) -> None:
        """Start the consumer loop. Blocks until stop() or SIGINT/SIGTERM.

        Messages are consumed in batches, grouped by transaction_id
        (when available), and applied through the CDCHandler.
        Offsets are committed after each successful batch.
        """
        self._running = True
        self._consumer = self._create_consumer()

        prev_sigint = signal.getsignal(signal.SIGINT)
        prev_sigterm = signal.getsignal(signal.SIGTERM)

        def _handle_signal(signum: int, _frame: Any) -> None:
            logger.info("kafka_signal_received", signal=signum)
            self._running = False

        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

        logger.info("kafka_consumer_started")
        processed_total = 0

        try:
            while self._running:
                try:
                    events = self._consume_batch()
                    if not events:
                        continue

                    groups = self.handler.group_by_transaction(events)
                    for group in groups:
                        if len(group) == 1:
                            self.handler.handle_event(group[0])
                        else:
                            self.handler.handle_transaction(group)

                    self._commit()
                    processed_total += len(events)

                    logger.info(
                        "kafka_batch_processed",
                        events=len(events),
                        total=processed_total,
                        applied=self.handler.stats.deltas_applied,
                        failed=self.handler.stats.deltas_failed,
                        last_lsn=self.handler.stats.last_lsn,
                    )
                except Exception as exc:
                    if not self._running:
                        break
                    logger.error("kafka_consume_error", error=str(exc))
                    time.sleep(1.0)
        finally:
            signal.signal(signal.SIGINT, prev_sigint)
            signal.signal(signal.SIGTERM, prev_sigterm)
            self._consumer.close()
            logger.info(
                "kafka_consumer_stopped",
                total_processed=processed_total,
                stats=self.handler.stats.as_dict(),
            )

    def stop(self) -> None:
        """Signal the consumer loop to exit."""
        self._running = False
