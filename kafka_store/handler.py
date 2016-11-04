import base64
import logging
import time
from googleapiclient import http
from smyte_pylib.pacer import Pacer
from smyte_pylib.decorators import memoize

from .partition import PartitionBuffer

logger = logging.getLogger('kafka_store.handler')

# When calculating `now` without a message; use a timestamp an hour away
# to account for a *lot* of time skew.
KAFKA_SKEW_MS = 3600 * 1000

SCHEMA = {
  "name": "message",
  "type": "record",
  "fields": [
    {"name": "timestamp", "type": "long"},
    {"name": "key", "type": "bytes"},
    {"name": "value", "type": "bytes"}
  ]
}

class KafkaStoreHandler:
    def __init__(self, loop, stores, max_size, max_age_ms):
        self.loop = loop
        self.buffers = {}
        self.stores = stores

        self.max_size = max_size
        self.max_age_ms = max_age_ms

        # Don't need to check the age of everything of logs every loop
        self.check_age_pacer = Pacer(limit=None, timeout=15)

    def reset(self):
        pass

    def assign_partitions(self, topic_partitions):
        pass

    def revoke_partitions(self, topic_partitions):
        for (topic, partition) in topic_partitions:
            self.buffers.pop((topic, partition), None)

    def _commit_buffer(self, key):
        for store in self.stores:
            store.save(self.buffers[key])

        self.loop.commit_next_offset(
            self.buffers[key].topic,
            self.buffers[key].partition,
            self.buffers[key].commit_next_offset
        )
        del self.buffers[key]

    def _commit_aged_buffers(self):
        keys = [
            key for key, buffer in self.buffers.items()
            if buffer.is_silent_closed()
        ]
        for key in keys:
            self._commit_buffer(key)

    def no_message(self):
        if self.check_age_pacer.test():
            self._commit_aged_buffers()

    def process_eof(self, topic, partition):
        key = (topic, partition)
        if key in self.loggers:
            self.loggers[key].mark_eof()

    def process_message(self, topic, partition, offset, timestamp_ms, key, value):
        assert timestamp_ms is not None, (
            'Kafka store requires timestamps for its guarentees. ' +
            'Please use Kafka protocol >= 0.10.0.'
        )

        # If this message would push the buffer age over the max, commit before
        # appending the new message
        key = (topic, partition)
        if key in self.buffers:
            if self.buffers[key].is_closed(timestamp_ms):
                self._commit_buffer(key)

        if not key in self.buffers:
            self.buffers[key] = PartitionBuffer(
                topic=topic,
                partition=partition,
                first_offset=offset,
                first_timestamp_ms=timestamp_ms,
                max_age_ms=self.max_age_ms,
            )

        self.buffers[key].log(
            offset,
            key,
            value,
            timestamp_ms
        )

        if self.buffers[key].byte_size >= self.max_size:
            self._commit_buffer(key)

        if self.check_age_pacer.test():
            self._commit_aged_buffers()
