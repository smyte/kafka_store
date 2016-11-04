import logging
from smyte_pylib.pacer import Pacer

from .buffer import PartitionBuffer

logger = logging.getLogger('kafka_store.handler')

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

    def _commit_buffer(self, buffer_key):
        self.buffers[buffer_key].close()
        for store in self.stores:
            store.save(self.buffers[buffer_key])

        self.loop.commit_next_offset(
            self.buffers[buffer_key].topic,
            self.buffers[buffer_key].partition,
            self.buffers[buffer_key].commit_next_offset
        )
        del self.buffers[buffer_key]

    def _commit_aged_buffers(self):
        buffer_keys = [
            buffer_key for buffer_key, buffer in self.buffers.items()
            if buffer.is_silent_closed()
        ]
        for buffer_key in buffer_keys:
            self._commit_buffer(buffer_key)

    def no_message(self):
        self._commit_aged_buffers()

    def process_eof(self, topic, partition):
        buffer_key = (topic, partition)
        if buffer_key in self.buffers:
            self.buffers[buffer_key].mark_eof()

    def process_message(self, topic, partition, offset, timestamp_ms, key, value):
        assert timestamp_ms is not None, (
            'Kafka store requires timestamps for its guarentees. ' +
            'Please use Kafka protocol >= 0.10.0.'
        )

        # If this message would push the buffer age over the max, commit before
        # appending the new message
        buffer_key = (topic, partition)
        if buffer_key in self.buffers:
            if self.buffers[buffer_key].is_closed(timestamp_ms):
                self._commit_buffer(buffer_key)

        if not buffer_key in self.buffers:
            self.buffers[buffer_key] = PartitionBuffer(
                topic=topic,
                partition=partition,
                first_offset=offset,
                first_timestamp_ms=timestamp_ms,
                max_age_ms=self.max_age_ms,
            )

        self.buffers[buffer_key].log(
            offset,
            key,
            value,
            timestamp_ms
        )

        if self.buffers[buffer_key].byte_size >= self.max_size:
            self._commit_buffer(buffer_key)

        if self.check_age_pacer.test():
            self._commit_aged_buffers()
