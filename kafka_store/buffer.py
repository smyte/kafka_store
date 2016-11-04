from fastavro.writer import Writer
import hashlib
import logging
import tempfile
import time

# Use deflate codec for c++ compatibility
CODEC = 'deflate'

SCHEMA = {
  "name": "message",
  "type": "record",
  "fields": [
    {"name": "timestamp", "type": "long"},
    {"name": "key", "type": ["null", "bytes"]},
    {"name": "value", "type": ["null", "bytes"]}
  ]
}


# Allow a lot of skew with kafka. If we haven't seen a message in this amount
# of time, then assume the topic has not been written to and flush the
# incomplete file.
HOUR_MS = 3600 * 1000
KAFKA_SKEW_MS = 8 * HOUR_MS

logger = logging.getLogger('kafka_store.buffer')

class OutputFile:
    def __init__(self, file):
        self.file = file
        self.md5 = hashlib.md5()
        self.byte_size = 0

    def write(self, data):
        self.file.write(data)
        self.md5.update(data)
        self.byte_size += len(data)

    def flush(self):
        return self.file.flush()

class PartitionBuffer:
    def __init__(
        self,
        topic, partition, first_offset, first_timestamp_ms, max_age_ms,
        schema=SCHEMA, codec=CODEC
    ):
        self._fo = tempfile.NamedTemporaryFile()
        self._output = OutputFile(self._fo)
        self._writer = Writer(
            fo=self._output,
            schema=schema,
            codec=codec,
        )

        self.filename = self._fo.name
        self.count = 0
        self.closed = False
        self.eof = False
        self.max_age_ms = max_age_ms

        self.topic = topic
        self.partition = partition
        self.commit_next_offset = None

        self.first_offset = first_offset
        self.final_offset = None
        self.first_timestamp_ms = first_timestamp_ms

        logger.info(
            'Saving %s %d to %s from %d at %d',
            self.topic, self.partition, self.filename,
            self.first_offset, self.first_timestamp_ms
        )

    def mark_eof(self):
        self.eof = True

    def log(self, offset, key, value, timestamp_ms):
        assert offset == self.first_offset + self.count
        assert not self.closed
        self._writer.write({
            'key': key,
            'value': value,
            'timestamp': timestamp_ms,
        })
        self.count += 1
        self.commit_next_offset = offset + 1
        self.final_offset = offset
        self.eof = False

    def close(self):
        self._writer.flush()
        self.closed = True

    @property
    def byte_size(self):
        return self._output.byte_size

    @property
    def md5_hex(self):
        assert self.closed
        return self._output.md5.hexdigest()

    @property
    def md5(self):
        assert self.closed
        return self._output.md5.digest()

    def get_rewound_file(self):
        assert self.closed
        self._fo.seek(0)
        return self._fo

    def is_closed(self, timestamp_ms):
        return (timestamp_ms - self.first_timestamp_ms) >= self.max_age_ms

    def is_silent_closed(self):
        '''
        If a topic has been not received any new messages then close it after
        the maximum age anyway. Add some extra wait time just incase Kafka has
        a message that belongs in this file, but hasn't delivered it yet.
        '''
        if self.eof:
            return self.is_closed(int(time.time() * 1000) - KAFKA_SKEW_MS)
        else:
            return False
