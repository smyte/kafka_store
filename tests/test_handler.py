import time
from collections import defaultdict
from mock import (
    MagicMock,
    patch,
)

from kafka_store.buffer import (
    KAFKA_SKEW_MS,
)
from kafka_store.handler import (
    KafkaStoreHandler,
)
from kafka_store.read import reader


TOPIC = 'sample-topic'
PARTITION = 0

@patch('time.time')
def test_buffer(mock_time):
    # Start at some arbitrary point in time
    mock_time.return_value = 1478297942.9726593
    start_ms = int(time.time() * 1000)

    max_size = 64 * 1024
    max_age_ms = 60 * 1000

    loop = MagicMock()
    store = MagicMock()

    handler = KafkaStoreHandler(
        loop, [store],
        max_size=max_size,
        max_age_ms=max_age_ms,
    )

    next_offset = defaultdict(int)

    def _process(offset_ms, key, value, topic=TOPIC, partition=PARTITION):
        nonlocal next_offset
        offset_key = (topic, partition)
        handler.process_message(
            topic, partition, next_offset[offset_key], start_ms + offset_ms,
            key, value
        )
        next_offset[offset_key] += 1

    # Log eight messages within a 60-second period
    offset_ms = [10000, 20000, 30000, 40000, 50000, 60000, 65000, 69999]
    for idx in range(len(offset_ms)):
        _process(offset_ms[idx], None, b'message#%d' % idx)

    assert store.save.call_count == 0

    # Log one more message at the sixty second mark
    _process(70000, b'with key', b'final countdown')

    assert store.save.call_count == 1
    (buffer, ) = store.save.call_args[0]
    assert list(reader(buffer.get_rewound_file())) == [
        {
            'key': None,
            'value': b'message#%d' % idx,
            'timestamp': start_ms + offset_ms[idx],
        }
        for idx in range(len(offset_ms))
    ]

    handler.process_eof(TOPIC, PARTITION)
    assert store.save.call_count == 1
    handler.no_message()
    assert store.save.call_count == 1
    handler.no_message()

    # This is the point at which if a message came through it would no longer
    # be in the same closed file.
    close_at_ms = start_ms + 70000 + max_age_ms
    mock_time.return_value = close_at_ms / 1000
    handler.no_message()
    assert store.save.call_count == 1

    # The file should only actually be saved, once we've accounted for kafka
    # potential skew.
    mock_time.return_value = (close_at_ms + KAFKA_SKEW_MS - 1) / 1000
    handler.no_message()
    assert store.save.call_count == 1

    mock_time.return_value = (close_at_ms + KAFKA_SKEW_MS) / 1000
    handler.no_message()
    assert store.save.call_count == 2

    # Now that we got a call, ensure the written file looks good
    (buffer, ) = store.save.call_args[0]
    assert list(reader(buffer.get_rewound_file())) == [{
        'key': b'with key',
        'value': b'final countdown',
        'timestamp': start_ms + 70000,
    }]
