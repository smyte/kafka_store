import time
from mock import patch

from kafka_store.buffer import (
    KAFKA_SKEW_MS,
    PartitionBuffer,
)
from kafka_store.read import reader

MINUTE_IN_MS = 60 * 1000
MAX_AGE_MS = 10 * MINUTE_IN_MS

def test_buffer():
    # Used a fixed starting timestamp
    start_ms = 1475273684343

    p = PartitionBuffer(
        topic='topic',
        partition=0,
        first_offset=0,
        first_timestamp_ms=start_ms,
        max_age_ms=MAX_AGE_MS,
    )

    assert not p.is_closed(start_ms)
    assert not p.is_closed(start_ms + MAX_AGE_MS - 1)
    assert p.is_closed(start_ms + MAX_AGE_MS)

    for offset in range(5):
        timestamp_ms = start_ms + offset * MINUTE_IN_MS
        p.log(
            offset=offset,
            key=b'',
            value=('<%d>' % offset).encode('ascii'),
            timestamp_ms=timestamp_ms
        )
        assert not p.is_closed(timestamp_ms)

    # Make sure that the silent commit happens at the expected time
    # (time of the first message seen + file max age + allowance for kafka skew)
    silent_commit_time = (start_ms + MAX_AGE_MS + KAFKA_SKEW_MS) / 1000
    with patch('time.time') as mock_time:
        mock_time.return_value = (start_ms) / 1000
        assert not p.is_silent_closed()
        mock_time.return_value = silent_commit_time
        assert not p.is_silent_closed()

        # Now mark an eof on the file
        p.mark_eof()

        mock_time.return_value = (start_ms) / 1000
        assert not p.is_silent_closed()
        mock_time.return_value = silent_commit_time
        assert p.is_silent_closed()
        mock_time.return_value = silent_commit_time - 0.1
        assert not p.is_silent_closed()
        mock_time.return_value = silent_commit_time + 0.1
        assert p.is_silent_closed()


    assert p.is_closed(start_ms + MAX_AGE_MS)
    assert p.commit_next_offset == 5

    # Make sure we can read the records on the other side
    p.close()

    records = list(reader(p.get_rewound_file()))
    assert records == [
        {'key': b'', 'timestamp': start_ms + idx * MINUTE_IN_MS, 'value': b'<%d>' % idx}
        for idx in range(5)
    ]
