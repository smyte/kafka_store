import os
import pytest
import time
from googleapiclient.http import HttpError

from kafka_store.buffer import PartitionBuffer
from kafka_store.store import (
    GCloudStore,
    MySQLStore,
)

TEST_GCLOUD_URL = 'gs://authbox-kafka-dev'
TEST_MYSQL_URL = 'mysql://127.0.0.1/kafka_store'

def _create_buffer():
    start_ms = int(time.time() * 1000)
    p = PartitionBuffer(
        topic='topic',
        partition=0,
        first_offset=0,
        first_timestamp_ms=start_ms,
        max_age_ms=3600 * 1000,
    )
    p.log(
        offset=0,
        key=b'',
        value=b'Hello world!',
        timestamp_ms=start_ms,
    )
    p.close()
    return p

@pytest.mark.skipif(
    os.environ.get('SKIP_INTEGRATION_TESTS'),
    reason='Skipping integration tests'
)
def test_gcloud():
    raise Exception('ooh')
    buffer = _create_buffer()

    # Make sure we can upload; but upload fails if md5 is wrong.
    store = GCloudStore(TEST_GCLOUD_URL)
    store.save(buffer)

    # Purposefully break the MD5 sum and ensure it raises an error
    buffer._output.md5.update(b'X')
    with pytest.raises(HttpError) as excinfo:
        store.save(buffer)
    excinfo.match('Provided MD5 hash "[^"]+" doesn\'t match calculated MD5 hash')

@pytest.mark.skipif(
    os.environ.get('SKIP_INTEGRATION_TESTS'),
    reason='Skipping integration tests'
)
def test_mysql():
    buffer = _create_buffer()
    store = MySQLStore(TEST_MYSQL_URL)
    store.save(buffer)
