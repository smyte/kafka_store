#!/usr/bin/env python3
"""kafka-store

Usage:
  ./main [options]
  ./main (-h | --help)

Options:
  -h --help                Show this screen.
  -v --verbose             Enable verbose output

  --gcloud-url <url>       Google store url for files
  --mysql-url <url>        MySQL server to write updates to
  --broker-list <broker>   Kafka broker list
  --topic <topic>          Consumer topic
  --group <group>          Consumer group
  --source <source>        Consumer source
  --offset-reset <reset>   Reset offset on start
"""
import logging
from docopt import docopt

from .handler import KafkaStoreHandler
from .store import (
    GCloudStore,
    MySQLStore,
)
from smyte_pylib.kafka.loop import KafkaLoop

UPLOAD_BYTES = 1024 * 1024 * 64
UPLOAD_MAX_AGE_MS = 45 * 60 * 1000

# Try fetch some Smyte-specific initialization configuration, but fall back to
# sane defaults if they're missing
try:
    from pylib.config import setup_config
    from pylib.log import configure_logging
    from pylib.kafka10 import get_bootstrap_servers
except ImportError:
    setup_config = lambda paths: None
    get_bootstrap_servers = lambda group: None
    def configure_logging(level):
        logging.getLogger().setLevel(level)

def main():
    arguments = docopt(__doc__)

    if setup_config:
        setup_config([])

    configure_logging(
        logging.DEBUG if arguments['--verbose'] else logging.INFO
    )

    group = arguments.get('--group')
    assert arguments.get('--topic')
    assert group

    config = {
        'queued.max.messages.kbytes': 10 * 1024,
    }

    broker_list = arguments.get('--broker-list')
    if broker_list is None and get_bootstrap_servers:
        broker_list = get_bootstrap_servers(group)
    if broker_list is not None:
        config['metadata.broker.list'] = broker_list

    loop = KafkaLoop(
        topic=arguments.get('--topic'),
        group=arguments['--group'],
        auto_commit=False,
        offset_reset=arguments.get('--offset-reset') or 'error',
        consumer_config=config,
    )

    stores = []
    if arguments['--gcloud-url']:
        stores.append(
            GCloudStore(arguments['--gcloud-url'])
        )
    if arguments['--mysql-url']:
        stores.append(
            MySQLStore(arguments['--mysql-url'])
        )

    loop.run(KafkaStoreHandler(
        loop,
        stores=stores,
        max_age_ms=UPLOAD_MAX_AGE_MS,
        max_size=UPLOAD_BYTES,
    ))

if __name__ == '__main__':
    main()
