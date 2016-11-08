## Kafka Store

[![Build Status](https://travis-ci.org/smyte/kafka_store.svg?branch=master)](https://travis-ci.org/smyte/kafka_store)

Kafka store provides a safe method for long term archiving of Kafka topics.

## Features

* **Simple guarantee**. Kafka Store ensures that every single message in a Kafka topic is backed up to Google Cloud Storage **exactly once**, with a **predictable filename** and in a **fault tolerant** manner.
* Saves large **compressed** avro-encoded files to your server with **low memory requirements**.
* Optionally logs files to a **MySQL table** with offset ranges for quicker lookup.

## Comparison to Secor

This tool is *very* similar to a previously released tool called [Secor](https://github.com/pinterest/secor). We started out using Secor, but our motivation for writing a replacement was primarily due to the **predictable filename** guarantee, as well as many production problems while trying to use a tool that was far more complicated than necessary for our use case.

Our guarantee is **stronger**. By using the new timestamp feature of Kafka we can ensure that each message always lands up in the **same file**. Since our files are *always* named with the offset of the initial message, streaming from S3 is simplified since the filename of the next dump is predictable (`final_offset + 1`).


* We only target long term archiving. There is no support for output partitioning, transformation, etc.
* There is no statistics interface. We recommend alarming based on Kafka lag.

## Requirements

* Timestamps must be enabled on your Kafka Broker. This requires newer versions of Kafka and minimum protocol 0.10.0.0 enabled.
* A `librdkafka` version that supports timestamps. If you're using compression you might want to check our [un-merged patch](https://github.com/edenhill/librdkafka/pull/858).
* We do not (yet) support compacted topics.

## Example

```
# Write some sample data into partition 5 on the `sample` topic
$ (echo hello; sleep 5; echo world; sleep 15; echo '!') | kafkacat -P -b localhost -t sample -p 5

# Start up the kafka store
$ kafka-store --broker-list localhost --topic test --group kafka-store --local-store ~/kafka-data/ --offset-reset earliest --verbose
INFO:pylib.seqconsumer:Consuming from partitions: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
INFO:kafka_store.buffer:Saving sample/000005/00000000000000000000 > /tmp/tmpirblvjzx
INFO:kafka_store.buffer:Closed sample/000005/00000000000000000000 > /tmp/tmpirblvjzx records=2 0.3kB
INFO:kafka_store.handler:Committed sample/000005/00000000000000000000
INFO:kafka_store.buffer:Saving sample/000005/00000000000000000002 > /tmp/tmpkz9mro1t

# In a separate window
$ kafka-store-reader local --wait ~/kafka-data/sample/000005/00000000000000000000
{"filename": "00000000000000000000", "key": null, "offset": 0, "timestamp": 1478570870012, "value": "hello"}
{"filename": "00000000000000000000", "key": null, "offset": 1, "timestamp": 1478570875023, "value": "world"}
Next file not ready yet. Waiting for: /home/josh/kafka-data/sample/000005/00000000000000000002
{"filename": "00000000000000000002", "key": null, "offset": 2, "timestamp": 1478570890054, "value": "!"}
Next file not ready yet. Waiting for: /home/josh/kafka-data/sample/000005/00000000000000000003

```

**NOTE**: The `offset-reset` is required for the initial run, but not recommended to be left on in production after that.

You can also see that the final message `'!'` does not come through immediately. The first file is closed after "world" because of twenty seconds elapsed from "hello" to "!", but since there are no more messages the final file is not immediately closed. We cannot guarantee Kafka will not send a message with a timestamp <15 seconds after the previous message ([time is hard](http://infiniteundo.com/post/25326999628/falsehoods-programmers-believe-about-time)).

Eventually if there is no more traffic on the topic it will be closed anyway. The current setting waits eight hours to be super safe, but that ensures that topics with no more traffic are committed eventually.

## Future work

We're releasing a product that works for our requirements, but we're very aware it won't fulfil all (or even most) of potential use cases. Unfortunately as a startup we don't have the time to spare to complete these, but we're happy to review pull requests and work with the community to get required features out the door.

* Using a configuration file rather than taking all options via the command line. This will be a pre-requisite for most of the other tasks.
* Full support for Google Cloud authentication. At the moment we're running inside GCE so the default authentication *just works*.
* Support for S3, Azure, and other long term storage systems.
* Consuming from mulitple topics on the same instance. At the moment we only support a single topic.
