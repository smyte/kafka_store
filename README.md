## Kafka Store

[![Build Status](https://travis-ci.org/smyte/kafka_store.svg?branch=master)](https://travis-ci.org/smyte/kafka_store)

Kafka store provides a safe method for long term archiving of Kafka topics.

## Features

* **Simple guarantee**. Properly set up Kafka Store ensures that every single message in a Kafka topic is backed up to Google Cloud Storage **exactly once**, with a **predictable filename** and in a **fault tolerent** manner.
* Saves large **compressed** avro-encoded files to your server with **low memory requirements**.
* Optionally logs files to a **MySQL table** with offset ranges for quicker lookup.

## Comparison to Secor

This tool is *very* similar to a previously released tool called [Secor](https://github.com/pinterest/secor). We started out using Secor, but our motivation for writing a replacement was primarily due to the **predictable filename** guarantee, as well as many production problems while trying to use a tool that was far more complicated than neccessary for our use case.

Our guarantee is **stronger**. By using the new timestamp feature of Kafka we can ensure that each message always lands up in the **same file**. Since our files are *always* named with the offset of the initial message, streaming from S3 is simplified since the filename of the next dump is predictable (`final_offset + 1`).


* We only target long term archiving. There is no support for output partitioning, transformation, etc.
* There is no statistics interface. We recommend alarming based on Kafka lag.

## Requirements

* Timestamps must be enabled on your Kafka Broker. This requires newer versions of Kafka and minimum protocol 0.10.0.0 enabled.
* Your `librdkafka` must be support timestamps. If you're using compression you might want to check our [un-merged patch](https://github.com/edenhill/librdkafka/pull/858).
* We do not (yet) support compacted topics.

## Example

## Future work

We're releasing a product that works as required by us, but we're very aware it won't fulful all (or even most) of potential use cases. Unfortunately as a startup we don't have the time to spare to complete these, but we're happy to review pull requests and work with the community to get required features out the door.

* Configuration file rather than taking all options via the command line. This will be a pre-requisite for most of the other tasks.
* Full support for Google Cloud authentication. At the moment we're running inside GCE so the default authentication *just works*.
* Support for S3, Azure, and other long term storage systems.
* Consuming from mulitple topics on the same instance. At the moment we only support a single topic.
