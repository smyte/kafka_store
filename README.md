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

* Timestamps must be enabled on your Kafka Broker.
* We do not (yet) support compacted topics.
* 
fault tolerance: any component of Secor is allowed to crash at any given point without compromising data integrity,
load distribution: Secor may be distributed across multiple machines,
horizontal scalability: scaling the system out to handle more load is as easy as starting extra Secor processes. Reducing the resource footprint can be achieved by killing any of the running Secor processes. Neither ramping up nor down has any impact on data consistency,

* TODO

