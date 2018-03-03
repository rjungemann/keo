# Keo

## Introduction

Kafka recently supports exactly-once semantics, and there is a great Scala
example showing how it works. I ported the example to Clojure and used `thrush`
to combine pure functions into a pipeline.

Messages come in, get processed by the pipeline, and sent back out.

* [Kafka Exactly-Once Example in Scala](https://github.com/simplesteph/kafka-0.11-examples/blob/master/src/main/scala/au/com/simplesteph/kafka/kafka0_11/demo/ExactlyOnceLowLevel.scala)
* [Kafka Javadoc](https://kafka.apache.org/0102/javadoc/overview-summary.html)
* [Kafkacat](https://github.com/edenhill/kafkacat)

## Usage

```bash
# Install prerequisites.
brew install clojure kafka kafkacat
# If kafka crashes with an "ABORT TRAP", this *might* help.
sudo ulimit -c unlimited
# Start kafka.
./start-kafka.sh
# Start simple pipeline.
clj -m keo
# Listen for output messages.
kafkacat -b localhost:9092 -t bar
# Publish a message for the pipeline to pick up.
kafkacat -b localhost:9092 -t foo <<< 'Hello, world!'
```
