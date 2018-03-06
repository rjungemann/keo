(ns keo.consumer
  (:require [perseverance.core]
            [keo.env])
  (:import java.util.Properties
           org.apache.kafka.clients.consumer.KafkaConsumer
           org.apache.kafka.clients.consumer.ConsumerConfig
           org.apache.kafka.clients.consumer.ConsumerRecords))

(defn create-consumer []
  (KafkaConsumer. (doto (Properties.)
                        (.put "bootstrap.servers" keo.env/kafka-servers)
                        (.put "group.id" keo.env/kafka-consumer-group-id)
                        (.put "enable.auto.commit" "false")
                        (.put "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer")
                        (.put "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer")
                        (.put ConsumerConfig/ISOLATION_LEVEL_CONFIG "read_committed"))))

(defn init-consumer [consumer]
  (. consumer subscribe [keo.env/in-topic])
  consumer)

(defn fetch-records-with-consumer [consumer poll-size]
  (. consumer poll poll-size))
