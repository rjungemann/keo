(ns keo.producer
  (:require [perseverance.core]
            [keo.env])
  (:import java.util.Properties
           org.apache.kafka.clients.producer.KafkaProducer
           org.apache.kafka.clients.producer.ProducerConfig
           org.apache.kafka.clients.producer.ProducerRecord
           org.apache.kafka.common.KafkaException
           org.apache.kafka.common.serialization.StringSerializer
           org.apache.kafka.clients.producer.ProducerRecord
           org.apache.kafka.common.TopicPartition
           org.apache.kafka.clients.consumer.OffsetAndMetadata))

(defonce retry-strategy
  (perseverance.core/progressive-retry-strategy :initial-delay 1000
                                                :max-delay 10000
                                                :max-count 5))

(defn find-out-offsets [records]
  (reduce #(assoc %1
                  (TopicPartition. (. %2 topic) (. %2 partition))
                  (OffsetAndMetadata. (. %2 offset)))
          {}
          records))

(defn create-producer []
  (KafkaProducer. (doto (Properties.)
                        (.put "bootstrap.servers" keo.env/kafka-servers)
                        (.put ProducerConfig/TRANSACTIONAL_ID_CONFIG keo.env/kafka-producer-transactional-id)
                        (.put ProducerConfig/ENABLE_IDEMPOTENCE_CONFIG "true"))
                  (StringSerializer.)
                  (StringSerializer.)))

;; ------------
;; Exactly-Once
;; ------------

(defn init-producer [producer]
  (. producer initTransactions)
  producer)

(defn send-records-with-producer [producer in-records out-records]
  (perseverance.core/retry {:strategy retry-strategy
                            :catch [KafkaException]}
    (try
      (. producer beginTransaction)
      (doseq [record out-records]
        (. producer send record))
      (. producer sendOffsetsToTransaction (find-out-offsets in-records) keo.env/kafka-consumer-group-id)
      (. producer commitTransaction)
      (catch KafkaException e
        ; Catch the KafkaException, abort the transaction, and re-raise.
        (println "Failed to publish records. Retrying...")
        (. producer abortTransaction)
        (throw e)))))

;; ----------------
;; Non-Exactly-Once
;; ----------------

(defn create-basic-producer []
  (KafkaProducer. (doto (Properties.)
                        (.put "bootstrap.servers" keo.env/kafka-servers))
                  (StringSerializer.)
                  (StringSerializer.)))

(defn basic-send-records-with-producer [producer out-records]
  (perseverance.core/retry {:strategy retry-strategy
                            :catch [KafkaException]}
    (try
      (doseq [record out-records]
        (. producer send record))
      (catch KafkaException e
        ; Catch the KafkaException, abort the transaction, and re-raise.
        (println "Failed to publish records. Retrying...")
        (throw e)))))
