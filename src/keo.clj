(ns keo
  (:require [clojure.stacktrace]
            [perseverance.core])
  (:import java.util.Properties
           org.apache.kafka.clients.consumer.KafkaConsumer
           org.apache.kafka.clients.consumer.ConsumerConfig
           org.apache.kafka.clients.consumer.ConsumerRecords
           org.apache.kafka.clients.consumer.OffsetAndMetadata
           org.apache.kafka.clients.producer.KafkaProducer
           org.apache.kafka.clients.producer.ProducerConfig
           org.apache.kafka.clients.producer.ProducerRecord
           org.apache.kafka.common.KafkaException
           org.apache.kafka.common.TopicPartition
           org.apache.kafka.common.errors.AuthorizationException
           org.apache.kafka.common.errors.OutOfOrderSequenceException
           org.apache.kafka.common.errors.ProducerFencedException
           org.apache.kafka.common.requests.IsolationLevel
           org.apache.kafka.common.serialization.StringSerializer))

;; ---------------------
;; Environment variables
;; ---------------------

(defonce env
  (java.lang.System/getenv))

(defonce kafka-servers
  (or (get env "KAFKA_SERVERS") "localhost:9092"))

(defonce kafka-producer-transactional-id
  (or (get env "KAFKA_PRODUCER_TRANSACTIONAL_ID") "my-transactional-id"))

(defonce kafka-consumer-group-id
  (or (get env "KAFKA_CONSUMER_GROUP_ID") "my-transactional-consumer-group"))

(defonce in-topics
  (clojure.string/split (or (get env "IN_TOPICS") "foo") #":"))

; TODO: Consider a mapping between in topics and out topics.
(defonce out-topic
  (or (get env "OUT_TOPIC") "bar"))

(defonce poll-size
  (Integer/parseInt (or (get env "POLL_SIZE") "100")))

;; -------
;; Helpers
;; -------

(defn find-out-offsets [records]
  (reduce #(assoc %1
                  (TopicPartition. (. %2 topic) (. %2 partition))
                  (OffsetAndMetadata. (. %2 offset)))
          {}
          records))

; Composes functions in the order they are written (reverse of `comp`).
(defn thrush [& args]
  (reduce #(%2 %1) args))

(defn serialize-record [record]
  {:checksum  (. record checksum)
   :key       (. record key)
   :offset    (. record offset)
   :partition (. record partition)
   :topic     (. record topic)
   :timestamp (. record timestamp)
   :value     (. record value)})

(defonce retry-strategy
  (perseverance.core/progressive-retry-strategy :initial-delay 1000
                                                :max-delay 10000
                                                :max-count 5))

;; --------
;; Producer
;; --------

(defn create-producer []
  (KafkaProducer. (doto (Properties.)
                        (.put "bootstrap.servers" kafka-servers)
                        (.put ProducerConfig/TRANSACTIONAL_ID_CONFIG kafka-producer-transactional-id)
                        (.put ProducerConfig/ENABLE_IDEMPOTENCE_CONFIG "true"))
                  (StringSerializer.)
                  (StringSerializer.)))

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
      (. producer sendOffsetsToTransaction (find-out-offsets in-records) kafka-consumer-group-id)
      (. producer commitTransaction)
      (catch KafkaException e
        ; Catch the KafkaException, abort the transaction, and re-raise.
        (println "Failed to publish records. Retrying...")
        (. producer abortTransaction)
        (throw e)))))

;; --------
;; Consumer
;; --------

(defn create-consumer []
  (KafkaConsumer. (doto (Properties.)
                        (.put "bootstrap.servers" kafka-servers)
                        (.put "group.id" kafka-consumer-group-id)
                        (.put "enable.auto.commit" "false")
                        (.put "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer")
                        (.put "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer")
                        (.put ConsumerConfig/ISOLATION_LEVEL_CONFIG "read_committed"))))

(defn init-consumer [consumer]
  (. consumer subscribe in-topics)
  consumer)

(defn fetch-records-with-consumer [consumer poll-size]
  (. consumer poll poll-size))

;; ------------------
;; Pipeline Functions
;; ------------------

; Examine the record coming in.
(defn print-and-pass-records [records]
  (mapcat (fn [record]
            (println (serialize-record record))
            [record])
          records))

; Stateful function that counts records coming in.
(defn count-records [state]
  (fn [records]
    (let [current-count (count records)]
      (if (< 0 current-count)
        (do
          (swap! state #(assoc % :count (+ (or (:count %) 0)
                                           current-count)))
          (println "Record count:" (:count @state))
          records)))))

; Generate the record(s) going out.
(defn generate-producer-records [records]
  (mapcat (fn [record]
            [(ProducerRecord. out-topic
                              (. record partition)
                              (. record timestamp)
                              (. record key)
                              (. record value))])
          records))

;; ----
;; Main
;; ----

(defn -main []
  (let [consumer (-> (create-consumer)
                     (init-consumer))
        producer (-> (create-producer)
                     (init-producer))
        state (atom {})
        ; Chain more functions here of the form
        ; `[ConsumerRecord] -> [ConsumerRecord]`.
        ;
        ; Of course, you can write a function which returns something else, then
        ; a subsequent function which takes that value.
        ;
        pipeline #(thrush %
                          print-and-pass-records
                          (count-records state)
                          generate-producer-records)]
    (loop []
      (let [in-records (fetch-records-with-consumer consumer poll-size)
            out-records (pipeline in-records)]
        (send-records-with-producer producer in-records out-records))
      (recur))))
