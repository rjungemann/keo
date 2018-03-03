(ns keo
  (:require [clojure.stacktrace])
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

;; --------
;; Producer
;; --------

(defn create-producer []
  (let [props (Properties.)]
    (. props put "bootstrap.servers" kafka-servers)
    (. props put ProducerConfig/TRANSACTIONAL_ID_CONFIG kafka-producer-transactional-id)
    (. props put ProducerConfig/ENABLE_IDEMPOTENCE_CONFIG "true")
    (KafkaProducer. props (StringSerializer.) (StringSerializer.))))

(defn init-producer [producer]
  (. producer initTransactions)
  producer)

;; --------
;; Consumer
;; --------

(defn create-consumer []
  (let [props (Properties.)]
    (. props put "bootstrap.servers" kafka-servers)
    (. props put "group.id" kafka-consumer-group-id)
    (. props put "enable.auto.commit" "false")
    (. props put "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer")
    (. props put "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer")
    (. props put ConsumerConfig/ISOLATION_LEVEL_CONFIG "read_committed")
    (KafkaConsumer. props)))

(defn init-consumer [consumer]
  (. consumer subscribe in-topics)
  consumer)

;; -------
;; Helpers
;; -------

(defn find-out-offsets [records]
  (reduce (fn [result record]
            (assoc result
                   (TopicPartition. (. record topic) (. record partition))
                   (OffsetAndMetadata. (. record offset))))
          {}
          records))

(defn make-pipeline [consumer producer f]
  (try
    (loop []
      (let [records (. consumer poll poll-size)
            producer-records (f records)]
        (. producer beginTransaction)
        (doseq [record producer-records]
          (. producer send record))
        (. producer sendOffsetsToTransaction (find-out-offsets records) kafka-consumer-group-id)
        (. producer commitTransaction))
      (recur))
    (catch Exception e
      (case (type e)
        ; Stop processing on these exceptions.
        (ProducerFencedException OutOfOrderSequenceException AuthorizationException)
        (do
          (clojure.stacktrace/print-stack-trace e)
          (. producer close))
        ; Abort the transaction but continue on this exception.
        KafkaException
        (do
          (clojure.stacktrace/print-stack-trace e)
          (. producer abortTransaction))
        ; Otherwise, just re-raise.
        (throw e)))))

; Composes functions in the order they are written (reverse of `comp`).
(defn thrush [& args]
  (reduce #(%2 %1) args))

;; ------------------
;; Pipeline Functions
;; ------------------

(defn print-and-pass [records]
  (mapcat (fn [record]
            ; Examine the record coming in.
            (println {:checksum  (. record checksum)
                      :key       (. record key)
                      :offset    (. record offset)
                      :partition (. record partition)
                      :topic     (. record topic)
                      :timestamp (. record timestamp)
                      :value     (. record value)})
            [record])
          records))

(defn generate-producer-records [records]
  (mapcat (fn [record]
            ; Generate the record(s) going out.
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
  (make-pipeline (-> (create-consumer)
                     (init-consumer))
                 (-> (create-producer)
                     (init-producer))
                 #(thrush %
                          print-and-pass
                          ; Chain more functions here of the form
                          ; `[ConsumerRecord] -> [ConsumerRecord]`.
                          ;
                          ; Of course, you can write a function which returns
                          ; something else, then a subsequent function which
                          ; takes that value.
                          ;
                          generate-producer-records)))
