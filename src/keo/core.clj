(ns keo.core
  (:require [perseverance.core]
            [keo.env]
            [keo.consumer]
            [keo.producer])
  (:import org.apache.kafka.clients.producer.ProducerRecord))

;; -------
;; Helpers
;; -------

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

;; --------
;; Consumer
;; --------

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
;
; NOTE: You may need to play with the partition value.
;
(defn generate-producer-records [records]
  (mapcat (fn [record]
            [(ProducerRecord. keo.env/out-topic
                              (. record partition)
                              (. record timestamp)
                              (. record key)
                              (. record value))])
          records))

;; ----
;; Main
;; ----

; Add your own functions within the `thrush` call below.
(defn -main []
  (let [consumer (-> (keo.consumer/create-consumer)
                     (keo.consumer/init-consumer))
        producer (-> (keo.producer/create-producer)
                     (keo.producer/init-producer))
        state (atom {})
        ; Chain more functions here of the form
        ; `[ConsumerRecord] -> [ConsumerRecord]`.
        ;
        ; Of course, you can write a function which returns something else, then
        ; a subsequent function which takes that modified value.
        ;
        pipeline #(thrush % print-and-pass-records
                            (count-records state)
                            generate-producer-records)]
    (loop []
      (let [in-records (keo.consumer/fetch-records-with-consumer consumer keo.env/poll-size)
            out-records (pipeline in-records)]
        (keo.producer/send-records-with-producer producer in-records out-records))
      (recur))))
