(ns mikroservisna-arh-is-speakers.messaging
  (:require [cheshire.core :as json]
            [langohr.basic :as lb]
            [langohr.consumers :as lc]
            [mikroservisna-arh-is-speakers.repository :as repo]
            [clojure.string :as str]))

(defn- decode [payload]
  (json/parse-string  (String. payload "UTF-8") true))

(defn- encode [data]
  (json/generate-string data))

(defn- publish-response [ch metadata data]
  (println "[publish-response] metadata:" metadata)
  (println "[publish-response] reply-to:" (:reply-to metadata))
  (println "[publish-response] correlation-id:" (:correlation-id metadata))
  (println "[publish-response] data:" data)
  (let [reply-to (:reply-to metadata)
        corr-id (:correlation-id metadata)]
    (lb/publish ch "" reply-to (encode data)
                {:correlation-id corr-id})))

(defn handle-get-all [ch metadata _ ds]
  (let [speakers (repo/find-all ds)]
    (println "[handle-get-all] result count:" (count speakers))
    (publish-response ch metadata speakers)))

(defn handle-get-by-id [ch metadata payload ds]
  (let [id (decode payload)
        speaker (repo/find-by-id ds id)]
    (println "[handle-get-by-id] decoded id:" id)
    (println "[handle-get-by-id] result:" speaker)
    (publish-response ch metadata speaker)))

(defn handle-save [payload ds]
  (let [data (decode payload)]
    (println "[handle-save] decoded data:" data)
    (repo/save! ds data)))

(defn handle-delete [payload ds]
  (let [id (decode payload)]
    (println "[handle-delete] decoded id:" id)
    (repo/delete! ds id)))

(defn start-consumers [ch ds]
  (lc/subscribe ch "speaker.get.all"
                (fn [ch metadata payload]
                  (println "\n[QUEUE speaker.get.all] RECEIVED")
                  (handle-get-all ch metadata payload ds))
                {:auto-ack true})

  (lc/subscribe ch "speaker.get.id"
                (fn [ch metadata payload]
                  (println "\n[QUEUE speaker.get.id] RECEIVED")
                  (handle-get-by-id ch metadata payload ds))
                {:auto-ack true})

  (lc/subscribe ch "speaker.save"
                (fn [_ _ payload]
                  (println "\n[QUEUE speaker.save] RECEIVED")
                  (handle-save payload ds))
                {:auto-ack true})

  (lc/subscribe ch "speaker.delete"
                (fn [_ _ payload]
                  (println "\n[QUEUE speaker.delete] RECEIVED")
                  (println "payload:" payload)
                  (handle-delete payload ds))
                {:auto-ack true}))