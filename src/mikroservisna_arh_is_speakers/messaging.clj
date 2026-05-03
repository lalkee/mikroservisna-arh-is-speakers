(ns mikroservisna-arh-is-speakers.messaging
  (:require [cheshire.core :as json]
            [langohr.basic :as lb]
            [langohr.consumers :as lc]
            [langohr.queue :as lq] ; Added requirement
            [next.jdbc :as jdbc]
            [mikroservisna-arh-is-speakers.repository :as repo]))

(defn- decode [payload]
  (json/parse-string (String. payload "UTF-8") true))

(defn- encode [data]
  (json/generate-string data))

(defn- publish-response [ch metadata data]
  (println "[publish-response] reply-to:" (:reply-to metadata))
  (println "[publish-response] correlation-id:" (:correlation-id metadata))
  (println "[publish-response] data:" data)
  (let [reply-to (:reply-to metadata)
        corr-id (:correlation-id metadata)]
    (lb/publish ch "" reply-to (encode data)
                {:correlation-id corr-id})))

;===================== SPEAKER =====================================

(defn handle-get-all-speakers [ch metadata _ ds]
  (let [speakers (repo/find-all-speakers ds)]
    (println "[handle-get-all] result count:" (count speakers))
    (publish-response ch metadata speakers)))

(defn handle-get-speaker-by-id [ch metadata payload ds]
  (let [id (decode payload)
        speaker (repo/find-speaker-by-id ds id)]
    (println "[handle-get-by-id] decoded id:" id)
    (println "[handle-get-by-id] result:" speaker)
    (publish-response ch metadata speaker)))

(defn handle-save-speaker [payload ds]
  (let [data (decode payload)
        id (:id data)]
    (if (and id (repo/find-speaker-by-id ds id))
      (repo/update-speaker! ds id (dissoc data :id))
      (repo/insert-speaker! ds data))))

(defn handle-delete-speaker [payload ds]
  (let [id (decode payload)]
    (println "[handle-delete] decoded id:" id)
    (repo/delete-speaker! ds id)))

(defn handle-get-speakers-by-events [ch metadata payload ds]
  (let [event-ids (decode payload) ; expecting a list [1, 2, 3]
        grouped-map (repo/find-speakers-by-event-ids ds event-ids)
        ; ensure we return a list of lists in the exact order requested
        ordered-results (map #(get grouped-map % []) event-ids)]
    (println "[handle-get-speakers-by-events] processing IDs:" event-ids)
    (publish-response ch metadata ordered-results)))

;===================== PARTICIPATION ===============================

(defn handle-save-participation [ch payload ds]
  (let [participations (decode payload)]
    (if (seq participations)
      (let [event-id (:eventId (first participations))]
        (jdbc/with-transaction [tx ds]
          (repo/delete-participations-by-event! tx event-id)
          (doseq [p participations]
            (repo/insert-participation! tx (repo/transform-in p))))
        ; signal completion
        (lb/publish ch "" "participation.save.res"
                    (encode {:eventId event-id :status "done"})))
      (println "[handle-save-participations] Empty list, skipping."))))

(defn handle-delete-participation [payload ds]
  (let [id (decode payload)]
    (repo/delete-participations-by-event! ds id)))

;===================== CONSUMER ====================================

(defn start-consumers [ch ds]
  ;; Explicitly declare queues before subscribing
  (doseq [q ["speaker.get.all" "speaker.save" "speaker.delete"
             "participation.save" "participation.delete"
             "speaker.get.id" "speaker.get.byEventIds"]]
    (lq/declare ch q {:durable true :exclusive false :auto-delete false}))

  (lc/subscribe ch "speaker.get.all"
                (fn [ch metadata payload]
                  (println "\n[QUEUE speaker.get.all] RECEIVED")
                  (handle-get-all-speakers ch metadata payload ds))
                {:auto-ack true})

  (lc/subscribe ch "speaker.save"
                (fn [_ _ payload]
                  (println "\n[QUEUE speaker.save] RECEIVED")
                  (handle-save-speaker payload ds))
                {:auto-ack true})

  (lc/subscribe ch "speaker.delete"
                (fn [_ _ payload]
                  (println "\n[QUEUE speaker.delete] RECEIVED")
                  (handle-delete-speaker payload ds))
                {:auto-ack true})

  (lc/subscribe ch "participation.save"
                (fn [ch _ payload]
                  (println "\n[QUEUE participation.save] RECEIVED")
                  (handle-save-participation ch payload ds))
                {:auto-ack true})

  (lc/subscribe ch "participation.delete"
                (fn [_ _ payload]
                  (println "\n[QUEUE participation.delete] RECEIVED")
                  (handle-delete-participation payload ds))
                {:auto-ack true})

  (lc/subscribe ch "speaker.get.id"
                (fn [ch metadata payload]
                  (println "\n[QUEUE speaker.get.id] RECEIVED")
                  (handle-get-speaker-by-id ch metadata payload ds))
                {:auto-ack true})

  (lc/subscribe ch "speaker.get.byEventIds"
                (fn [ch metadata payload]
                  (println "\n[QUEUE speaker.get.byEventIds] RECEIVED")
                  (handle-get-speakers-by-events ch metadata payload ds))
                {:auto-ack true}))