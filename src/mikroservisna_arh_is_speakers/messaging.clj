(ns mikroservisna-arh-is-speakers.messaging
  (:require [cheshire.core :as json]
            [langohr.basic :as lb]
            [langohr.consumers :as lc]
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

;===================== BATCH EVENT LOOKUP ==========================

(defn handle-get-speakers-by-events [ch metadata payload ds]
  (let [event-ids (decode payload) ; Expecting a list [1, 2, 3] from Java
        grouped-map (repo/find-speakers-by-event-ids ds event-ids)
        ; Ensure we return a list of lists in the exact order requested
        ordered-results (map #(get grouped-map % []) event-ids)]
    (println "[handle-get-speakers-by-events] processing IDs:" event-ids)
    (publish-response ch metadata ordered-results)))

;===================== PARTICIPATION ===============================

(defn handle-save-participation [ch payload ds]
  (let [participations (decode payload)]
    (if (seq participations)
      (let [event-id (:eventId (first participations))]
        (jdbc/with-transaction [tx ds]
          (println "[handle-save-participations] Syncing event:" event-id)
          (repo/delete-participations-by-event! tx event-id)
          (doseq [p participations]
            (repo/insert-participation! tx (repo/transform-in p))))

        ;; Signal completion to a dedicated queue
        ;; We don't use publish-response here because this isn't an RPC call from the UI
        (lb/publish ch "" "participation.save.res"
                    (encode {:eventId event-id :status "done"})))
      (println "[handle-save-participations] Empty list, skipping."))))

;===================== CONSUMER ====================================

(defn start-consumers [ch ds]
  ;; 1. Standard CRUD Queues
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
                (fn [ch _ payload] ;; metadata ignored as outbox doesn't provide reply-to
                  (println "\n[QUEUE participation.save] RECEIVED")
                  (handle-save-participation ch payload ds))
                {:auto-ack true})

  ;; 2. Specific Query Queues
  ;; Used to get a specific speaker record by speaker-id
  (lc/subscribe ch "speaker.get.id"
                (fn [ch metadata payload]
                  (println "\n[QUEUE speaker.get.id] RECEIVED")
                  (handle-get-speaker-by-id ch metadata payload ds))
                {:auto-ack true})

  ;; Used by Event service to get speakers belonging to specific event-ids
  (lc/subscribe ch "speaker.get.byEventIds"
                (fn [ch metadata payload]
                  (println "\n[QUEUE speaker.get.byEventIds] RECEIVED")
                  (handle-get-speakers-by-events ch metadata payload ds))
                {:auto-ack true}))