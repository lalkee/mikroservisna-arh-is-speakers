(ns mikroservisna-arh-is-speakers.outbox
  (:require [mikroservisna-arh-is-speakers.repository :as repo]
            [langohr.basic :as lb]
            [next.jdbc :as jdbc]))

(defn process-outbox! [ds ch]
  (let [records (repo/find-pending-outbox ds)]
    (when (seq records)
      (println "[Outbox] Processing" (count records) "records")
      (doseq [record records]
        (println "[Outbox] Keys found in record:" (keys record))
        (try
          ;; Send the raw JSON string as bytes (matching your Java logic)
          (lb/publish ch "" (:queue record) (:payload record)
                      {:content-type "application/json"})
          ;; Delete after successful send
          (repo/delete-outbox-record! ds (:id record))
          (catch Exception e
            (println "[Outbox] Error processing record" (:id record) ":" (.getMessage e))))))))

(defn start-outbox-worker [ds ch]
  (.start (Thread. (fn []
                     (println "[OUTBOX] Started outbox worker.")
                     (while true
                       (Thread/sleep 3000) ;; Check every 3 seconds
                       (try
                         (process-outbox! ds ch)
                         (catch Exception e
                           (println "[Outbox Worker Loop Error]" (.getMessage e)))))))))