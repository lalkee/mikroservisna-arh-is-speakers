(ns mikroservisna-arh-is-speakers.core
  (:require [langohr.core :as rmq]
            [langohr.channel :as lch]
            [next.jdbc :as jdbc]
            [mikroservisna-arh-is-speakers.repository :as repo]
            [mikroservisna-arh-is-speakers.messaging :as messaging])
  (:gen-class))

(def db-spec {:dbtype "h2" :dbname "./speakers_db"})

(defn -main [& args]
  (println "Starting Speaker Microservice...")

  (let [ds (jdbc/get-datasource db-spec)]
    (repo/create-table! ds)
    (println "Database initialized.")

    ;;uses localhost:5672 by default
    (try
      (let [conn (rmq/connect)
            ch   (lch/open conn)]
        (messaging/start-consumers ch ds)
        @(promise))

      (catch Exception e
        (println "Failed to start service:" (.getMessage e))
        (System/exit 1)))))