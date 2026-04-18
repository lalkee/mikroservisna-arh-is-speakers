(ns mikroservisna-arh-is-speakers.core
  (:require
   [clojure.stacktrace]
   [langohr.channel :as lch]
   [langohr.core :as rmq]
   [mikroservisna-arh-is-speakers.messaging :as messaging]
   [mikroservisna-arh-is-speakers.repository :as repo]
   [next.jdbc :as jdbc])
  (:gen-class))

(def db-spec {:dbtype "h2"
              :dbname "/home/milan/Documents/MikroservisnaArhitekturaIS/mikroservisna-arh-is-speakers/speakers_db;AUTO_SERVER=TRUE"})

(defn -main [& args]
  (println "Starting Speaker Microservice...")

  (let [ds (jdbc/get-datasource db-spec)]
    (repo/create-tables! ds)
    (println "Database initialized.")

    ;uses localhost:5672 by default
    (try
      (let [conn (rmq/connect)
            ch   (lch/open conn)]
        (messaging/start-consumers ch ds)
        @(promise))

      (catch Exception e
        (clojure.stacktrace/print-stack-trace e)
        (println "Failed to start service:" (.getMessage e))
        (System/exit 1)))))