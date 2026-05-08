(ns mikroservisna-arh-is-speakers.repository
  (:require [next.jdbc.sql :as sql]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [clojure.string :as str]
            [cheshire.core :as json]))

(defn create-tables! [ds]
  (jdbc/execute! ds ["CREATE TABLE IF NOT EXISTS speakers (
    id BIGINT NOT NULL AUTO_INCREMENT,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    title VARCHAR(255),
    expertise VARCHAR(255),
    PRIMARY KEY (id)
);"])
  (jdbc/execute! ds ["CREATE TABLE IF NOT EXISTS participations (
    id BIGINT NOT NULL AUTO_INCREMENT,
    event_id BIGINT,
    speaker_id BIGINT,
    PRIMARY KEY (id),
    CONSTRAINT fk_speaker
      FOREIGN KEY (speaker_id) 
      REFERENCES speakers(id)
      ON DELETE CASCADE);"])
  (jdbc/execute! ds ["CREATE TABLE IF NOT EXISTS outbox (
      id BIGINT NOT NULL AUTO_INCREMENT,
      queue VARCHAR(255),
      payload TEXT,
      timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (id)
  );"]))

(defn- to-camel [k]
  (let [parts (clojure.string/split (clojure.string/lower-case (name k)) #"_")]
    (keyword (apply str (first parts) (map clojure.string/capitalize (rest parts))))))

(defn transform-out [data]
  (cond
    (map? data)
    (reduce-kv (fn [m k v]
                 (assoc m (to-camel k) (transform-out v)))
               {}
               data)
    (sequential? data)
    (map transform-out data)
    :else data))

(defn- to-kebab [k]
  (let [s (name k)
        res (clojure.string/replace s #"([a-z])([A-Z])" "$1_$2")]
    (keyword (clojure.string/lower-case res))))

(defn transform-in [data]
  (if (map? data)
    (reduce-kv (fn [m k v] (assoc m (to-kebab k) v)) {} data)
    data))

;======================== OUTBOX ===================================

(defn insert-outbox! [tx queue payload]
  (sql/insert! tx :outbox {:queue queue
                           :payload payload}))

(defn find-pending-outbox [ds]
  (sql/query ds ["SELECT * FROM outbox ORDER BY timestamp ASC LIMIT 50"]
             {:builder-fn rs/as-unqualified-lower-maps}))

(defn delete-outbox-record! [ds id]
  (sql/delete! ds :outbox {:id id}))

;===================== SPEAKER =====================================

(defn insert-speaker! [ds speaker]
  (sql/insert! ds :speakers (transform-in speaker)))

(defn update-speaker! [ds id speaker]
  (sql/update! ds :speakers (transform-in speaker) {:id id}))

(defn find-speaker-by-id [ds id]
  (transform-out (sql/get-by-id ds :speakers id)))

(defn find-all-speakers [ds]
  (transform-out (sql/query ds ["SELECT * FROM speakers"])))

(defn delete-speaker! [ds id]
  (jdbc/with-transaction [tx ds]
    (let [speaker (find-speaker-by-id tx id)
          ;; FIX: Add the builder-fn here!
          event-ids (map :event_id
                         (sql/query tx
                                    ["SELECT event_id FROM participations WHERE speaker_id = ?" id]
                                    {:builder-fn rs/as-unqualified-lower-maps}))]

      (sql/delete! tx :speakers {:id id})

      (let [payload (json/generate-string {:speaker speaker
                                           :eventIds event-ids})]
        (insert-outbox! tx "events.delete.speaker" payload)))))

(defn find-speakers-by-event-ids [ds event-ids]
  (if (empty? event-ids)
    []
    (let [query (str "SELECT s.*, p.event_id FROM speakers s "
                     "JOIN participations p ON s.id = p.speaker_id "
                     "WHERE p.event_id IN ("
                     (clojure.string/join "," (repeat (count event-ids) "?"))
                     ")")
          results (transform-out (sql/query ds (into [query] event-ids)))]
      ;group the flat list into a map: {event-id [speaker1, speaker2]}    
      (group-by :eventId results))))

;===================== PARTICIPATION ===============================

(defn insert-participation! [ds participation]
  (sql/insert! ds :participations participation))

(defn delete-participations-by-event! [ds event-id]
  (println "[repo] Clearing old participations for event-id:" event-id)
  (sql/delete! ds :participations {:event_id event-id}))

(defn check-participation [ds speaker-id]
  (let [result (sql/query ds ["SELECT 1 FROM participations WHERE speaker_id = ? LIMIT 1" speaker-id])]
    (boolean (seq result))))

