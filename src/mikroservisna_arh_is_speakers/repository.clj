(ns mikroservisna-arh-is-speakers.repository
  (:require [next.jdbc.sql :as sql]
            [next.jdbc :as jdbc]
            [clojure.string :as str]))
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
      ON DELETE RESTRICT
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
  (sql/delete! ds :speakers {:id id}))

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

