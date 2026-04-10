(ns mikroservisna-arh-is-speakers.repository
  (:require [next.jdbc.sql :as sql]
            [next.jdbc :as jdbc]
            [clojure.string :as str]))

(defn create-table! [ds]
  (jdbc/execute! ds ["CREATE TABLE IF NOT EXISTS speakers (
    id BIGINT NOT NULL AUTO_INCREMENT,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    title VARCHAR(255),
    expertise VARCHAR(255),
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

;; Update your save! function
(defn save! [ds speaker]
  (sql/insert! ds :speakers (transform-in speaker)))

(defn find-by-id [ds id]
  (transform-out (sql/get-by-id ds :speakers id)))

(defn find-all [ds]
  (transform-out (sql/query ds ["SELECT * FROM speakers"])))

(defn save! [ds speaker]
  (sql/insert! ds :speakers (transform-in speaker)))

(defn delete! [ds id]
  (sql/delete! ds :speakers id))

