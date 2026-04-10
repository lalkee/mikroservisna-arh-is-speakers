(ns mikroservisna-arh-is-speakers.repository
  (:require [next.jdbc.sql :as sql]
            [next.jdbc :as jdbc]))

(defn create-table! [ds]
  (jdbc/execute! ds ["CREATE TABLE IF NOT EXISTS speakers (
    id BIGINT NOT NULL AUTO_INCREMENT,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    title VARCHAR(255),
    expertise VARCHAR(255),
    PRIMARY KEY (id)
);"]))

(defn find-by-id [ds id]
  (sql/get-by-id ds :speakers id))

(defn find-all [ds]
  (sql/query ds ["SELECT * FROM speakers"]))

(defn save! [ds speaker]
  (sql/insert! ds :speakers speaker))

(defn delete! [ds id]
  (sql/delete! ds :speakers {:id id}))

