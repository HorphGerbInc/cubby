(ns cubby.core
  (:require [debugger.core :refer :all]
            [clj-time.core :as t]
            [clojure.tools.trace :as trace])
  (:import  [java.io RandomAccessFile]))

(def collection-manager-map (atom {}))

;; Declare functions
(declare add-ref)
(declare get-collection-manager)
(declare add-collection-manager)
(declare ensure-collection-manager)

(defn add-collection-manager
  [^java.lang.String collectionName]
  (swap! collection-manager-map assoc collectionName (cubby.storage.Collection. collectionName)))

(defn get-collection-manager
  [^java.lang.String collectionName]
  (get @collection-manager-map collectionName))

(defn ensure-collection-manager
  [^java.lang.String collectionName]
  (if (nil? (get-collection-manager collectionName))
    (add-collection-manager collectionName)))

(defn write-cubby
  ([^java.lang.String collectionName ^java.lang.Long spot msg] 
    (write-cubby collectionName spot msg (if (= (str (type msg)) "class [B") "bin" "UTF-8")))
  ([^java.lang.String collectionName ^java.lang.Long spot ^java.lang.String msg ^java.lang.String encoding]
    (ensure-collection-manager collectionName)
    (if (= encoding "bin")
      (.write (get-collection-manager collectionName) spot msg)
      (.write (get-collection-manager collectionName) spot msg encoding))))

(defn read-cubby
  ([^java.lang.String collectionName ^java.lang.Long spot]
    (read-cubby collectionName spot false))
  ([^java.lang.String collectionName ^java.lang.Long spot ^java.lang.Boolean add-header?]
    (ensure-collection-manager collectionName)
    (.read (get-collection-manager collectionName) spot add-header?)))

;; Get filename of a collection
(defn get-filename
  [^java.lang.String collectionName]
  (.getFilename (get-collection-manager collectionName)))

  ;; Close all colections
(defn close-collections
  []
  (doseq [col @collection-manager-map]
    (.close (second col))))

