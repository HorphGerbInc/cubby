(ns cubby.core
  (:require [debugger.core :refer :all]
            [clj-time.core :as t]
            [clojure.tools.trace :as trace])
  (:import  [java.io RandomAccessFile]))

(def collection-manager-map (atom {}))

;; Forward declarators
(declare add-ref)
(declare get-collection-manager)
(declare add-collection-manager)
(declare ensure-collection-manager)

;; Create a new collection manager for a collection and add it
;;
;;  @collectionName Name of the collection
(defn add-collection-manager
  [^java.lang.String collectionName]
  (swap! collection-manager-map assoc collectionName (cubby.storage.Collection. collectionName)))

;; Get the collection manager for a collection
;;
;;  @collectionName Name of the collection
(defn get-collection-manager
  [^java.lang.String collectionName]
  (get @collection-manager-map collectionName))

;; Ensure that a collection manager exists for a collection.  If it
;; it does not exist it is created.
;;
;;  @collectionName The name of the collection
(defn ensure-collection-manager
  [^java.lang.String collectionName]
  (if (nil? (get-collection-manager collectionName))
    (add-collection-manager collectionName)))

;; Write to a collection
;;
;;  @collectionName The name of the collection
;;  @spot The bucket to write to
;;  @msg The message to write
;;  @encoding The underlying encoding
(defn write-cubby
  ([^java.lang.String collectionName ^java.lang.Long spot msg] 
    (write-cubby collectionName spot msg (if (= (str (type msg)) "class [B") "bin" "UTF-8")))
  ([^java.lang.String collectionName ^java.lang.Long spot ^java.lang.String msg ^java.lang.String encoding]
    (ensure-collection-manager collectionName)
    (if (= encoding "bin")
      (.write (get-collection-manager collectionName) spot msg)
      (.write (get-collection-manager collectionName) spot msg encoding))))

;; Read from a collection
;;
;;  @collectionName The name of the collection
;;  @spot Bucket to read from
;;  @add-header? Switch to add a header containing metadata about a bucket
(defn read-cubby
  ([^java.lang.String collectionName ^java.lang.Long spot]
    (read-cubby collectionName spot false))
  ([^java.lang.String collectionName ^java.lang.Long spot ^java.lang.Boolean add-header?]
    (ensure-collection-manager collectionName)
    (.read (get-collection-manager collectionName) spot add-header?)))

;; Get then name of the underlying file for a collection
;;
;;  @collectionName The name of the collection
(defn get-filename
  [^java.lang.String collectionName]
  (.getFilename (get-collection-manager collectionName)))

  ;; Close all collections
(defn close-collections
  []
  (doseq [col @collection-manager-map]
    (.close (second col))))

