(ns cubby.storage.Collection
  (:require [clj-mmap :as mmap]
            [digest :as digest]
            [clojure.java.io :as io]
            [clojurewerkz.buffy.core :refer :all]
            [debugger.core :refer :all]
            [clojure.data.codec.base64 :as b64]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clojure.core.async :refer [go-loop <! timeout]])
  (:import (java.io RandomAccessFile)))


(def encodings ["US-ASCII"
                "ISO-8859-1"
                "UTF-8"
                "UTF-16BE"
                "UTF-16LE"
                "UTF-16"
                "bin"])

(def printLock (Object.))

(defn decode
  [base64-str]
  "Decode a base-64 encoded string, used for converting base64 to a byte array"
  (b64/decode (.getBytes base64-str "UTF-8")))

(defn safePrint
  [msg & args]
  (locking printLock (apply println msg args)))

(defn getLongTime
  []
  (c/to-long (t/now)))

(defn byteArrayToStr
  [b encoding]
  "Get an encoded string from a byte array"
  (if (nil? b)
    "nil"
    (try
      (.trim (String. b encoding))
      (catch Exception e (do (println "Error:" e) "nil")))))

(defn getSignature
  [s]
  "Get a 256 bit SHA hash of a string s"
  (digest/sha-256 s))

(defn ensureFileSize
  [f size]
  "Ensure that a particular file is at least a certain size"
  (let [needed (- size (.length f))]
    (if (> needed 0)
      (do
        (with-open [file (RandomAccessFile. f "rw")]
          (.setLength file (+ size needed)))
        true)
      false)))

(defn mapKV 
  [f coll]
  "Map a function over a collection"
  (reduce-kv (fn [m k v] (assoc m k (f [k v]))) (empty coll) coll))

(defn filterVals
  [pred m]
  "Filter a map by value with a predicate function"
  (into {} (filter (fn [[k v]] (pred v)) m)))

(defn movingSum
  ([coll]
    "Calculate moving sum starting at position 0"
    (movingSum coll 0))
  ([coll s]
    "Calculate moving sum from arbitatray position"
    (if (empty? coll)
      (empty coll)
      (let [new-val (+ s (first coll))]
        (lazy-seq
          (cons 
            new-val 
            (movingSum (rest coll) new-val)))))))

(defn sortedMapByValue
  ([m f]
    "If no order is set, default to descending"
    (sortedMapByValue m f 'desc))
  ([m f o]
    "Order a map by value using a function f and order ('desc or 'asc)"
    (cond 
      (= o 'desc)
        (do
          (into (sorted-map-by 
            (fn [key1 key2]
              (compare 
                [(f (get m key2)) key2]
                [(f (get m key1)) key1])))
                  m))
      (= o 'asc)
        (do
          (into (sorted-map-by 
            (fn [key1 key2]
              (compare 
                [(f (get m key1)) key1]
                [(f (get m key2)) key2])))
                  m))
      :else (throw (Exception. "Invalid order!")))))

(defn getEncodingFromCode
  [code]
  "Get the encoding name from numeric code.  Fall back to binary if the code passed in is out of range"
  (if (or (> code (- (count encodings) 1)) (< code 0))
    "bin"
    (nth encodings code)))

(defn getCodeFromEncoding
  [enc]
  "Get a numeric code/index from an encoding name"
  (let [ind (.indexOf encodings enc)]
    ;; Fall back to binary if the encoding passed in does not exist
    (if (or (nil? ind) (> ind (- (count encodings) 1)) (< ind 0))
      (- (count encodings) 1)
      ind)))

(defn writeCubbyAgent
  [^clojure.lang.PersistentArrayMap cur
   ^java.io.File f
   ^clojurewerkz.buffy.core.BuffyBuf header-buf
   ^java.lang.Long header-size
   ^java.lang.Long header-pos 
   ^java.lang.String to-write
   ^java.lang.Long write-pos
   ^java.lang.Long write-len
   mapped-file]
  "Don't actually write to disk if the data hasn't changed"
  (if (and (not (nil? cur)) (= (:signature (:header cur)) (get-field header-buf :signature)))
    cur
    (do
      (let [header (assoc (update (decompose header-buf) :encoding getEncodingFromCode) :lastupdate (getLongTime))
        header-bytes (clojurewerkz.buffy.util/read-nonempty-bytes (.buf header-buf) header-size)
        encoding (:encoding header)
        content (.getBytes to-write (if (= encoding "bin") "UTF-8" encoding))]
        (mmap/put-bytes mapped-file header-bytes header-pos)
        (mmap/put-bytes mapped-file content write-pos)
        {:header header :contents to-write}))))
    
(defn readCubbyAgent
  [^clojure.lang.PersistentArrayMap cur
   ^java.lang.Long id
   ^clojurewerkz.buffy.core.BuffyBuf header-buf
   ^java.lang.String filename
   ^java.lang.Long header-size
   ^java.lang.Long slot-capacity
   mapped-file]
  "Read the contents of a cubby agent, relying on the cache if available and disk as secondary"
  (let [header (assoc (update (decompose header-buf) :encoding getEncodingFromCode) :lastupdate (getLongTime))
    f (io/file filename)
    read-pos (+ header-size (* id (+ header-size slot-capacity)))
    read-len (:length header)
    encoding (:encoding header)] 
    ;; Don't actually read from disk if the data hasnt changed
    (if (and (not (nil? cur)) (= (:signature (:header cur)) (:signature header)))
      cur
      (if (and (.exists f) (<= (+ read-pos read-len) (.length f)))
        (do
          (let [raw-bytes (mmap/get-bytes mapped-file read-pos read-len) 
            msg (byteArrayToStr raw-bytes (if (= encoding "bin") "UTF-8" encoding))]
            {:header header :contents msg}))
        {:header nil :contents nil}))))

