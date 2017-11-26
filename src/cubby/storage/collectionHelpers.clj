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

; Define all available encoding schemes
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

;; Ensure that a particular file is at least a certain size
;;
;;  @size The required size of the file
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

;; Map a function over a collection
;;
;;  @func   Mapping function
;;  @col    Map to apply function to
(defn mapKV 
  [func col]
  "Map a function over a collection"
  (reduce-kv (fn [m k v] (assoc m k (func [k v]))) (empty col) col))

;; Filter a map by value with a predicate function
;;
;;  @pred Predicate function
;;  @col  Map to filter
(defn filterVals
  [pred col]
  (into {} (filter (fn [[k v]] (pred v)) col)))

;;  Calculate prefix sum
;;
;;  @col                      Collection to sum over
;;  @[Optional,Default=0]sum  Starting sum
(defn movingSum
  ([col]
    (movingSum col 0))
  ([col sum]
    (if (empty? col)
      (empty col)
      (let [
          new-val (+ sum (first col))
        ]
        (lazy-seq
          (cons 
            new-val 
            (movingSum (rest col) new-val)))))))

;;  Sort a map by its value
;;
;;    @col                    The map
;;    @comp                   Compares two elements?
;;    @order[Default='desc]   'desc or 'asc
(defn sortedMapByValue
  ([col comp]
    "If no order is set, default to descending"
    (sortedMapByValue col comp 'desc))
  ([col comp order]
    "Order a map by value using a function f and order ('desc or 'asc)"
    (cond 
      (= order 'desc)
        (do
          (into (sorted-map-by 
            (fn [key1 key2]
              (compare 
                [(comp (get col key2)) key2]
                [(comp (get col key1)) key1])))
                  col))
      (= order 'asc)
        (do
          (into (sorted-map-by 
            (fn [key1 key2]
              (compare 
                [(comp (get col key1)) key1]
                [(comp (get col key2)) key2])))
                  col))
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
        (.release (.buf header-buf)) ; Release the buffer from memory
        {:header header :contents to-write}))))
    
(defn readCubbyAgent
  [^clojure.lang.PersistentArrayMap cur
   ^java.lang.Long id
   ^clojurewerkz.buffy.core.BuffyBuf header-buf
   ^java.lang.String filename
   ^java.lang.Long header-size
   ^java.lang.Long bucket-capacity
   mapped-file]
  "Read the contents of a cubby agent, relying on the cache if available and disk as secondary"
  (let [header (assoc (update (decompose header-buf) :encoding getEncodingFromCode) :lastupdate (getLongTime))
    f (io/file filename)
    read-pos (+ header-size (* id (+ header-size bucket-capacity)))
    read-len (:length header)
    encoding (:encoding header)] 
    (.release (.buf header-buf)) ; Release the buffer from memory
    ; Don't actually read from disk if the data hasnt changed
    (if (and (not (nil? cur)) (= (:signature (:header cur)) (:signature header)))
      cur
      (if (and (.exists f) (<= (+ read-pos read-len) (.length f)))
        (do
          (let [raw-bytes (mmap/get-bytes mapped-file read-pos read-len) 
            msg (byteArrayToStr raw-bytes (if (= encoding "bin") "UTF-8" encoding))]
            {:header header :contents msg}))
        {:header nil :contents nil}))))

