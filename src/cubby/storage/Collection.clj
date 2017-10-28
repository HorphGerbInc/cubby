(ns cubby.storage.Collection
  (:require [clj-mmap :as mmap]
            [digest :as digest]
            [clojure.java.io :as io]
            [clojurewerkz.buffy.core :refer :all]
            [debugger.core :refer :all]
            [clojure.data.codec.base64 :as b64]
            [clojure.core.async :refer [go-loop <! timeout]])
  (:import (java.io RandomAccessFile)))

  ;; Define class properties 
  (gen-class
    :name "cubby.storage.Collection"
    :state state
    :init init
    :post-init post-init
    :prefix "-"
    :main false
    :methods [[getFilename [] java.lang.String]
              [getHeaderSize [] java.lang.Integer]
              [getHeaderSize [clojurewerkz.buffy.core.BuffyBuf] java.lang.Integer]
              [readHeader [java.lang.Long] clojurewerkz.buffy.core.BuffyBuf] 
              [write [java.lang.Long bytes] java.lang.String]
              [write [java.lang.Long java.lang.String java.lang.String] java.lang.String]
              [read [java.lang.Long java.lang.Boolean] clojure.lang.PersistentArrayMap]
              [ensureAgent [java.lang.Long] void]
              [readRawHeader [java.lang.Long] bytes]
              [startGC [] void]
              [startGC [java.lang.Long] void]
              [gcAgents [] void]
              [close [] void]
              [getMappedFile [] clj_mmap.Mmap]
              [getAgents [] clojure.lang.Atom]
              [getAgent [java.lang.Long] clojure.lang.Agent]
              [getSlotCapacity [] java.lang.Long]]
    :constructors {[java.lang.String] []
                   [java.lang.String java.lang.Boolean java.lang.Long java.lang.Long] []})

;; Load helper functions
(load "collectionHelpers")

(def headerSpec (spec :signature (string-type 64) :length (int32-type) :encoding (byte-type)))
(def modelBuf (compose-buffer headerSpec))

;; Initialize a new cubby storage collection with a backing file.
(defn -init
  ([^java.lang.String filename]
    (-init filename true (* 4 1024 1024) (* 256 1024 1024)))
  ([^java.lang.String filename ^java.lang.Boolean gc? ^java.lang.Long slotCapacity ^java.lang.Long maxCache]
    [[] (atom {:filename (.getAbsolutePath (io/file filename))
               :slotCapacity slotCapacity 
               :maxCache maxCache
               :agents (atom {})
               :mapped-file (atom (mmap/get-mmap (.getAbsolutePath (io/file filename)) :read-write))})]))

;; After initialization we need to start garbage collection.
(defn -post-init
  ([this ^java.lang.String filename]
    "By default we should garbage collect agents"
    (future (.startGC this)))
  ([this ^java.lang.String filename ^java.lang.Boolean gc? ^java.lang.Long slotCapacity ^java.lang.Long maxCache]
    "Allows garbage collection to be disabled (not recommended)"
    (when (true? gc?)
      (future (.startGC this)))))

;; The size of the header
(defn -getHeaderSize
  ([this]
    "No header is passed in, use the model buffer as reference"
    (.getHeaderSize this modelBuf))
  ([this ^clojurewerkz.buffy.core.BuffyBuf header]
    "A header is passed in, use it"
    (.capacity (.buf header))))

;; Begin garbage collection
(defn -startGC
  ([this]
    "By default check if garbage collection is necessary every minute"
    (.startGC this 60000))
  ([this ^java.lang.Long t]
    "Garbage collect every t milliseconds"
    (go-loop []
      (do
        (.gcAgents this)
        (<! (timeout t))  
      (recur)))))

;;  Write a message to a bucket with a given encoding.
;;
;;    @id Bucket identifier
;;    @msg Message to write
;;    @[Optional]encoding Underlying encoding
(defn -write
  ([this ^java.lang.Long id
         ^bytes msg]
    (.write this id (java.lang.String. (b64/encode msg) "UTF-8") "bin"))
  ([this ^java.lang.Long id 
         ^java.lang.String msg
         ^java.lang.String encoding]
    (let [
        f (io/file (get @(.state this) :filename))                        ;; underlying file
        to-write (subs msg 0 (min (count msg) (.getSlotCapacity this)))   ;; data we can fit
        write-len (count to-write)                                        ;; length of data
        the-rest (subs msg write-len (count msg))                         ;; remaining data
        header-buf (compose-buffer headerSpec)                            ;; buffer that holds header
        header-size (.getHeaderSize this header-buf)                      ;; size of the header (constant?)
        header-pos (* id (+ header-size (.getSlotCapacity this)))         ;; offset in file to write header
        write-pos (+ header-pos header-size)                              ;; offset in file to write data
        signature (getSignature to-write)                                 ;; signature
      ]
 
      ;; Populate the header with the new data
      (set-field header-buf :signature signature)
      (set-field header-buf :length (count to-write))
      (set-field header-buf :encoding (getCodeFromEncoding encoding)) 

      ;; Ensure there is an agent available to handle the file IO
      (.ensureAgent this id)
    
      (when (ensureFileSize f (+ header-pos header-size (.getSlotCapacity this)))
        ;; Re-map the file
        (let [old-mapped-file (.getMappedFile this)]
          (reset! (get @(.state this) :mapped-file) (mmap/get-mmap (.getFilename this) :read-write))
          (.close old-mapped-file)))

      ;; Write the header and the msg
      (send-off (.getAgent this id) writeCubbyAgent f header-buf header-size header-pos to-write write-pos write-len (.getMappedFile this))

      ;; Return the remaining msg that wont fit in this slot
      (if (= (count the-rest) 0)
        nil
        the-rest))))

;; Ennsure that we have an agent
(defn -ensureAgent
  [this ^java.lang.Long id]
  "If the agent doesn't exist, create it atomically"
  (let [
      agent-map @(.getAgents this)
    ]
    (when (nil? (get agent-map id))
      (swap! (.getAgents this) assoc id (agent nil)))))

;; Garbage collect agents
(defn -gcAgents
  [this]
  (let [
      agent-vals (mapKV (fn [[k v]] (deref v)) @(.getAgents this))
      realized-agents (filterVals (fn [v] (not (nil? v))) agent-vals)
      sorted-by-time (sortedMapByValue realized-agents (fn [x] (:lastupdate (:header x))))
      sizes (mapKV (fn [[k v]] (:length (:header v))) sorted-by-time)
      size-sum (mapKV (fn [[k v]] (nth (movingSum (vals sizes)) (.indexOf (keys sizes) k))) sizes)
      index-exceed-max-size (keep-indexed #(when (>= %2 (get @(.state this) :maxCache)) %1) (vals size-sum))
    ]
    (doseq [index index-exceed-max-size]
      (let [
          gc-key (nth (keys size-sum) index)
          ]
        (safePrint "[gc]" gc-key)
        (send-off (.getAgent this  gc-key) (fn [_] nil))))))

(defn -readRawHeader
  [this ^java.lang.Long id]
  "Reads and returns a byte array containing the header for the cubby ID"
  (let [
      read-len (.getHeaderSize this)
      read-pos (* id (+ read-len (.getSlotCapacity this)))
      f (io/file (get @(.state this) :filename))
    ]
    (if (ensureFileSize f (+ read-pos read-len (.getSlotCapacity this)))
      nil
      (mmap/get-bytes (.getMappedFile this) read-pos read-len))))

(defn -readHeader
  [this ^java.lang.Long id]
  "Read a composed header for the cubby Id"
  (let [
      raw (.readRawHeader this id)
    ]
    (if (nil? raw)
      nil
      (compose-buffer headerSpec :orig-buffer raw))))

(defn -read
  [this ^java.lang.Long id 
        ^Boolean add-header?]
  "Read the cubby contents, and optionally include the header"

  (.ensureAgent this id)

  (let [*agent* (.getAgent this id)
        header (.readHeader this id)]
    (if (nil? header)
      nil
      (do
        (while (nil? @*agent*)
          (send-off *agent* readCubbyAgent id header (.getFilename this) (.getHeaderSize this) (.getSlotCapacity this) (.getMappedFile this))
          (await-for 10 *agent*) ;; Wait at most 10 ms for data to populate the agent
          (Thread/sleep 10))
        (let [value @*agent*]
          (if (true? add-header?)
            value
            {:contents (:contents value)}))))))

;; Get the name of the underlying file
(defn -getFilename
  [this]
  "Getter for the collection filename"
  (get @(.state this) :filename))

;; Get the capacity of a slot
(defn -getSlotCapacity
  [this]
  "Getter for the slot capacity"
  (get @(.state this) :slotCapacity))

;; Get the memory mapped file
(defn -getMappedFile
  [this]
  "Getter for the raw mmap"
  @(get @(.state this) :mapped-file))

;; Get the list of agents
(defn -getAgents
  [this]
  (get @(.state this) :agents))

;; Get an agent by id
(defn -getAgent
  [this id]
  (get @(.getAgents this) id))

;; Close the underlying memory mapped file
(defn -close
  [this]
  "Close the mapped file"
  (println "Closing" (.getFilename this))
  (.close (.getMappedFile this)))
