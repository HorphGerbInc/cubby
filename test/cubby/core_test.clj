(ns cubby.core-test
  (:require [clojure.test :refer :all]
            [cubby.core :refer :all]
            [clojure.java.io :as io]
            [clojure.data.codec.base64 :as b64]))

(deftest close
  (testing "Closing collections"
    (close-collections)
    (when (.exists (clojure.java.io/as-file "testcol"))
      (io/delete-file (get-filename "testcol")))))

(deftest single-thread-write-text
  (testing "Single threaded write and check"
    (write-cubby "testcol" 0 "abc" "US-ASCII")
    (Thread/sleep 100) ;; operations are asynchronous.  wait a little bit before testing 
    (is (= (get (read-cubby "testcol" 0) :contents) "abc")))
    (close))

(deftest write-binary
  (testing "Write binary"
    (write-cubby "testcol" 1 (.getBytes "abc"))
    (Thread/sleep 100) ;; operations are asynchronous.  wait a little bit before testing 
    (let [data (read-cubby "testcol" 1 true)]
      (is (= (get (get data :header) :encoding) "bin"))
      (is (= (String. (b64/decode (.getBytes (get data :contents) "UTF-8")) "UTF-8") "abc"))))
    (close))

(deftest many-writes
  (testing "Writing to 1000 slots"
    (def my-string (slurp "resources/ipsum.txt"))
    (doseq [i (range 1000)]
      (write-cubby "testcol" i my-string "UTF-8"))
    (doseq [i (range 1000)]
      (is (= (get (read-cubby "testcol" i) :contents) my-string))))
    (close))
