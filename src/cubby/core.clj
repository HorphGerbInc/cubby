(ns cubby.core)

(compile 'cubby.storage.Collection)

;;uncomment below to enable tracing
;;(trace/trace-ns 'cubby.storage.Collection)

(load "accessHelpers")

(defn -main
  [& args]
  (def x (read-cubby "testcol" 0 true))
  (println x)
  (close-collections)
  (shutdown-agents))
