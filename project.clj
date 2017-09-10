(defproject cubby "0.1.0-SNAPSHOT"
  :description "Storage engine that supports concurrent reads and writes with MVCC"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"],[org.clojure/core.async "0.3.443"],[org.clojure/data.codec "0.1.0"],[clj-mmap "1.1.2"],[clojurewerkz/buffy "1.0.2"],[digest "1.4.5"],[debugger "0.2.0"],[clj-time "0.14.0"],[org.clojure/tools.trace "0.7.9"]]
  :main ^:skip-aot cubby.core
  :aot [cubby.storage.Collection]
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
