(defproject jepsen.enroute "0.1.0-SNAPSHOT"
  :description "Jepsen tests for Enroute git push and ref reads"
  :license {:name "Apache-2.0"}
  :main jepsen.enroute
  :dependencies [[org.clojure/clojure "1.12.6"]
                 [jepsen "0.3.14"]]
  :jvm-opts ["-Xmx6g" "-server" "-Djava.awt.headless=true"]
  :repl-options {:init-ns jepsen.enroute})
