(ns jepsen.enroute.summary
  "Prints one line per run from the results in the store."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(defn- read-results [f]
  (edn/read-string {:default (fn [_ v] v)} (slurp f)))

(defn summarize [dir]
  (let [r  (read-results (io/file dir "results.edn"))
        by (get-in r [:stats :by-f])
        c  (fn [f] (let [s (by f)] (str (:ok-count s 0) "/" (:fail-count s 0) "/" (:info-count s 0))))]
    (str/join "  "
              [(.getName (.getParentFile (.getCanonicalFile (io/file dir))))
               (str "valid=" (:valid? r))
               (str "elle=" (get-in r [:elle :valid?])
                    (when-let [a (seq (get-in r [:elle :anomaly-types]))] (vec a)))
               (when-let [m (:node-monotonic r)]
                 (str "node-mono=" (:valid? m) (when (seq (:error-types m)) (:error-types m))))
               (str "lost=" (get-in r [:durability :lost-count]))
               (str "phantoms=" (get-in r [:phantoms :count]))
               (str "not-our-ref=" (get-in r [:fetches :not-our-ref]) "/" (get-in r [:fetches :fetches]))
               (str "writes(ok/fail/info)=" (c :write))
               (str "reads=" (c :read))])))

(defn -main [& dirs]
  (doseq [d dirs]
    (println (try (summarize d) (catch Exception e (str d " " (.getMessage e)))))))
