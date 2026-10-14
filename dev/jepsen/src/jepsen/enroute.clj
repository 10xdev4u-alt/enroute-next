(ns jepsen.enroute
  "Jepsen tests for Enroute: git pushes and ref reads over smart HTTP,
  checked with Elle list-append."
  (:require [clojure.string :as str]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [generator :as gen]
                    [history :as h]
                    [net :as net]
                    [os :as os]
                    [tests :as tests]]
            [jepsen.tests.cycle.append :as append]
            [jepsen.enroute [checker :as ec]
                            [client :as client]
                            [cluster :as cluster]
                            [nemesis :as nemesis]]))

(def workloads
  "What each workload pushes, how it reads, and the consistency models its
  reads promise."
  {:w1    {:push :atomic, :read-mode :ls-refs, :pin? true,
           :models [:strict-serializable :strong-session-serializable]}
   :w1-v0 {:push :atomic, :read-mode :v0, :pin? true,
           :models [:strict-serializable :strong-session-serializable]}
   :w2    {:push :non-atomic, :read-mode :ls-refs, :pin? true,
           :models [:strict-serializable :strong-session-serializable]}
   :w3    {:push :mixed, :read-mode :rp, :pin? true,
           :models [:strict-serializable]}
   :w4    {:push :atomic, :read-mode :ls-refs, :pin? false, :fetch? true,
           :models [:strict-serializable]}})

(defn- git-fetch-op [max-repo] {:f :git-fetch, :value @max-repo})

(defn txn-gen
  "Reads of every key of the current repository, and pushes of 1 to 4 of
  its keys. Moves to a new repository after `per-key` appends per key, so
  that lists stay short."
  [{:keys [key-count per-key max-repo push]}]
  (let [value (atom 0), appends (atom 0)]
    (fn []
      (when (<= (* key-count per-key) @appends)
        (reset! appends 0)
        (swap! max-repo inc))
      (let [keys (client/repo-keys {:key-count key-count} @max-repo)]
        (cond
          ;; A real git client now and then, for fetch completeness.
          (< (rand) 0.01) (git-fetch-op max-repo)

          (< (rand) 0.5)
          {:f :read, :value (mapv (fn [k] [:r k nil]) keys)}
          :else
          (let [ks (take (inc (rand-int 4)) (shuffle keys))]
            (swap! appends + (count ks))
            {:f :write
             :atomic? (case push :atomic true, :non-atomic false, :mixed (< (rand) 0.5))
             :value (mapv (fn [k] [:append k (swap! value inc)]) ks)}))))))

(defn elle
  "Elle on the transactions only, with the outcome of timed-out pushes that
  are not atomic resolved."
  [models]
  (let [c (append/checker {:consistency-models models})]
    (reify checker/Checker
      (check [_ test history opts]
        (checker/check c test
                       (->> history
                            ec/resolve-partial-pushes
                            (h/filter-f #{:read :write :final-read}))
                       opts)))))

(defn enroute-test
  [opts]
  (let [w        (workloads (:workload opts))
        topo     (cluster/topology (:topology opts))
        faults   (:nemesis opts)
        max-repo (atom 0)
        opts     (merge opts w {:max-repo max-repo})
        git-gen  #(git-fetch-op max-repo)]
    (merge tests/noop-test
           opts
           {:name      (str "enroute " (name (:workload opts)) " "
                            (name (:topology opts)) " "
                            (if (seq faults) (str/join "," (map name faults)) "none"))
            :nodes     (:nodes topo)
            :topology  topo
            :os        os/noop
            :net       net/noop
            :ssh       {:dummy? true}
            :db        (cluster/db)
            :client    (client/client)
            :nemesis   (nemesis/nemesis)
            :generator (gen/phases
                         (->> (txn-gen opts)
                              (gen/stagger (/ (:rate opts)))
                              (gen/nemesis (nemesis/generator faults (:interval opts)))
                              (gen/time-limit (:time-limit opts)))
                         (gen/nemesis (nemesis/final-generator))
                         (gen/log "Healed. Quiet period.")
                         (gen/sleep (:quiet opts))
                         (gen/clients (gen/each-thread {:f :final-read, :value []}))
                         (gen/clients (gen/each-thread (gen/once git-gen))))
            :checker   (checker/compose
                         (cond-> {:elle        (elle (:models w))
                                  :durability  (ec/durability)
                                  :phantoms    (ec/phantoms)
                                  :fetches     (ec/fetches)
                                  :stats       (checker/stats)
                                  :exceptions  (checker/unhandled-exceptions)}
                           (and (:pin? w) (not= :rp (:read-mode w)))
                           (assoc :node-monotonic (ec/node-monotonic))))})))

(def fault-names (set (map name (keys nemesis/faults))))

(def cli-opts
  [["-w" "--workload NAME" "w1, w1-v0, w2, w3, or w4"
    :default :w1, :parse-fn keyword, :validate [workloads "unknown workload"]]
   [nil "--topology NAME" "split (3 ledger and 2 compute nodes) or both (3 nodes)"
    :default :split, :parse-fn keyword, :validate [#{:split :both} "split or both"]]
   [nil "--nemesis FAULTS" (str "Comma-separated faults: " (str/join ", " (sort fault-names)))
    :default [], :parse-fn #(mapv keyword (remove str/blank? (str/split % #",")))
    :validate [#(every? (comp fault-names name) %) "unknown fault"]]
   [nil "--interval SECONDS" "Seconds between fault changes"
    :default 10, :parse-fn parse-double]
   [nil "--quiet SECONDS" "Seconds of quiet before the final reads"
    :default 30, :parse-fn parse-double]
   [nil "--rate HZ" "Client ops per second"
    :default 100, :parse-fn parse-double]
   [nil "--key-count N" "Keys (branches) per repository"
    :default 8, :parse-fn parse-long]
   [nil "--per-key N" "Appends per key before a new repository"
    :default 200, :parse-fn parse-long]
   [nil "--binary PATH" "The enroute binary"
    :default "../../target/release/enroute"]
   [nil "--dir PATH" "Parent of the per-test directories for node data and the bucket"
    :default "/var/tmp/enroute-jepsen"]
   [nil "--inline-layer-kb KB" "layers.inline_layer, in KiB. 0 makes every push upload its layer first"
    :parse-fn parse-long]
   [nil "--lazyfs PATH" "Put ledger data directories on LazyFS (this binary). A ledger kill then loses unsynced writes"]
   [nil "--rust-log FILTER" "RUST_LOG for the nodes" :default "warn"]])

(defn -main [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn enroute-test, :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
