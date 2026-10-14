(ns jepsen.enroute.checker-test
  (:require [clojure.test :refer [deftest is]]
            [jepsen [checker :as checker]
                    [history :as h]]
            [jepsen.enroute [checker :as ec]
                            [git :as git]]))

(defn- history
  "Ops as [process type f value node time], each invoked one tick earlier."
  [& ops]
  (h/history
    (mapcat (fn [[p type f value node t]]
              [{:process p, :type :invoke, :f f, :value value, :node node, :time (dec t)}
               {:process p, :type type, :f f, :value value, :node node, :time t}])
            ops)))

(defn- check [c hist] (checker/check c {} hist {}))

(deftest monotonic-reads-on-one-node
  (is (:valid? (check (ec/node-monotonic)
                      (history [0 :ok :read [[:r 1 [1 2]]] "n4" 10]
                               [1 :ok :read [[:r 1 [1 2 3]]] "n4" 20]))))
  (let [r (check (ec/node-monotonic)
                 (history [0 :ok :read [[:r 1 [1 2]]] "n4" 10]
                          [1 :ok :read [[:r 1 [1]]] "n4" 20]))]
    (is (false? (:valid? r)))
    (is (= {:non-monotonic 1} (:error-types r))))
  ;; Another node may lag.
  (is (:valid? (check (ec/node-monotonic)
                      (history [0 :ok :read [[:r 1 [1 2]]] "n4" 10]
                               [1 :ok :read [[:r 1 [1]]] "n5" 20])))))

(deftest read-your-writes-on-one-node
  (let [r (check (ec/node-monotonic)
                 (history [0 :ok :write [[:r 1 []] [:append 1 7]] "n4" 10]
                          [1 :ok :read [[:r 1 []]] "n4" 20]))]
    (is (= {:read-your-writes 1} (:error-types r)))))

(deftest durability
  (is (false? (:valid? (check (ec/durability)
                              (history [0 :ok :write [[:append 1 7]] "n4" 10]
                                       [1 :ok :final-read [[:r 1 []]] "n4" 20])))))
  (is (true? (:valid? (check (ec/durability)
                             (history [0 :ok :write [[:append 1 7]] "n4" 10]
                                      [1 :ok :final-read [[:r 1 [7]]] "n4" 20]))))))

(deftest partial-pushes-resolve-from-the-final-reads
  (let [h  (h/history
             [{:process 0, :type :invoke, :f :write, :atomic? false, :time 1
               :value [[:append 1 7] [:append 2 8]]}
              {:process 0, :type :info, :f :write, :atomic? false, :time 2
               :value [[:append 1 7] [:append 2 8]]}
              {:process 1, :type :invoke, :f :final-read, :value [], :time 3}
              {:process 1, :type :ok, :f :final-read, :time 4
               :value [[:r 1 [7]] [:r 2 []]]}])
        h' (ec/resolve-partial-pushes h)]
    (is (= [[:append 1 7]] (:value (nth h' 0)) (:value (nth h' 1))))
    (is (= :info (:type (nth h' 1))))))

(deftest packs-round-trip
  (let [body (git/commit-body nil 1 2)
        pack (git/pack [["tree" (byte-array 0)] ["commit" body]])]
    (is (= 2 (git/pack-object-count pack)))
    (is (= git/empty-tree (git/object-id "tree" (byte-array 0))))))
