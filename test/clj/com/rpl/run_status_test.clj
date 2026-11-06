(ns com.rpl.run-status-test
  (:use [clojure.test]
        [com.rpl.test-helpers]
        [com.rpl.rama]
        [com.rpl.rama.path])
  (:require
   [com.rpl.agent-o-rama :as aor]
   [com.rpl.agent-o-rama.impl.agent-node :as anode]
   [com.rpl.agent-o-rama.impl.analytics :as ana]
   [com.rpl.agent-o-rama.impl.helpers :as h]
   [com.rpl.agent-o-rama.impl.metrics :as metrics]
   [com.rpl.agent-o-rama.impl.queries :as queries]
   [com.rpl.agent-o-rama.impl.retries :as retries]
   [com.rpl.agent-o-rama.impl.types :as aor-types]
   [com.rpl.agent-o-rama.impl.ui.rpc.invocations :as rpc]
   [com.rpl.rama.aggs :as aggs]
   [com.rpl.rama.test :as rtest]
   [com.rpl.test-common :as tc])
  (:import
   [com.rpl.agentorama
    AgentInvoke]))

(def RELEASE)
(def INVOKES)

(defn- root-status
  [client ^AgentInvoke inv]
  (foreign-select-one [(keypath (.getAgentInvokeId inv)) :status]
                      (:root-pstate (aor-types/underlying-objects client))
                      {:pkey (.getTaskId inv)}))

(defn- trace-nodes
  [client ^AgentInvoke inv]
  (let [{:keys [root-pstate tracing-query]} (aor-types/underlying-objects client)
        task-id (.getTaskId inv)
        root-id (foreign-select-one [(keypath (.getAgentInvokeId inv)) :root-invoke-id]
                                    root-pstate
                                    {:pkey task-id})]
    (:invokes-map (foreign-invoke-query tracing-query
                                        task-id
                                        [[task-id root-id]]
                                        10000))))

(defn- node-statuses
  "Node name to the statuses of its invokes, ignoring aggregation's stub records"
  [client inv]
  (->> (vals (trace-nodes client inv))
       (filter :node)
       (group-by :node)
       (transform MAP-VALS #(mapv :status %))))

(defn- graph-page
  [client ^AgentInvoke inv]
  (rpc/get-graph-page!!
   {:aor-cache {"m" {:clients {"foo" client}}}}
   {:module-id  "m"
    :agent-name "foo"
    :invoke-id  (str (.getTaskId inv) "-" (.getAgentInvokeId inv))}))

(defmacro ^:private with-agent
  "Launches an agent \"foo\" of the given node definitions and binds client"
  [[client ipc] agent-form & body]
  `(with-open [~ipc (rtest/create-ipc)]
     (let [module#
           (aor/agentmodule
            [~'topology]
            (-> ~'topology
                (aor/new-agent "foo")
                ~agent-form))]
       (launch-module-without-eval-agent! ~ipc module# {:tasks 2 :threads 2})
       (let [~client (aor/agent-client (aor/agent-manager ~ipc (get-module-name module#))
                                       "foo")]
         ~@body))))

(deftest status-of-successful-run-test
  (with-redefs [RELEASE (tc/mk-cf)]
    (with-agent [foo ipc]
      (->
       (aor/node "start"
                 "work"
                 (fn [agent-node]
                   (aor/emit! agent-node "work")))
       (aor/node "work"
                 nil
                 (fn [agent-node]
                   (.get ^java.util.concurrent.CompletableFuture RELEASE)
                   (aor/result! agent-node :done))))
      (let [inv (aor/agent-initiate foo)]
        (is (= :pending (root-status foo inv)))
        (is (condition-attained?
             (= {"start" [:completed] "work" [:running]} (node-statuses foo inv))))
        (is (= :pending (root-status foo inv)))
        (tc/complete-cf! RELEASE true)
        (is (= :done (aor/agent-result foo inv)))
        (is (= :success (root-status foo inv)))
        (is (condition-attained?
             (= {"start" [:completed] "work" [:completed]} (node-statuses foo inv))))))))

(deftest status-through-retry-test
  (with-redefs [RELEASE (tc/mk-cf)
                INVOKES (atom 0)
                anode/log-node-error (fn [& _])
                aor-types/get-config (max-retries-override 3)]
    (with-agent [foo ipc]
      (aor/node "work"
                nil
                (fn [agent-node]
                  (when (= 1 (swap! INVOKES inc))
                    (throw (ex-info "first attempt fails" {})))
                  (.get ^java.util.concurrent.CompletableFuture RELEASE)
                  (aor/result! agent-node :done)))
      (let [inv (aor/agent-initiate foo)]
        ;; the re-run under the same invoke id is running again
        (is (condition-attained? (= 2 @INVOKES)))
        (is (condition-attained? (= {"work" [:running]} (node-statuses foo inv))))
        (is (= :pending (root-status foo inv)))
        (tc/complete-cf! RELEASE true)
        (is (= :done (aor/agent-result foo inv)))
        (is (= :success (root-status foo inv)))
        (is (condition-attained? (= {"work" [:completed]} (node-statuses foo inv))))))))

(deftest status-of-failed-run-test
  (with-redefs [anode/log-node-error (fn [& _])
                aor-types/get-config (max-retries-override 1)]
    (with-agent [foo ipc]
      (aor/node "work"
                nil
                (fn [_agent-node]
                  (throw (ex-info "always fails" {}))))
      (let [inv (aor/agent-initiate foo)]
        (is (thrown? Exception (aor/agent-result foo inv)))
        (is (= :failure (root-status foo inv)))
        (is (= {"work" [:failed]} (node-statuses foo inv)))))))

(deftest status-of-run-without-result-test
  (with-agent [foo ipc]
    (aor/node "work" nil (fn [_agent-node]))
    (let [inv (aor/agent-initiate foo)]
      (is (thrown? Exception (aor/agent-result foo inv)))
      (is (= :failure (root-status foo inv)))
      (is (condition-attained? (= {"work" [:completed]} (node-statuses foo inv)))))))

(deftest status-of-aggregation-test
  (with-agent [foo ipc]
    (->
     (aor/agg-start-node "begin"
                         "item"
                         (fn [agent-node]
                           (aor/emit! agent-node "item" 1)
                           (aor/emit! agent-node "item" 2)))
     (aor/node "item"
               "sum"
               (fn [agent-node n]
                 (aor/emit! agent-node "sum" n)))
     (aor/agg-node "sum"
                   nil
                   aggs/+sum
                   (fn [agent-node agg _start-res]
                     (aor/result! agent-node agg))))
    (let [inv (aor/agent-initiate foo)]
      (is (= 3 (aor/agent-result foo inv)))
      (is (condition-attained?
           (= {"begin" [:completed]
               "item"  [:completed :completed]
               "sum"   [:completed]}
              (node-statuses foo inv)))))))

(def UPDATES)
(def INITS)

(def +fails-first-update
  (accumulator
   (fn [n]
     (when (= 1 (swap! UPDATES inc))
       (throw (ex-info "bad update" {})))
     (term #(+ % n)))
   :init-fn
   (fn [] 0)))

(def +always-fails-update
  (accumulator
   (fn [_n]
     (throw (ex-info "bad update" {})))
   :init-fn
   (fn [] 0)))

(def +always-fails-init
  (accumulator
   (fn [n]
     (term #(+ % n)))
   :init-fn
   (fn []
     (throw (ex-info "bad init" {})))))

(def +fails-first-update-then-init
  (accumulator
   (fn [_n]
     (throw (ex-info "bad update" {})))
   :init-fn
   (fn []
     (when (= 2 (swap! INITS inc))
       (throw (ex-info "bad init" {})))
     0)))

(defn- agg-agent
  "begin -> item -> sum (aggregating with agg)"
  [topology agg]
  (->
   topology
   (aor/agg-start-node "begin"
                       "item"
                       (fn [agent-node]
                         (aor/emit! agent-node "item" 1)))
   (aor/node "item"
             "sum"
             (fn [agent-node n]
               (aor/emit! agent-node "sum" n)))
   (aor/agg-node "sum"
                 nil
                 agg
                 (fn [agent-node v _start-res]
                   (aor/result! agent-node v)))))

(defn- agg-record
  [client inv]
  (select-first [MAP-VALS (selected? :node (pred= "sum"))]
                (trace-nodes client inv)))

(deftest status-of-failed-aggregation-update-test
  (testing "a failing update fails the aggregation node"
    (with-redefs [anode/log-node-error (fn [& _])
                  aor-types/get-config (max-retries-override 1)]
      (with-agent [foo ipc]
        (agg-agent +always-fails-update)
        (let [inv (aor/agent-initiate foo)]
          (is (thrown? Exception (aor/agent-result foo inv)))
          (is (= :failure (root-status foo inv)))
          (let [{:keys [status exceptions]} (agg-record foo inv)]
            (is (= :failed status))
            (is (some #(re-find #"bad update" %) exceptions)))
          (let [page (graph-page foo inv)]
            (is (true? (:is-complete page)))
            (is (= :failed (:status (select-one! [:nodes MAP-VALS (selected? :node (pred= "sum"))]
                                                 page))))))))))

(deftest status-of-aggregation-retried-after-failure-test
  (with-redefs [UPDATES (atom 0)
                anode/log-node-error (fn [& _])
                aor-types/get-config (max-retries-override 3)]
    (with-agent [foo ipc]
      (agg-agent +fails-first-update)
      (let [inv (aor/agent-initiate foo)]
        (is (= 1 (aor/agent-result foo inv)))
        (is (= :success (root-status foo inv)))
        (is (condition-attained? (= :completed (:status (agg-record foo inv)))))
        ;; the attempt's failure stays visible on the run
        (is (= 1 (count (foreign-select-one [(keypath (.getAgentInvokeId ^AgentInvoke inv))
                                             :exception-summaries]
                                            (:root-pstate (aor-types/underlying-objects foo))
                                            {:pkey (.getTaskId ^AgentInvoke inv)}))))))))

(deftest status-of-failed-aggregation-init-test
  (testing "a new aggregation that fails to initialize leaves no node record"
    (with-redefs [anode/log-node-error (fn [& _])
                  aor-types/get-config (max-retries-override 1)]
      (with-agent [foo ipc]
        (agg-agent +always-fails-init)
        (let [inv (aor/agent-initiate foo)]
          (is (thrown? Exception (aor/agent-result foo inv)))
          (is (= :failure (root-status foo inv)))
          (is (nil? (agg-record foo inv)))
          (is (true? (:is-complete (graph-page foo inv))))))))
  (testing "reinitializing an aggregation on retry fails its existing record"
    (with-redefs [INITS (atom 0)
                  anode/log-node-error (fn [& _])
                  aor-types/get-config (max-retries-override 1)]
      (with-agent [foo ipc]
        (agg-agent +fails-first-update-then-init)
        (let [inv (aor/agent-initiate foo)]
          (is (thrown? Exception (aor/agent-result foo inv)))
          (is (= :failure (root-status foo inv)))
          (let [{:keys [status exceptions]} (agg-record foo inv)]
            (is (= :failed status))
            (is (some #(re-find #"bad init" %) exceptions))))))))

(deftest status-of-fork-test
  (with-agent [foo ipc]
    (->
     (aor/node "start"
               "add"
               (fn [agent-node n]
                 (aor/emit! agent-node "add" n)))
     (aor/agg-start-node "add"
                         "item"
                         (fn [agent-node n]
                           (aor/emit! agent-node "item" n)))
     (aor/node "item"
               "sum"
               (fn [agent-node n]
                 (aor/emit! agent-node "sum" n)))
     (aor/agg-node "sum"
                   "end"
                   aggs/+sum
                   (fn [agent-node agg _start-res]
                     (aor/emit! agent-node "end" agg)))
     (aor/node "end"
               nil
               (fn [agent-node v]
                 (aor/result! agent-node v))))
    (let [inv      (aor/agent-initiate foo 1)
          _        (is (= 1 (aor/agent-result foo inv)))
          _        (is (condition-attained?
                        (= :completed
                           (get-in (node-statuses foo inv) ["end" 0]))))
          end-id   (select-one! [ALL (selected? LAST :node (pred= "end")) FIRST]
                                (trace-nodes foo inv))
          finv     (aor/agent-initiate-fork foo inv {end-id [5]})]
      (is (= 5 (aor/agent-result foo finv)))
      (is (= :success (root-status foo finv)))
      ;; copied nodes carry their status over, including aggregation state
      (is (condition-attained?
           (= {"start" [:completed]
               "add"   [:completed]
               "item"  [:completed]
               "sum"   [:completed]
               "end"   [:completed]}
              (node-statuses foo finv)))))))

(deftest run-status-helper-test
  (testing "explicit status wins"
    (is (= :cancelled (h/run-status {:status :cancelled
                                     :result (aor-types/->AgentResult "r" true)}))))
  (testing "a root from before :status reads as it did"
    (is (= :pending (h/run-status {})))
    (is (= :failure (h/run-status {:result (aor-types/->AgentResult "r" true)})))
    (is (= :success (h/run-status {:result (aor-types/->AgentResult "r" false)})))))

(deftest node-status-helper-test
  (testing "explicit status wins"
    (is (= :cancelled (h/node-status {:status :cancelled :start-time-millis 1}))))
  (testing "a node from before :status reads as it did"
    (is (= :running (h/node-status {:start-time-millis 1})))
    (is (= :completed (h/node-status {:start-time-millis 1 :finish-time-millis 2})))
    (is (= :failed (h/node-status {:start-time-millis 1 :exceptions ["boom"]})))
    (testing "a retry that completed a node that failed before"
      (is (= :completed (h/node-status {:start-time-millis 1
                                        :finish-time-millis 2
                                        :exceptions ["boom"]}))))))

(deftest readers-use-status-test
  (testing "invoke-status"
    (is (= :cancelled (queries/invoke-status {:status :cancelled
                                              :result (aor-types/->AgentResult "r" true)})))
    (is (= :failure (queries/invoke-status {:result (aor-types/->AgentResult "r" true)}))))
  (testing "run-success? on a root"
    (let [root (fn [m] (assoc m :run-type :agent))]
      (is (metrics/run-success? (root {:status :success})))
      (is (not (metrics/run-success? (root {:status :failure}))))
      (is (not (metrics/run-success? (root {:status :cancelled}))))
      (is (metrics/run-success? (root {:result (aor-types/->AgentResult "r" false)})))
      (is (not (metrics/run-success? (root {:result (aor-types/->AgentResult "r" true)}))))))
  (testing "run-success? on a node"
    (let [node (fn [m] (assoc m :run-type :node))]
      (is (metrics/run-success? (node {:status :completed})))
      (is (not (metrics/run-success? (node {:status :failed}))))
      (is (not (metrics/run-success? (node {:status :running}))))
      (is (metrics/run-success? (node {:finish-time-millis 1})))
      (is (not (metrics/run-success? (node {})))))))

(def ^:private id1 #uuid "00000000-0000-0000-0000-000000000001")
(def ^:private id2 #uuid "00000000-0000-0000-0000-000000000002")

(deftest complete-node-map-test
  (let [now     1000000
        started (- now 1000)]
    (with-redefs [ana/node-stall-time      (constantly (- now 5000))
                  ana/max-node-scan-time   (constantly now)
                  retries/invoke-id-executing? (constantly false)]
      (testing "the scan waits for a failed node, whose retry may still complete it"
        (let [m (sorted-map id1 {:start-time-millis started :finish-time-millis now}
                            id2 {:start-time-millis started
                                 :status            :failed
                                 :exceptions        ["boom"]})]
          (is (= [id1] (keys (ana/complete-node-map m (ana/->AnaAllNodesTarget) nil))))))
      (testing "the scan waits for a running node"
        (let [m (sorted-map id1 {:start-time-millis started
                                 :finish-time-millis now
                                 :status            :completed}
                            id2 {:start-time-millis started :status :running})]
          (is (= [id1] (keys (ana/complete-node-map m (ana/->AnaAllNodesTarget) nil))))))
      (testing "the scan passes a cancelled node, which won't complete"
        (let [m (sorted-map id1 {:start-time-millis started :status :cancelled})]
          (is (= [id1] (keys (ana/complete-node-map m (ana/->AnaAllNodesTarget) nil))))))
      (testing "the scan passes a completed node"
        (let [m (sorted-map id1 {:start-time-millis  started
                                 :finish-time-millis now
                                 :status             :completed})]
          (is (= [id1] (keys (ana/complete-node-map m (ana/->AnaAllNodesTarget) nil)))))))))

(deftest page-complete-test
  (let [nodes (fn [status2]
                {id1 {:node "a" :status :completed :emits [{:invoke-id id2}]}
                 id2 {:node "b" :status status2 :emits []}})]
    (testing "a pending run is not complete"
      (is (not (rpc/invocation-complete? {:status :pending} (nodes :completed) id1))))
    (testing "a successful run waits for its other branches"
      (is (rpc/invocation-complete? {:status :success} (nodes :completed) id1))
      (is (not (rpc/invocation-complete? {:status :success} (nodes :running) id1))))
    (testing "a failed node is terminal"
      (is (rpc/invocation-complete? {:status :success} (nodes :failed) id1)))
    (testing "nothing more happens in a failed or cancelled run"
      (is (rpc/invocation-complete? {:status :failure} (nodes :running) id1))
      (is (rpc/invocation-complete? {:status :cancelled} (nodes :running) id1)))
    (testing "a successful run without its trace isn't complete yet"
      (is (not (rpc/invocation-complete? {:status :success} nil id1))))
    (testing "data from before :status"
      (let [result (fn [failure?] {:result (aor-types/->AgentResult "r" failure?)})
            old    (fn [node2]
                     {id1 {:node "a" :finish-time-millis 1 :emits [{:invoke-id id2}]}
                      id2 (assoc node2 :node "b" :emits [])})]
        (is (rpc/invocation-complete? (result false) (old {:finish-time-millis 1}) id1))
        (is (not (rpc/invocation-complete? (result false) (old {:start-time-millis 1}) id1)))
        (is (rpc/invocation-complete? (result true) (old {:start-time-millis 1}) id1))
        (is (not (rpc/invocation-complete? {} (old {:finish-time-millis 1}) id1)))))))

(deftest failed-run-page-test
  (with-redefs [anode/log-node-error (fn [& _])
                aor-types/get-config (max-retries-override 1)]
    (with-agent [foo ipc]
      (aor/node "work"
                nil
                (fn [_agent-node]
                  (throw (ex-info "always fails" {}))))
      (let [inv (aor/agent-initiate foo)]
        (is (thrown? Exception (aor/agent-result foo inv)))
        (let [page (graph-page foo inv)]
          (is (true? (:is-complete page)))
          (is (= :failure (get-in page [:summary :status])))
          (is (= [:failed] (mapv :status (vals (:nodes page))))))))))
