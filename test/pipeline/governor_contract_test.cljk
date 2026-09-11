(ns pipeline.governor-contract-test
  "The governor contract as executable tests. The single invariant
  under test:

    The PipelineTransport advisor never dispatches a batch or settles a
    delivery the Pipeline Integrity Governor would reject,
    `:batch/dispatch`/`:delivery/settle` NEVER auto-commit at any phase,
    `:batch/intake` (no direct capital risk) MAY auto-commit when clean,
    and every decision (commit OR hold) leaves exactly one ledger fact."
  (:require [clojure.test :refer [deftest is testing]]
            [langgraph.graph :as g]
            [pipeline.store :as store]
            [pipeline.operation :as op]))

(defn- fresh []
  (let [db (store/seed-db)]
    [db (op/build db)]))

(def operator {:actor-id "op-1" :actor-role :pipeline-controller :phase 3})

(defn- exec-op [actor tid request context]
  (g/run* actor {:request request :context context} {:thread-id tid}))

(defn- approve! [actor tid]
  (g/run* actor {:approval {:status :approved :by "op-1"}} {:thread-id tid :resume? true}))

(defn- verify!
  "Walks `subject` through segment verify -> approve, leaving an
  assessment on file. Uses distinct thread-ids per call site by
  suffixing `tid-prefix`."
  [actor tid-prefix subject]
  (exec-op actor (str tid-prefix "-verify") {:op :segment/verify :subject subject} operator)
  (approve! actor (str tid-prefix "-verify")))

(deftest clean-intake-auto-commits
  (let [[db actor] (fresh)
        res (exec-op actor "t1"
                  {:op :batch/intake :subject "batch-1"
                   :patch {:id "batch-1" :product-grade "JIS-K-2202-Diesel-CR"}} operator)]
    (is (= :commit (get-in res [:state :disposition])))
    (is (= "JIS-K-2202-Diesel-CR" (:product-grade (store/pipeline-batch db "batch-1"))) "SSoT actually updated")
    (is (= 1 (count (store/ledger db))))))

(deftest segment-verify-always-needs-approval
  (testing "segment verify is never in any phase's :auto set -- always human approval, even when clean"
    (let [[db actor] (fresh)
          res (exec-op actor "t2" {:op :segment/verify :subject "batch-1"} operator)]
      (is (= :interrupted (:status res)))
      (let [r2 (approve! actor "t2")]
        (is (= :commit (get-in r2 [:state :disposition])))
        (is (some? (store/segment-assessment-of db "batch-1")))))))

(deftest fabricated-jurisdiction-is-held
  (testing "a segment/verify proposal with no official spec-basis -> HOLD, never reaches a human"
    (let [[db actor] (fresh)
          res (exec-op actor "t3"
                    {:op :segment/verify :subject "batch-2"} operator)]
      (is (= :hold (get-in res [:state :disposition])))
      (is (some #{:no-spec-basis} (-> (store/ledger db) first :basis)))
      (is (nil? (store/segment-assessment-of db "batch-2")) "no assessment written"))))

(deftest batch-dispatch-without-assessment-is-held
  (testing "batch/dispatch before any segment assessment -> HOLD (evidence incomplete)"
    (let [[db actor] (fresh)
          res (exec-op actor "t4" {:op :batch/dispatch :subject "batch-1"} operator)]
      (is (= :hold (get-in res [:state :disposition])))
      (is (some #{:evidence-incomplete} (-> (store/ledger db) first :basis))))))

(deftest line-pressure-out-of-range-is-held-and-unoverridable
  (testing "a measured line pressure outside the safe window -> HOLD, and never reaches request-approval -- a genuinely new sub-category (the aerospace two-sided-tolerance discipline applied to a pressurized pipeline)"
    (let [[db actor] (fresh)
          _ (verify! actor "t5pre" "batch-3")
          res (exec-op actor "t5" {:op :batch/dispatch :subject "batch-3"} operator)]
      (is (= :hold (get-in res [:state :disposition])) "settles immediately, no interrupt")
      (is (not= :interrupted (:status res)))
      (is (some #{:line-pressure-out-of-range} (-> (store/ledger db) last :basis)))
      (is (empty? (store/dispatch-history db))))))

(deftest pod-chain-integrity-broken-is-held-and-unoverridable
  (testing "an unconfirmed prior-segment POD chain -> HOLD, and never reaches request-approval -- the freight 4920 POD-chain discipline applied to pipeline custody"
    (let [[db actor] (fresh)
          _ (verify! actor "t6pre" "batch-4")
          res (exec-op actor "t6" {:op :batch/dispatch :subject "batch-4"} operator)]
      (is (= :hold (get-in res [:state :disposition])) "settles immediately, no interrupt")
      (is (not= :interrupted (:status res)))
      (is (some #{:pod-chain-integrity-broken} (-> (store/ledger db) last :basis)))
      (is (empty? (store/dispatch-history db))))))

(deftest product-contamination-flag-unresolved-is-held-and-unoverridable
  (testing "an unresolved product contamination flag -> HOLD, and never reaches request-approval (interface mixing / grade contamination)"
    (let [[db actor] (fresh)
          _ (verify! actor "t7pre" "batch-5")
          res (exec-op actor "t7" {:op :batch/dispatch :subject "batch-5"} operator)]
      (is (= :hold (get-in res [:state :disposition])) "settles immediately, no interrupt")
      (is (not= :interrupted (:status res)))
      (is (some #{:product-contamination-flag-unresolved} (-> (store/ledger db) last :basis)))
      (is (empty? (store/dispatch-history db))))))

(deftest integrity-assessment-stale-is-held-and-unoverridable
  (testing "a stale integrity assessment (ILI/pigging/hydrotest interval expired) -> HOLD, and never reaches request-approval (the PHMSA recurring ILI duty)"
    (let [[db actor] (fresh)
          _ (verify! actor "t8pre" "batch-6")
          res (exec-op actor "t8" {:op :batch/dispatch :subject "batch-6"} operator)]
      (is (= :hold (get-in res [:state :disposition])) "settles immediately, no interrupt")
      (is (not= :interrupted (:status res)))
      (is (some #{:integrity-assessment-stale} (-> (store/ledger db) last :basis)))
      (is (empty? (store/dispatch-history db))))))

(deftest bonding-grounding-unconfirmed-is-held-and-unoverridable
  (testing "unconfirmed bonding/grounding -> HOLD, and never reaches request-approval (static-electricity ignition during flow)"
    (let [[db actor] (fresh)
          _ (verify! actor "t9pre" "batch-7")
          res (exec-op actor "t9" {:op :batch/dispatch :subject "batch-7"} operator)]
      (is (= :hold (get-in res [:state :disposition])) "settles immediately, no interrupt")
      (is (not= :interrupted (:status res)))
      (is (some #{:bonding-grounding-unconfirmed} (-> (store/ledger db) last :basis)))
      (is (empty? (store/dispatch-history db))))))

(deftest batch-dispatch-always-escalates-then-human-decides
  (testing "a clean, fully-assessed, in-window, POD-confirmed, contamination-clear, integrity-current, bonding-grounding-ok batch still ALWAYS interrupts for human approval -- :batch/dispatch is never auto"
    (let [[db actor] (fresh)
          _ (verify! actor "t10pre" "batch-1")
          r1 (exec-op actor "t10" {:op :batch/dispatch :subject "batch-1"} operator)]
      (is (= :interrupted (:status r1)) "pauses for human approval even when governor-clean")
      (testing "approve -> commit, dispatch record drafted"
        (let [r2 (approve! actor "t10")]
          (is (= :commit (get-in r2 [:state :disposition])))
          (is (true? (:dispatched? (store/pipeline-batch db "batch-1"))))
          (is (= 1 (count (store/dispatch-history db))) "one draft dispatch record"))))))

(deftest delivery-settle-always-escalates-then-human-decides
  (testing "a clean, fully-assessed, already-dispatched batch still ALWAYS interrupts for human approval -- :delivery/settle is never auto"
    (let [[db actor] (fresh)
          _ (verify! actor "t11pre" "batch-1")
          _ (exec-op actor "t11dispatch" {:op :batch/dispatch :subject "batch-1"} operator)
          _ (approve! actor "t11dispatch")
          r1 (exec-op actor "t11" {:op :delivery/settle :subject "batch-1"} operator)]
      (is (= :interrupted (:status r1)) "pauses for human approval even when governor-clean")
      (testing "approve -> commit, delivery record drafted"
        (let [r2 (approve! actor "t11")]
          (is (= :commit (get-in r2 [:state :disposition])))
          (is (true? (:delivered? (store/pipeline-batch db "batch-1"))))
          (is (= 1 (count (store/delivery-history db))) "one draft delivery record"))))))

(deftest batch-dispatch-double-dispatch-is-held
  (testing "dispatching the same batch twice -> HOLD on the second attempt"
    (let [[db actor] (fresh)
          _ (verify! actor "t12pre" "batch-1")
          _ (exec-op actor "t12a" {:op :batch/dispatch :subject "batch-1"} operator)
          _ (approve! actor "t12a")
          res (exec-op actor "t12" {:op :batch/dispatch :subject "batch-1"} operator)]
      (is (= :hold (get-in res [:state :disposition])))
      (is (some #{:already-dispatched} (-> (store/ledger db) last :basis)))
      (is (= 1 (count (store/dispatch-history db))) "still only the one earlier dispatch"))))

(deftest delivery-settle-double-delivery-is-held
  (testing "settling the same batch's delivery twice -> HOLD on the second attempt"
    (let [[db actor] (fresh)
          _ (verify! actor "t13pre" "batch-1")
          _ (exec-op actor "t13dispatch" {:op :batch/dispatch :subject "batch-1"} operator)
          _ (approve! actor "t13dispatch")
          _ (exec-op actor "t13a" {:op :delivery/settle :subject "batch-1"} operator)
          _ (approve! actor "t13a")
          res (exec-op actor "t13" {:op :delivery/settle :subject "batch-1"} operator)]
      (is (= :hold (get-in res [:state :disposition])))
      (is (some #{:already-delivered} (-> (store/ledger db) last :basis)))
      (is (= 1 (count (store/delivery-history db))) "still only the one earlier delivery"))))

(deftest every-decision-leaves-one-ledger-fact
  (testing "write-only-through-ledger: N operations -> N ledger facts"
    (let [[db actor] (fresh)]
      (exec-op actor "a" {:op :batch/intake :subject "batch-1"
                          :patch {:id "batch-1" :product-grade "JIS-K-2202-Diesel-CR"}} operator)
      (exec-op actor "b" {:op :segment/verify :subject "batch-2"} operator)
      (is (= 2 (count (store/ledger db)))
          "one commit + one hold, both recorded"))))
