(ns pipeline.store-contract-test
  "The Store contract, run against BOTH backends. Proving MemStore and
  the Datomic-backed (langchain.db) store satisfy the same contract is
  what makes 'swap the SSoT for Datomic / kotoba-server' a
  configuration change, not a rewrite -- see `cloud-itonami-isic-6511`'s
  `underwriting.store-contract-test` for the same pattern on the
  sibling actor."
  (:require [clojure.test :refer [deftest is testing]]
            [pipeline.store :as store]))

(defn- backends []
  [["MemStore" (store/seed-db)] ["DatomicStore" (store/datomic-seed-db)]])

(deftest read-parity
  (doseq [[label s] (backends)]
    (testing label
      (is (= "JPN" (:jurisdiction (store/pipeline-batch s "batch-1"))))
      (is (= "JIS-K-2202-Diesel-CR" (:product-grade (store/pipeline-batch s "batch-1"))))
      (is (= 50000 (:volume-barrels (store/pipeline-batch s "batch-1"))))
      (is (= "ATL" (:jurisdiction (store/pipeline-batch s "batch-2"))))
      (is (= 12.0 (:line-pressure-mpa-actual (store/pipeline-batch s "batch-3"))) "batch-3 line pressure out of range")
      (is (false? (:prior-segment-pod-confirmed? (store/pipeline-batch s "batch-4"))) "batch-4 POD chain broken")
      (is (true? (:contamination-flag-raised? (store/pipeline-batch s "batch-5"))) "batch-5 contamination flag raised")
      (is (false? (:contamination-flag-resolved? (store/pipeline-batch s "batch-5"))))
      (is (false? (:integrity-assessment-current? (store/pipeline-batch s "batch-6"))) "batch-6 integrity assessment stale")
      (is (false? (:bonding-grounding-confirmed? (store/pipeline-batch s "batch-7"))) "batch-7 bonding/grounding unconfirmed")
      (is (false? (:dispatched? (store/pipeline-batch s "batch-1"))))
      (is (false? (:delivered? (store/pipeline-batch s "batch-1"))))
      (is (= ["batch-1" "batch-2" "batch-3" "batch-4" "batch-5" "batch-6" "batch-7"]
             (mapv :id (store/all-pipeline-batches s))))
      (is (nil? (store/segment-assessment-of s "batch-1")))
      (is (= [] (store/ledger s)))
      (is (= [] (store/dispatch-history s)))
      (is (= [] (store/delivery-history s)))
      (is (zero? (store/next-dispatch-sequence s "JPN")))
      (is (zero? (store/next-delivery-sequence s "JPN")))
      (is (false? (store/pipeline-batch-already-dispatched? s "batch-1")))
      (is (false? (store/pipeline-batch-already-delivered? s "batch-1"))))))

(deftest write-and-ledger-parity
  (doseq [[label s] (backends)]
    (testing label
      (testing "partial upsert merges, preserving untouched fields"
        (store/commit-record! s {:effect :batch/upsert
                                 :value {:id "batch-1" :product-grade "JIS-K-2202-Naphtha-CR"}})
        (is (= "JIS-K-2202-Naphtha-CR" (:product-grade (store/pipeline-batch s "batch-1"))))
        (is (= "JPN" (:jurisdiction (store/pipeline-batch s "batch-1"))) "unrelated field preserved"))
      (testing "assessment payloads commit and read back"
        (store/commit-record! s {:effect :segment-assessment/set :path ["batch-1"]
                                 :payload {:jurisdiction "JPN" :checklist ["a" "b"]}})
        (is (= {:jurisdiction "JPN" :checklist ["a" "b"]} (store/segment-assessment-of s "batch-1"))))
      (testing "batch dispatch drafts a record and advances the dispatch sequence"
        (store/commit-record! s {:effect :batch/mark-dispatched :path ["batch-1"]})
        (is (= "JPN-DISPATCH-000000" (get (first (store/dispatch-history s)) "record_id")))
        (is (= "batch-dispatch-draft" (get (first (store/dispatch-history s)) "kind")))
        (is (true? (:dispatched? (store/pipeline-batch s "batch-1"))))
        (is (= 1 (count (store/dispatch-history s))))
        (is (= 1 (store/next-dispatch-sequence s "JPN")))
        (is (true? (store/pipeline-batch-already-dispatched? s "batch-1"))))
      (testing "batch delivery drafts a record and advances the delivery sequence"
        (store/commit-record! s {:effect :batch/mark-delivered :path ["batch-1"]})
        (is (= "JPN-DELIVERY-000000" (get (first (store/delivery-history s)) "record_id")))
        (is (= "batch-delivery-draft" (get (first (store/delivery-history s)) "kind")))
        (is (true? (:delivered? (store/pipeline-batch s "batch-1"))))
        (is (= 1 (count (store/delivery-history s))))
        (is (= 1 (store/next-delivery-sequence s "JPN")))
        (is (true? (store/pipeline-batch-already-delivered? s "batch-1"))))
      (testing "ledger is append-only and order-preserving"
        (store/append-ledger! s {:op :a :disposition :commit})
        (store/append-ledger! s {:op :b :disposition :hold})
        (is (= [:commit :hold] (mapv :disposition (store/ledger s))))))))

(deftest datomic-empty-store-is-usable
  (let [s (store/datomic-store)]
    (is (nil? (store/pipeline-batch s "nope")))
    (is (= [] (store/all-pipeline-batches s)))
    (is (= [] (store/ledger s)))
    (is (= [] (store/dispatch-history s)))
    (is (= [] (store/delivery-history s)))
    (is (zero? (store/next-dispatch-sequence s "JPN")))
    (is (zero? (store/next-delivery-sequence s "JPN")))
    (store/with-pipeline-batches s {"x" {:id "x" :batch-id "PKT-X-1"
                                         :origin-terminal "T-1" :destination-terminal "T-2"
                                         :product-grade "grade-x" :volume-barrels 1000
                                         :line-pressure-mpa-actual 8.0
                                         :line-pressure-mpa-min 6.0 :line-pressure-mpa-max 10.0
                                         :prior-segment-pod-confirmed? true
                                         :integrity-assessment-current? true
                                         :contamination-flag-raised? false :contamination-flag-resolved? false
                                         :bonding-grounding-confirmed? true
                                         :dispatched? false :delivered? false
                                         :jurisdiction "JPN" :status :intake}})
    (is (= "grade-x" (:product-grade (store/pipeline-batch s "x"))))))
