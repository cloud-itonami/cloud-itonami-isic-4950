(ns pipeline.registry-test
  (:require [clojure.test :refer [deftest is]]
            [pipeline.registry :as r]))

;; ----------------------------- range-check pure functions -----------------------------

(deftest line-pressure-two-sided-window
  (is (not (r/line-pressure-out-of-range? 8.0 6.0 10.0)) "in-window -> ok")
  (is (not (r/line-pressure-out-of-range? 6.0 6.0 10.0)) "at min boundary -> ok")
  (is (not (r/line-pressure-out-of-range? 10.0 6.0 10.0)) "at max boundary -> ok")
  (is (r/line-pressure-out-of-range? 12.0 6.0 10.0) "above max -> out of range")
  (is (r/line-pressure-out-of-range? 4.0 6.0 10.0) "below min -> out of range")
  (is (r/line-pressure-out-of-range? nil 6.0 10.0) "missing actual -> unsafe")
  (is (r/line-pressure-out-of-range? 8.0 nil 10.0) "missing min -> unsafe"))

;; ----------------------------- register-dispatch-record -----------------------------

(deftest dispatch-is-a-draft-not-a-real-dispatch
  (let [result (r/register-dispatch-record "batch-1" "JPN" 0)]
    (is (nil? (get-in result ["certificate" "proof"])))
    (is (= (get-in result ["certificate" "issued_by_registry"]) false))
    (is (= (get-in result ["certificate" "status"]) "draft-unsigned"))))

(deftest dispatch-assigns-dispatch-number
  (let [result (r/register-dispatch-record "batch-1" "JPN" 7)]
    (is (= (get result "dispatch_number") "JPN-DISPATCH-000007"))
    (is (= (get-in result ["record" "pipeline_batch_id"]) "batch-1"))
    (is (= (get-in result ["record" "kind"]) "batch-dispatch-draft"))
    (is (= (get-in result ["record" "immutable"]) true))))

(deftest dispatch-validation-rules
  (is (thrown? Exception (r/register-dispatch-record "" "JPN" 0)))
  (is (thrown? Exception (r/register-dispatch-record "batch-1" "" 0)))
  (is (thrown? Exception (r/register-dispatch-record "batch-1" "JPN" -1))))

;; ----------------------------- register-delivery-record -----------------------------

(deftest delivery-is-a-draft-not-a-real-delivery
  (let [result (r/register-delivery-record "batch-1" "JPN" 0)]
    (is (nil? (get-in result ["certificate" "proof"])))
    (is (= (get-in result ["certificate" "issued_by_registry"]) false))
    (is (= (get-in result ["certificate" "status"]) "draft-unsigned"))))

(deftest delivery-assigns-delivery-number
  (let [result (r/register-delivery-record "batch-1" "JPN" 7)]
    (is (= (get result "delivery_number") "JPN-DELIVERY-000007"))
    (is (= (get-in result ["record" "pipeline_batch_id"]) "batch-1"))
    (is (= (get-in result ["record" "kind"]) "batch-delivery-draft"))
    (is (= (get-in result ["record" "immutable"]) true))))

(deftest delivery-validation-rules
  (is (thrown? Exception (r/register-delivery-record "" "JPN" 0)))
  (is (thrown? Exception (r/register-delivery-record "batch-1" "" 0)))
  (is (thrown? Exception (r/register-delivery-record "batch-1" "JPN" -1))))

(deftest history-is-append-only
  (let [c1 (r/register-dispatch-record "batch-1" "JPN" 0)
        hist (r/append [] c1)
        c2 (r/register-dispatch-record "batch-2" "JPN" 1)
        hist2 (r/append hist c2)]
    (is (= 2 (count hist2)))
    (is (= "JPN-DISPATCH-000000" (get-in hist2 [0 "record_id"])))
    (is (= "JPN-DISPATCH-000001" (get-in hist2 [1 "record_id"])))))
