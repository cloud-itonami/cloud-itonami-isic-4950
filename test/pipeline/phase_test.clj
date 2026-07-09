(ns pipeline.phase-test
  "The phase table as executable tests. The invariant this repo cannot
  regress on: `:batch/dispatch`/`:delivery/settle` must NEVER be a
  member of any phase's `:auto` set."
  (:require [clojure.test :refer [deftest is testing]]
            [pipeline.phase :as phase]))

(deftest batch-dispatch-never-auto-at-any-phase
  (testing "structural invariant: no phase, now or in any future entry, auto-commits a real batch dispatch"
    (doseq [[n {:keys [auto]}] phase/phases]
      (is (not (contains? auto :batch/dispatch))
          (str "phase " n " must not auto-commit :batch/dispatch")))))

(deftest delivery-settle-never-auto-at-any-phase
  (testing "structural invariant: no phase, now or in any future entry, auto-commits a real delivery settlement"
    (doseq [[n {:keys [auto]}] phase/phases]
      (is (not (contains? auto :delivery/settle))
          (str "phase " n " must not auto-commit :delivery/settle")))))

(deftest phase-0-is-fully-read-only
  (is (empty? (:writes (get phase/phases 0)))))

(deftest phase-3-auto-commits-only-no-capital-risk-ops
  (testing ":batch/intake carries no direct capital risk -- auto-eligible; it is the ONLY auto-eligible op in this domain"
    (is (= #{:batch/intake} (:auto (get phase/phases 3))))))

(deftest gate-hold-always-wins
  (is (= :hold (:disposition (phase/gate 3 {:op :batch/intake} :hold)))))

(deftest gate-escalates-a-clean-non-auto-write
  (is (= :escalate (:disposition (phase/gate 3 {:op :batch/dispatch} :commit))))
  (is (= :escalate (:disposition (phase/gate 3 {:op :delivery/settle} :commit)))))

(deftest gate-holds-a-write-disabled-in-this-phase
  (is (= :hold (:disposition (phase/gate 0 {:op :batch/intake} :commit)))))
