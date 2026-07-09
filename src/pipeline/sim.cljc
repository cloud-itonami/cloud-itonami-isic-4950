(ns pipeline.sim
  "Demo driver -- `clojure -M:dev:run`. Walks a clean batch through
  intake -> segment-integrity assessment -> batch dispatch (escalate/
  approve/commit) -> delivery settlement (escalate/approve/commit),
  then shows HARD-hold scenarios: a jurisdiction with no spec-basis,
  a line pressure outside its safe window, a broken prior-segment POD
  chain, an unresolved product contamination flag, a stale integrity
  assessment, unconfirmed bonding/grounding, a double dispatch, and a
  double delivery.

  Like every sibling actor's new checks, this actor's pipeline-integrity
  checks (`line-pressure-out-of-range?`, `pod-chain-integrity-broken?`,
  `product-contamination-flag-unresolved?`, `integrity-assessment-stale?`,
  `bonding-grounding-unconfirmed?`) are evaluated directly at
  `:batch/dispatch` time rather than via a separate screening op -- a
  real dispatch decision validates line pressure, custody chain,
  contamination state, integrity currency and bonding/grounding at the
  point of the act itself, not as a discrete pre-screening ceremony.
  Each check is still exercised directly and independently below, one
  batch per HARD-hold scenario, following the SAME 'exercise the failure
  mode directly, never only via a happy-path actuation' discipline
  `parksafety`'s ADR-2607071922 Decision 5 and every sibling since
  establish."
  (:require [langgraph.graph :as g]
            [pipeline.store :as store]
            [pipeline.operation :as op]))

(def operator {:actor-id "op-1" :actor-role :pipeline-controller :phase 3})

(defn- exec-op [actor tid request context]
  (g/run* actor {:request request :context context} {:thread-id tid}))

(defn- approve! [actor tid]
  (g/run* actor {:approval {:status :approved :by "op-1"}} {:thread-id tid :resume? true}))

(defn -main [& _]
  (let [db (store/seed-db)
        actor (op/build db)]
    (println "== batch/intake batch-1 (JPN, clean) ==")
    (println (exec-op actor "t1" {:op :batch/intake :subject "batch-1"
                                  :patch {:id "batch-1" :product-grade "JIS-K-2202-Diesel-CR"}} operator))

    (println "== segment/verify batch-1 (escalates -- human approves) ==")
    (println (exec-op actor "t2" {:op :segment/verify :subject "batch-1"} operator))
    (println (approve! actor "t2"))

    (println "== batch/dispatch batch-1 (always escalates -- :batch/dispatch) ==")
    (let [r (exec-op actor "t3" {:op :batch/dispatch :subject "batch-1"} operator)]
      (println r)
      (println "-- human pipeline controller approves --")
      (println (approve! actor "t3")))

    (println "== delivery/settle batch-1 (always escalates -- :delivery/settle) ==")
    (let [r (exec-op actor "t4" {:op :delivery/settle :subject "batch-1"} operator)]
      (println r)
      (println "-- human pipeline controller approves --")
      (println (approve! actor "t4")))

    (println "== segment/verify batch-2 (no spec-basis -> HARD hold) ==")
    (println (exec-op actor "t5" {:op :segment/verify :subject "batch-2"} operator))

    (println "== segment/verify batch-3 (escalates -- human approves; sets up the line-pressure test) ==")
    (println (exec-op actor "t6" {:op :segment/verify :subject "batch-3"} operator))
    (println (approve! actor "t6"))

    (println "== batch/dispatch batch-3 (line pressure out of range -> HARD hold) ==")
    (println (exec-op actor "t7" {:op :batch/dispatch :subject "batch-3"} operator))

    (println "== segment/verify batch-4 (escalates -- human approves; sets up the POD-chain test) ==")
    (println (exec-op actor "t8" {:op :segment/verify :subject "batch-4"} operator))
    (println (approve! actor "t8"))

    (println "== batch/dispatch batch-4 (prior-segment POD chain broken -> HARD hold) ==")
    (println (exec-op actor "t9" {:op :batch/dispatch :subject "batch-4"} operator))

    (println "== segment/verify batch-5 (escalates -- human approves; sets up the contamination test) ==")
    (println (exec-op actor "t10" {:op :segment/verify :subject "batch-5"} operator))
    (println (approve! actor "t10"))

    (println "== batch/dispatch batch-5 (product contamination flag unresolved -> HARD hold) ==")
    (println (exec-op actor "t11" {:op :batch/dispatch :subject "batch-5"} operator))

    (println "== segment/verify batch-6 (escalates -- human approves; sets up the integrity-assessment test) ==")
    (println (exec-op actor "t12" {:op :segment/verify :subject "batch-6"} operator))
    (println (approve! actor "t12"))

    (println "== batch/dispatch batch-6 (integrity assessment stale -> HARD hold) ==")
    (println (exec-op actor "t13" {:op :batch/dispatch :subject "batch-6"} operator))

    (println "== segment/verify batch-7 (escalates -- human approves; sets up the bonding-grounding test) ==")
    (println (exec-op actor "t14" {:op :segment/verify :subject "batch-7"} operator))
    (println (approve! actor "t14"))

    (println "== batch/dispatch batch-7 (bonding/grounding unconfirmed -> HARD hold) ==")
    (println (exec-op actor "t15" {:op :batch/dispatch :subject "batch-7"} operator))

    (println "== batch/dispatch batch-1 AGAIN (double-dispatch -> HARD hold) ==")
    (println (exec-op actor "t16" {:op :batch/dispatch :subject "batch-1"} operator))

    (println "== delivery/settle batch-1 AGAIN (double-delivery -> HARD hold) ==")
    (println (exec-op actor "t17" {:op :delivery/settle :subject "batch-1"} operator))

    (println "== audit ledger ==")
    (doseq [f (store/ledger db)] (println f))

    (println "== draft batch-dispatch records ==")
    (doseq [r (store/dispatch-history db)] (println r))

    (println "== draft batch-delivery records ==")
    (doseq [r (store/delivery-history db)] (println r))))
