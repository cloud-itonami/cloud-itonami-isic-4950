(ns pipeline.registry
  "Pure-function batch-dispatch + batch-delivery record construction --
  an append-only pipeline-batch book-of-record draft -- AND the pure
  pipeline-integrity range-check function the Pipeline Integrity Governor
  calls to re-verify a batch's own physical ground truth before any
  dispatch.

  Unlike `freightops`/4920's own registry (which delegates tracking-
  number validation to a real, pre-existing bespoke capability library
  `kotoba-lang/logistics`), this pipeline-transport vertical has NO
  pre-existing capability library to wrap -- there is no 'kotoba-lang/
  pipeline' to call. So this namespace is self-contained: the line-
  pressure two-sided range check is a pure function defined HERE, not
  delegated. The actor layer adds the governed proposal/approval loop on
  top; the governor calls this same pure function to INDEPENDENTLY re-
  verify the batch's own recorded line pressure before any real-world
  dispatch, rather than trusting the advisor's self-reported confidence.

  Like every sibling actor's registry, there is no single international
  reference-number standard for a batch-dispatch or batch-delivery
  record -- every operator/jurisdiction assigns its own reference
  format. This namespace does NOT invent one beyond a jurisdiction-
  scoped sequence number; it validates the record's required fields,
  the same honest, non-fabricating discipline `pipeline.facts` uses.

  This namespace is pure data + pure functions -- no I/O, no network
  call to any real SCADA/pipeline-control system. It builds the RECORD
  an operator would keep, not the act of dispatching a real batch into a
  pressurized line or settling a real delivery itself (that is
  `pipeline.operation`'s `:batch/dispatch`/`:delivery/settle`, always
  human-gated -- see README `Actuation`)."
  (:require [kotoba.lang.text :as str]))

(defn- unsigned-certificate
  "Every certificate this actor produces is UNSIGNED -- signature is
  the operator's act, not this actor's. See README `Actuation`."
  [kind subject record-id]
  {"@context" ["https://www.w3.org/ns/credentials/v2"]
   "type" ["VerifiableCredential" kind]
   "credentialSubject" {"id" subject "record" record-id}
   "proof" nil
   "issued_by_registry" false
   "status" "draft-unsigned"})

(defn- zero-pad [n w]
  (let [s (str n)]
    (str (apply str (repeat (max 0 (- w (count s))) "0")) s)))

;; ----------------------------- pipeline-integrity range checks (pure) -----------------------------
;;
;; The Pipeline Integrity Governor calls these to INDEPENDENTLY re-verify
;; the batch's own recorded physical values before authorizing a dispatch.
;; Each returns true when the value is provably OUTSIDE the safe envelope --
;; the conservative integrity choice, matching the two-sided-tolerance
;; discipline of the aerospace siblings: a value that cannot be certified
;; inside the safe envelope is treated as a violation, not as 'unknown
;; therefore ok'. Missing data -> violation (cannot verify safe to dispatch).

(defn line-pressure-out-of-range?
  "Two-sided line-pressure window (the aerospace two-sided-tolerance
  pattern, applied to a pressurized pipeline): the batch's measured line
  pressure must stay within its declared safe operating window [min, max].
  Actual below min risks slack-line / cavitation / column separation (and,
  for product, incomplete sweep); above max risks overpressure and line
  rupture. Missing any bound -> unsafe (cannot verify the safe window
  before opening the segment to flow)."
  [actual min max]
  (cond
    (or (nil? actual) (nil? min) (nil? max)) true
    (or (< actual min) (> actual max))       true
    :else                                    false))

;; ----------------------------- record construction -----------------------------

(defn register-dispatch-record
  "Validate + construct the BATCH-DISPATCH registration DRAFT -- the
  operator's own legal act of dispatching a real batch into a
  pressurized pipeline. Pure function -- does not touch any real SCADA
  or pipeline-control system; it builds the RECORD an operator would
  keep. `pipeline.governor` independently re-verifies the batch's own
  line-pressure window, prior-segment POD chain, contamination-flag
  state, integrity-assessment currency, bonding-and-grounding ground
  truth, and blocks a double-dispatch of the same batch, before this is
  ever allowed to commit."
  [pipeline-batch-id jurisdiction sequence]
  (when-not (and pipeline-batch-id (not= pipeline-batch-id ""))
    (throw (ex-info "batch-dispatch: pipeline_batch_id required" {})))
  (when-not (and jurisdiction (not= jurisdiction ""))
    (throw (ex-info "batch-dispatch: jurisdiction required" {})))
  (when (< sequence 0)
    (throw (ex-info "batch-dispatch: sequence must be >= 0" {})))
  (let [dispatch-number (str (str/upper jurisdiction) "-DISPATCH-" (zero-pad sequence 6))
        record {"record_id" dispatch-number
                "kind" "batch-dispatch-draft"
                "pipeline_batch_id" pipeline-batch-id
                "jurisdiction" jurisdiction
                "immutable" true}]
    {"record" record "dispatch_number" dispatch-number
     "certificate" (unsigned-certificate "BatchDispatch" dispatch-number dispatch-number)}))

(defn register-delivery-record
  "Validate + construct the BATCH-DELIVERY registration DRAFT -- the
  operator's own legal act of settling a real delivery (custody transfer
  at the destination terminal, volume / grade reconciliation). Pure
  function -- does not touch any real pipeline-accounting system; it
  builds the RECORD an operator would keep. `pipeline.governor`
  independently re-verifies the batch's own evidence completeness and
  blocks a double-delivery of the same batch, before this is ever
  allowed to commit."
  [pipeline-batch-id jurisdiction sequence]
  (when-not (and pipeline-batch-id (not= pipeline-batch-id ""))
    (throw (ex-info "batch-delivery: pipeline_batch_id required" {})))
  (when-not (and jurisdiction (not= jurisdiction ""))
    (throw (ex-info "batch-delivery: jurisdiction required" {})))
  (when (< sequence 0)
    (throw (ex-info "batch-delivery: sequence must be >= 0" {})))
  (let [delivery-number (str (str/upper jurisdiction) "-DELIVERY-" (zero-pad sequence 6))
        record {"record_id" delivery-number
                "kind" "batch-delivery-draft"
                "pipeline_batch_id" pipeline-batch-id
                "jurisdiction" jurisdiction
                "immutable" true}]
    {"record" record "delivery_number" delivery-number
     "certificate" (unsigned-certificate "BatchDelivery" delivery-number delivery-number)}))

(defn append [history result]
  (conj (vec history) (get result "record")))
