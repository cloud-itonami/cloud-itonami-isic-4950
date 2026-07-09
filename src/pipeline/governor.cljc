(ns pipeline.governor
  "Pipeline Integrity Governor -- the independent compliance layer that
  earns the PipelineTransport advisor the right to commit. The LLM has
  no notion of jurisdictional pipeline-safety law, whether a batch's own
  line pressure actually lies inside its declared safe window, whether
  custody was actually proved on the prior segment, whether a product
  contamination flag has actually been resolved, whether the line's
  integrity assessment (ILI/pigging/hydrotest) is actually current,
  whether bonding-and-grounding was actually confirmed before flow, or
  when an act stops being a draft and becomes a real-world batch
  dispatch or delivery settlement, so this MUST be a separate system
  able to *reject* a proposal and fall back to HOLD.

  Unlike `freightops`/4920's own governor (built on TOP of a real,
  pre-existing bespoke capability library `kotoba-lang/logistics`),
  this pipeline-transport vertical has NO pre-existing pipeline
  capability library to delegate to -- so the one physical range check
  (line-pressure two-sided window) is a pure function defined in
  `pipeline.registry` and called directly here, the SAME 'reuse a
  capability library's own validated function' discipline
  `retailops.governor`'s ean13 check establishes, here applied to this
  vertical's OWN pure registry functions rather than a separate
  library.

  `:itonami.blueprint/governor` is `:pipeline-integrity-governor`,
  grep-verified UNIQUE fleet-wide -- no naming-collision precedent
  question, a fresh independent build following the SAME governed-
  actor architecture (langgraph StateGraph + independent Governor +
  Phase 0->3 rollout) established by `cloud-itonami-isic-6511`.

  Seven checks, in priority order, ALL HARD violations: a human
  approver CANNOT override them. The confidence/actuation gate is
  SOFT: it asks a human to look (low confidence / actuation), and the
  human may approve -- but see `pipeline.phase`: for `:stake :batch/
  dispatch`/`:delivery/settle` (a real dispatch or delivery) NO phase
  ever allows auto-commit either. Two independent layers agree that
  actuation is always a human call.

    1. Spec-basis                  -- did the jurisdiction proposal cite
                                       an OFFICIAL source
                                       (`pipeline.facts`), or invent one?
    2. Evidence incomplete         -- for `:batch/dispatch`/`:delivery/
                                       settle`, has the jurisdiction
                                       actually been assessed with a
                                       full pipeline-integrity / custody
                                       / bonding-grounding evidence
                                       checklist on file?
    3. Line pressure out of range  -- for `:batch/dispatch`, INDEPENDENTLY
                                       verify the batch's own measured
                                       line pressure stays inside its
                                       declared safe window [min,max] via
                                       `pipeline.registry/line-pressure-
                                       out-of-range?` (the aerospace two-
                                       sided-tolerance discipline),
                                       evaluated UNCONDITIONALLY.
    4. POD-chain integrity broken  -- for `:batch/dispatch`, INDEPENDENTLY
                                       verify custody was proved on the
                                       prior segment (`:prior-segment-pod-
                                       confirmed?`) -- the freight 4920
                                       POD-chain discipline, evaluated
                                       UNCONDITIONALLY.
    5. Product contamination flag
       unresolved                  -- for `:batch/dispatch`, INDEPENDENTLY
                                       verify an interface-mixing / grade
                                       contamination flag has been
                                       resolved (raised AND not resolved
                                       -> hold), evaluated
                                       UNCONDITIONALLY.
    6. Integrity assessment stale  -- for `:batch/dispatch`, INDEPENDENTLY
                                       verify the line's integrity
                                       assessment (ILI/pigging/hydrotest)
                                       is current (`:integrity-assessment-
                                       current?`) -- the PHMSA recurring
                                       ILI duty, evaluated
                                       UNCONDITIONALLY.
    7. Bonding/grounding
       unconfirmed                 -- for `:batch/dispatch`, INDEPENDENTLY
                                       verify bonding-and-grounding was
                                       confirmed (`:bonding-grounding-
                                       confirmed?`) -- static-electricity
                                       ignition during flow, evaluated
                                       UNCONDITIONALLY.
    8. Confidence floor / actuation
       gate                        -- LLM confidence below threshold,
                                       OR the op is `:batch/dispatch`/
                                       `:delivery/settle` (REAL acts)
                                       -> escalate.

  Two more guards, double-dispatch/double-delivery prevention, are
  enforced but NOT listed as numbered HARD checks above because they
  need no upstream comparison at all -- `already-dispatch-violations`/
  `already-delivery-violations` refuse to dispatch/deliver the SAME
  batch twice, off dedicated `:dispatched?`/`:delivered?` facts (never
  a `:status` value) -- the SAME 'check a dedicated boolean, not
  status' discipline every prior governor's guards establish, informed
  by `cloud-itonami-isic-6492`'s status-lifecycle bug (ADR-2607071320)."
  (:require [pipeline.facts :as facts]
            [pipeline.registry :as registry]
            [pipeline.store :as store]))

(def confidence-floor 0.6)

(def high-stakes
  "Stakes grave enough to always require a human, even when clean.
  Dispatching a real batch into a pressurized line (starting flow
  against a live pressure envelope) and settling a real delivery
  (custody transfer at the destination terminal) are the two
  real-world actuation events this actor performs -- a two-member set,
  matching every sibling's own dual-actuation shape."
  #{:batch/dispatch :delivery/settle})

;; ----------------------------- checks -----------------------------

(defn- spec-basis-violations
  "A `:segment/verify` (or `:batch/dispatch`/`:delivery/settle`) proposal
  with no spec-basis citation is a HARD violation -- never invent a
  jurisdiction's pipeline-integrity requirements."
  [{:keys [op]} proposal]
  (when (contains? #{:segment/verify :batch/dispatch :delivery/settle} op)
    (let [value (:value proposal)]
      (when (or (empty? (:cites proposal))
                (and (contains? value :spec-basis) (nil? (:spec-basis value))))
        [{:rule :no-spec-basis
          :detail "公式spec-basisの引用が無い提案は法域要件として扱えない"}]))))

(defn- evidence-incomplete-violations
  "For `:batch/dispatch`/`:delivery/settle`, the jurisdiction's required
  pipeline-integrity / custody / bonding-grounding evidence must
  actually be satisfied -- do not trust the advisor's self-reported
  confidence alone."
  [{:keys [op subject]} st]
  (when (contains? #{:batch/dispatch :delivery/settle} op)
    (let [b (store/pipeline-batch st subject)
          assessment (store/segment-assessment-of st subject)]
      (when-not (and assessment
                     (facts/required-evidence-satisfied?
                      (:jurisdiction b) (:checklist assessment)))
        [{:rule :evidence-incomplete
          :detail "法域の必要書類(パイプラインインテグリティ評価/バッチ引渡記録/接地確認等)が充足していない状態での提案"}]))))

(defn- line-pressure-out-of-range-violations
  "For `:batch/dispatch`, INDEPENDENTLY verify the batch's own measured
  line pressure stays inside its declared safe window [min,max] via
  `pipeline.registry/line-pressure-out-of-range?` (the aerospace two-
  sided-tolerance discipline). Evaluated UNCONDITIONALLY (every
  dispatch needs a line pressure inside its safe window)."
  [{:keys [op subject]} st]
  (when (= op :batch/dispatch)
    (let [b (store/pipeline-batch st subject)]
      (when (registry/line-pressure-out-of-range?
             (:line-pressure-mpa-actual b)
             (:line-pressure-mpa-min b)
             (:line-pressure-mpa-max b))
        [{:rule :line-pressure-out-of-range
          :detail (str subject " の管内圧力(" (:line-pressure-mpa-actual b)
                      " MPa)が安全窓[" (:line-pressure-mpa-min b) ", "
                      (:line-pressure-mpa-max b) "] MPa の外 -- 出荷提案は進められない")}]))))

(defn- pod-chain-integrity-broken-violations
  "For `:batch/dispatch`, INDEPENDENTLY verify custody was proved on the
  prior segment (`:prior-segment-pod-confirmed?`) -- the freight 4920
  POD-chain discipline. Evaluated UNCONDITIONALLY."
  [{:keys [op subject]} st]
  (when (= op :batch/dispatch)
    (let [b (store/pipeline-batch st subject)]
      (when (not (true? (:prior-segment-pod-confirmed? b)))
        [{:rule :pod-chain-integrity-broken
          :detail (str subject " は前区間のPOD(引渡証明)連鎖が未確認 -- 出荷提案は進められない")}]))))

(defn- product-contamination-flag-unresolved-violations
  "For `:batch/dispatch`, INDEPENDENTLY verify a product contamination
  flag (interface mixing / grade contamination) has been resolved.
  Raised AND not resolved -> hold. Evaluated UNCONDITIONALLY."
  [{:keys [op subject]} st]
  (when (= op :batch/dispatch)
    (let [b (store/pipeline-batch st subject)]
      (when (and (true? (:contamination-flag-raised? b))
                 (not (true? (:contamination-flag-resolved? b))))
        [{:rule :product-contamination-flag-unresolved
          :detail (str subject " は未解決の製品汚染フラグがある -- 出荷提案は進められない")}]))))

(defn- integrity-assessment-stale-violations
  "For `:batch/dispatch`, INDEPENDENTLY verify the line's integrity
  assessment (ILI/pigging/hydrotest interval) is current
  (`:integrity-assessment-current?`) -- the PHMSA recurring ILI duty.
  Evaluated UNCONDITIONALLY."
  [{:keys [op subject]} st]
  (when (= op :batch/dispatch)
    (let [b (store/pipeline-batch st subject)]
      (when (not (true? (:integrity-assessment-current? b)))
        [{:rule :integrity-assessment-stale
          :detail (str subject " は管路インテグリティ評価(ILI/ピグ/耐圧試験)が期限切れ -- 出荷提案は進められない")}]))))

(defn- bonding-grounding-unconfirmed-violations
  "For `:batch/dispatch`, INDEPENDENTLY verify bonding-and-grounding was
  confirmed (`:bonding-grounding-confirmed?`) -- static-electricity
  ignition during flow. Evaluated UNCONDITIONALLY."
  [{:keys [op subject]} st]
  (when (= op :batch/dispatch)
    (let [b (store/pipeline-batch st subject)]
      (when (not (true? (:bonding-grounding-confirmed? b)))
        [{:rule :bonding-grounding-unconfirmed
          :detail (str subject " は接地面のボンディング・接地が未確認 -- 静電気引火リスクのため出荷提案は進められない")}]))))

(defn- already-dispatch-violations
  "For `:batch/dispatch`, refuses to dispatch the SAME batch twice, off
  a dedicated `:dispatched?` fact (never a `:status` value)."
  [{:keys [op subject]} st]
  (when (= op :batch/dispatch)
    (when (store/pipeline-batch-already-dispatched? st subject)
      [{:rule :already-dispatched
        :detail (str subject " は既に出荷済み")}])))

(defn- already-delivery-violations
  "For `:delivery/settle`, refuses to settle the SAME batch's delivery
  twice, off a dedicated `:delivered?` fact (never a `:status` value)."
  [{:keys [op subject]} st]
  (when (= op :delivery/settle)
    (when (store/pipeline-batch-already-delivered? st subject)
      [{:rule :already-delivered
        :detail (str subject " は既に引渡済み")}])))

(defn check
  "Censors a PipelineTransport advisor proposal against the governor rules.
  Returns {:ok? bool :violations [..] :confidence c :escalate? bool
  :high-stakes? bool :hard? bool}."
  [request _context proposal st]
  (let [hard (into []
                   (concat (spec-basis-violations request proposal)
                           (evidence-incomplete-violations request st)
                           (line-pressure-out-of-range-violations request st)
                           (pod-chain-integrity-broken-violations request st)
                           (product-contamination-flag-unresolved-violations request st)
                           (integrity-assessment-stale-violations request st)
                           (bonding-grounding-unconfirmed-violations request st)
                           (already-dispatch-violations request st)
                           (already-delivery-violations request st)))
        conf (:confidence proposal 0.0)
        low? (< conf confidence-floor)
        stakes? (boolean (high-stakes (:stake proposal)))
        hard? (boolean (seq hard))]
    {:ok?          (and (not hard?) (not low?) (not stakes?))
     :violations   hard
     :confidence   conf
     :hard?        hard?
     :escalate?    (and (not hard?) (or low? stakes?))
     :high-stakes? stakes?}))

(defn hold-fact
  "The audit fact written when a proposal is rejected (HOLD)."
  [request context verdict]
  {:t          :governor-hold
   :op         (:op request)
   :actor      (:actor-id context)
   :subject    (:subject request)
   :disposition :hold
   :basis      (mapv :rule (:violations verdict))
   :violations (:violations verdict)
   :confidence (:confidence verdict)})
