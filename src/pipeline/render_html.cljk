(ns pipeline.render-html
  "Build-time HTML renderer for `docs/samples/operator-console.html`.

  This namespace drives the REAL actor stack (`pipeline.operation` ->
  `pipeline.governor` -> `pipeline.phase` -> `pipeline.store`, on the
  langgraph-clj StateGraph via `langgraph.graph/run*` with real
  `interrupt-before` approval resumes) through a scenario, and renders
  the console from what that run actually produced.

  The page this replaces was hand-written and had NO generator behind
  it: it asserted governor verdicts as literal strings, showed 4 of the
  7 seeded batches, and could not go stale-loudly when the governor
  changed. Every entity id, terminal name, product grade, pressure,
  jurisdiction, dispatch/delivery number, hold rule and hold detail on
  the new page is read back out of `pipeline.store` after a real run.

  Two rules this file holds itself to:

    1. DERIVE, do not assert. The action-gate table is computed from
       `pipeline.phase/phases` + `pipeline.governor/high-stakes` at
       render time, so if someone ever adds `:batch/dispatch` to a
       phase's `:auto` set the page says so instead of continuing to
       claim it 'never auto-commits'. The HARD-hold table is grouped
       from the ledger the run produced, not from a list of rules
       someone believed were reachable.
    2. MEASURE approval attribution, do not assume it. `run-demo!`
       actually approves ops, and `render` then inspects the persisted
       artifacts for an approver key. This repo's `MemStore` turns out
       to be split: `:segment-assessment/set` persists `:payload` (so
       `:approved-by` survives) while `:batch/mark-dispatched` /
       `:batch/mark-delivered` ignore `:payload` entirely (so it does
       not). The page reports whichever it finds -- it self-corrects
       if the store is changed.

  Deterministic: no timestamps, no random, no wall-clock in page
  content. Two runs against the same seed are byte-identical.

  Usage: `clojure -M:dev:render-html [out-file]`
  (default `docs/samples/operator-console.html`)."
  (:require [jp-go-dds.skin]
            [kotoba.lang.text :as str]
            [langgraph.graph :as g]
            [pipeline.facts :as facts]
            [pipeline.governor :as governor]
            [pipeline.operation :as op]
            [pipeline.phase :as phase]
            [pipeline.registry :as registry]
            [pipeline.store :as store]))

(def ^:private operator
  "The injected actor context -- same shape `pipeline.sim` uses."
  {:actor-id "op-1" :actor-role :pipeline-controller :phase 3})

(def ^:private approver "op-1")
(def ^:private rejecter "op-2")

;; ----------------------------- scenario driver -----------------------------

(defn run-demo!
  "Runs a fresh `store/seed-db` through a scenario that reaches every
  disposition this actor can produce, using the real graph.

  Happy path (batch-1, the one clean JPN batch in `store/demo-data`):
  intake auto-commits at phase 3 (`:batch/intake` is the only member of
  phase 3's `:auto` set); a segment-integrity assessment escalates on
  the PHASE gate and is approved; the batch dispatch escalates on the
  GOVERNOR's high-stakes gate and is approved; the delivery settlement
  does the same.

  Every HARD rule in `pipeline.governor` is exercised, one isolated
  failure mode per batch, and one is exercised on batch-1 BEFORE its
  assessment exists so `:evidence-incomplete` fires alone:

    :evidence-incomplete                      batch-1, dispatch before verify
    :no-spec-basis                            batch-2 (jurisdiction ATL, unregistered)
    :line-pressure-out-of-range               batch-3 (12.0 MPa vs [6.0, 10.0])
    :pod-chain-integrity-broken               batch-4
    :product-contamination-flag-unresolved    batch-5
    :integrity-assessment-stale               batch-6
    :bonding-grounding-unconfirmed            batch-7
    :already-dispatched                       batch-1, second dispatch
    :already-delivered                        batch-1, second settlement

  batch-7's assessment is additionally REJECTED by a human once before
  being approved, so the human-rejection path (`:approval-rejected`,
  basis `:approver-rejected`) is on the page too -- it is a different
  thing from a governor HOLD and the console should not blur them.

  Returns {:db store :audit [fact ..]} where `:audit` is the langgraph
  `:audit` channel of each thread's FINAL state, in thread order (the
  store ledger only carries commits and holds; `:approval-granted`,
  `:approval-requested` and the advisor traces live in the graph
  state)."
  []
  (let [db     (store/seed-db)
        actor  (op/build db)
        states (atom [])
        start! (fn [tid request]
                 (let [s (:state (g/run* actor {:request request :context operator}
                                         {:thread-id tid}))]
                   (swap! states conj [tid s])
                   s))
        resume! (fn [tid status by]
                  (let [s (:state (g/run* actor {:approval {:status status :by by}}
                                          {:thread-id tid :resume? true}))]
                    ;; the resumed state's :audit already contains the
                    ;; pre-interrupt facts (they came back off the
                    ;; checkpoint), so replace rather than append.
                    (swap! states (fn [v] (mapv (fn [[t st]] (if (= t tid) [t s] [t st])) v)))
                    s))
        approve! (fn [tid] (resume! tid :approved approver))
        reject!  (fn [tid] (resume! tid :rejected rejecter))
        verify+approve! (fn [tid batch]
                          (start! tid {:op :segment/verify :subject batch})
                          (approve! tid))]

    ;; --- batch-1: full governed lifecycle -------------------------------
    (start! "t01-intake" {:op :batch/intake :subject "batch-1"
                          :patch {:id "batch-1"
                                  :product-grade "JIS-K-2202-Diesel-CR"}})

    ;; dispatch attempted BEFORE any assessment exists -> evidence-incomplete alone
    (start! "t02-dispatch-no-evidence" {:op :batch/dispatch :subject "batch-1"})

    (verify+approve! "t03-verify" "batch-1")

    (start! "t04-dispatch" {:op :batch/dispatch :subject "batch-1"})
    (approve! "t04-dispatch")

    (start! "t05-settle" {:op :delivery/settle :subject "batch-1"})
    (approve! "t05-settle")

    (start! "t06-dispatch-again" {:op :batch/dispatch :subject "batch-1"})
    (start! "t07-settle-again" {:op :delivery/settle :subject "batch-1"})

    ;; --- batch-2: unregistered jurisdiction ------------------------------
    (start! "t08-verify-atl" {:op :segment/verify :subject "batch-2"})

    ;; --- batch-3..6: one isolated physical failure mode each --------------
    (verify+approve! "t09-verify" "batch-3")
    (start! "t10-dispatch-pressure" {:op :batch/dispatch :subject "batch-3"})

    (verify+approve! "t11-verify" "batch-4")
    (start! "t12-dispatch-pod" {:op :batch/dispatch :subject "batch-4"})

    (verify+approve! "t13-verify" "batch-5")
    (start! "t14-dispatch-contamination" {:op :batch/dispatch :subject "batch-5"})

    (verify+approve! "t15-verify" "batch-6")
    (start! "t16-dispatch-integrity" {:op :batch/dispatch :subject "batch-6"})

    ;; --- batch-7: human rejects once, then approves ----------------------
    (start! "t17-verify-rejected" {:op :segment/verify :subject "batch-7"})
    (reject! "t17-verify-rejected")

    (verify+approve! "t18-verify" "batch-7")
    (start! "t19-dispatch-bonding" {:op :batch/dispatch :subject "batch-7"})

    {:db db
     :audit (into [] (mapcat (comp :audit second)) @states)}))

;; ----------------------------- html helpers -----------------------------

(defn- esc [v]
  (-> (str v)
      (str/replace "&" "&amp;")
      (str/replace "<" "&lt;")
      (str/replace ">" "&gt;")
      (str/replace "\"" "&quot;")))

(defn- kw-name [k]
  (cond
    (keyword? k) (subs (str k) 1)
    (nil? k)     ""
    :else        (str k)))

(defn- code [v] (str "<code>" (esc v) "</code>"))

(defn- yes-no
  "A boolean rendered with its own semantic class. `good?` says which
  value is the safe one for this particular field."
  [v good?]
  (if (= (boolean v) (boolean good?))
    (str "<span class=\"ok\">" (if v "yes" "no") "</span>")
    (str "<span class=\"err\">" (if v "yes" "no") "</span>")))

(defn- row [& cells]
  (str "        <tr>" (str/join (map #(str "<td>" % "</td>") cells)) "</tr>"))

(defn- table [headers rows]
  (str "    <table>\n"
       "      <thead><tr>" (str/join (map #(str "<th>" (esc %) "</th>") headers)) "</tr></thead>\n"
       "      <tbody>\n"
       (str/join "\n" rows) "\n"
       "      </tbody>\n"
       "    </table>\n"))

(defn- section [title lead body]
  (str "  <section class=\"card\">\n"
       "    <h2>" (esc title) "</h2>\n"
       (when lead (str "    <p class=\"muted\">" lead "</p>\n"))
       body
       "  </section>\n"))

;; ----------------------------- derivations -----------------------------

(defn- facts-for-subject [ledger id]
  (filterv #(= (:subject %) id) ledger))

(defn- last-outcome-cell [ledger id]
  (let [f (last (facts-for-subject ledger id))]
    (cond
      (nil? f) "<span class=\"muted\">no activity</span>"
      (= :governor-hold (:t f))
      (str "<span class=\"critical\">HARD hold &middot; "
           (esc (str/join ", " (map kw-name (:basis f)))) "</span>")
      (= :approval-rejected (:t f))
      (str "<span class=\"warn\">rejected by approver</span>")
      (= :committed (:t f))
      (str "<span class=\"ok\">committed &middot; " (esc (kw-name (:op f))) "</span>")
      :else (str "<span class=\"muted\">" (esc (kw-name (:t f))) "</span>"))))

(defn- hold-facts [ledger] (filterv #(= :governor-hold (:t %)) ledger))

(defn- hold-rule-groups
  "Group the HARD holds this run actually produced by rule, preserving
  first-seen order. Nothing here is a list of rules someone believed
  were reachable -- a rule that never fired never appears."
  [ledger]
  (let [hs (hold-facts ledger)
        pairs (for [f hs v (:violations f)] [(:rule v) f v])]
    (->> pairs
         (reduce (fn [acc [rule f v]]
                   (if (contains? (:index acc) rule)
                     (update-in acc [:index rule :count] inc)
                     (-> acc
                         (update :order conj rule)
                         (assoc-in [:index rule]
                                   {:rule rule :count 1
                                    :first-subject (:subject f)
                                    :first-op (:op f)
                                    :detail (:detail v)}))))
                 {:order [] :index {}})
         ((fn [{:keys [order index]}] (mapv index order))))))

(defn- approver-entry
  "Look for an approver identity actually persisted in `m`, by scanning
  its keys for one whose name mentions approval. Returns [k v] or nil.
  Deterministic: candidates are sorted by key name."
  [m]
  (when (map? m)
    (->> m
         (filter (fn [[k _]] (re-find #"(?i)approv" (kw-name k))))
         (sort-by (comp kw-name key))
         first)))

(defn- granted-approvals
  "Every `:approval-granted` audit fact this run produced."
  [audit]
  (filterv #(= :approval-granted (:t %)) audit))

(defn- persisted-artifact
  "The SSoT artifact a granted approval's op actually wrote, so we can
  ask it (not a convention) whether it kept the approver."
  [db {:keys [op subject]}]
  (case op
    :segment/verify  {:label "segment-assessment payload"
                      :value (store/segment-assessment-of db subject)}
    :batch/dispatch  {:label "batch-dispatch record"
                      :value (first (filter #(= subject (get % "pipeline_batch_id"))
                                            (store/dispatch-history db)))}
    :delivery/settle {:label "batch-delivery record"
                      :value (first (filter #(= subject (get % "pipeline_batch_id"))
                                            (store/delivery-history db)))}
    {:label "batch record" :value (store/pipeline-batch db subject)}))

;; ----------------------------- sections -----------------------------

(defn- batches-section [db ledger]
  (section
   "Pipeline batches"
   (str "All " (count (store/all-pipeline-batches db))
        " batches seeded in <code>pipeline.store/demo-data</code>, read back after the run. "
        "Dispatch and delivery numbers are whatever "
        "<code>pipeline.registry/register-dispatch-record</code> / "
        "<code>register-delivery-record</code> minted for the commits that were approved.")
   (table ["Batch" "Reference" "Route" "Product grade" "Volume (bbl)" "Juris." "Dispatched" "Delivered" "Last outcome"]
          (for [{:keys [id batch-id origin-terminal destination-terminal product-grade
                        volume-barrels jurisdiction dispatched? delivered?
                        dispatch-number delivery-number]}
                (store/all-pipeline-batches db)]
            (row (code id)
                 (esc batch-id)
                 (str (esc origin-terminal) " &rarr; " (esc destination-terminal))
                 (esc product-grade)
                 (str "<span class=\"num\">" (esc volume-barrels) "</span>")
                 (esc jurisdiction)
                 (if dispatched?
                   (str "<span class=\"ok\">" (esc dispatch-number) "</span>")
                   "<span class=\"muted\">&mdash;</span>")
                 (if delivered?
                   (str "<span class=\"ok\">" (esc delivery-number) "</span>")
                   "<span class=\"muted\">&mdash;</span>")
                 (last-outcome-cell ledger id))))))

(defn- ground-truth-section [db]
  (section
   "Physical ground truth the governor re-verifies"
   (str "The governor does not trust the advisor's self-reported confidence: for every "
        "<code>:batch/dispatch</code> it re-reads these fields off the SSoT and re-runs "
        "<code>pipeline.registry/line-pressure-out-of-range?</code> itself. "
        "The pressure verdict column below is that same pure function, called here at render time. "
        "Evidence completeness is <code>pipeline.facts/required-evidence-satisfied?</code> "
        "against the checklist actually committed for that batch.")
   (table ["Batch" "Line pressure (MPa)" "Safe window" "In window" "Prior-segment POD"
           "Contamination clear" "Integrity assessment current" "Bonding / grounding" "Evidence on file"]
          (for [{:keys [id jurisdiction line-pressure-mpa-actual line-pressure-mpa-min
                        line-pressure-mpa-max prior-segment-pod-confirmed?
                        contamination-flag-raised? contamination-flag-resolved?
                        integrity-assessment-current? bonding-grounding-confirmed?]}
                (store/all-pipeline-batches db)]
            (let [out? (registry/line-pressure-out-of-range?
                        line-pressure-mpa-actual line-pressure-mpa-min line-pressure-mpa-max)
                  assessment (store/segment-assessment-of db id)
                  evidence-ok? (facts/required-evidence-satisfied?
                                jurisdiction (:checklist assessment))
                  clear? (or (not contamination-flag-raised?) contamination-flag-resolved?)]
              (row (code id)
                   (str "<span class=\"num\">" (esc line-pressure-mpa-actual) "</span>")
                   (str "<span class=\"num\">[" (esc line-pressure-mpa-min) ", "
                        (esc line-pressure-mpa-max) "]</span>")
                   (yes-no (not out?) true)
                   (yes-no prior-segment-pod-confirmed? true)
                   (yes-no clear? true)
                   (yes-no integrity-assessment-current? true)
                   (yes-no bonding-grounding-confirmed? true)
                   (if assessment
                     (yes-no (boolean evidence-ok?) true)
                     "<span class=\"muted\">no assessment committed</span>")))))))

(defn- action-gate-section [audit]
  (let [ph          phase/default-phase
        {:keys [label writes auto]} (get phase/phases ph)
        every-auto  (into {} (for [[p spec] phase/phases] [p (:auto spec)]))
        never-auto? (fn [o] (every? #(not (contains? % o)) (vals every-auto)))
        observed    (reduce (fn [m f]
                              (cond
                                (= :approval-requested (:t f))
                                (update-in m [(:op f) :escalations] (fnil conj #{}) (:reason f))
                                :else m))
                            {} audit)
        ops         (sort-by kw-name (concat phase/read-ops phase/write-ops))]
    (section
     "Action gate"
     (str "Computed at render time from <code>pipeline.phase/phases</code> and "
          "<code>pipeline.governor/high-stakes</code> at phase " ph " (<em>" (esc label)
          "</em>), not written down here as prose. If a future change ever put "
          "<code>:batch/dispatch</code> into a phase's <code>:auto</code> set, this table would "
          "say so rather than keep claiming otherwise. Confidence floor: <span class=\"num\">"
          (esc governor/confidence-floor) "</span>.")
     (table ["Op" "Writes at this phase" "Auto-commit at this phase" "Auto-commit at ANY phase"
             "Governor high-stakes" "Escalation reasons seen in this run"]
            (for [o ops]
              (row (code o)
                   (yes-no (contains? writes o) true)
                   (if (contains? auto o)
                     "<span class=\"ok\">yes</span>"
                     "<span class=\"warn\">no &middot; human approval</span>")
                   (if (never-auto? o)
                     "<span class=\"critical\">never</span>"
                     "<span class=\"ok\">yes, at some phase</span>")
                   (if (contains? governor/high-stakes o)
                     "<span class=\"critical\">yes &middot; always escalates</span>"
                     "<span class=\"muted\">no</span>")
                   (let [rs (get-in observed [o :escalations])]
                     (if (seq rs)
                       (esc (str/join ", " (sort (map kw-name rs))))
                       "<span class=\"muted\">&mdash;</span>"))))))))

(defn- holds-section [ledger]
  (let [groups (hold-rule-groups ledger)]
    (section
     (str "HARD governor holds exercised in this run (" (count groups) " distinct rules, "
          (count (hold-facts ledger)) " holds)")
     (str "Grouped from the ledger this run produced. A HARD hold is un-overridable: it never "
          "reaches a human approver at all. The detail text is the governor's own, verbatim.")
     (table ["Rule" "Times" "First seen on" "Op" "Governor detail"]
            (for [{:keys [rule count first-subject first-op detail]} groups]
              (row (str "<span class=\"critical\">" (esc (kw-name rule)) "</span>")
                   (str "<span class=\"num\">" count "</span>")
                   (code first-subject)
                   (code first-op)
                   (esc detail)))))))

(defn- approval-section [db audit]
  (let [grants (granted-approvals audit)
        rows   (for [{:keys [op subject by] :as gr} grants]
                 (let [{:keys [label value]} (persisted-artifact db gr)
                       found (approver-entry value)]
                   (row (code op)
                        (code subject)
                        (esc by)
                        (esc label)
                        (if found
                          (str "<span class=\"ok\">retained &middot; "
                               (code (kw-name (key found))) " = " (esc (val found)) "</span>")
                          (str "<span class=\"warn\">not retained</span> "
                               "<span class=\"muted\">&mdash; "
                               (esc by) " (audit fact only)</span>")))))
        retained (count (filter (fn [gr] (approver-entry (:value (persisted-artifact db gr))))
                                grants))]
    (section
     "Approval attribution (measured, not assumed)"
     (str "Each row is a real <code>:approval-granted</code> fact from this run, joined against "
          "the artifact its commit actually wrote to the SSoT. The right-hand column is the "
          "result of looking for an approver key <em>in that artifact</em> &mdash; it is not a "
          "claim about how the store behaves. Measured here: <span class=\"num\">" retained
          "</span> of <span class=\"num\">" (count grants) "</span> approvals kept the approver. "
          "Where it is not retained, the approver is shown from the audit fact and labelled as "
          "such, so an empty cell can never be mistaken for &quot;nobody approved this&quot;.")
     (table ["Op" "Subject" "Approver (audit fact)" "Persisted artifact" "Approver in artifact"]
            rows))))

(defn- records-section [db]
  (let [ds (store/dispatch-history db)
        vs (store/delivery-history db)]
    (section
     "Draft registration records"
     (str "Append-only book-of-record drafts produced by "
          "<code>pipeline.registry</code>. Every certificate this actor produces is "
          "<code>status: draft-unsigned</code> &mdash; signature is the operator's act, "
          "not this actor's.")
     (table ["Kind" "Record id" "Batch" "Jurisdiction" "Immutable"]
            (for [r (concat ds vs)]
              (row (esc (get r "kind"))
                   (code (get r "record_id"))
                   (code (get r "pipeline_batch_id"))
                   (esc (get r "jurisdiction"))
                   (yes-no (get r "immutable") true)))))))

(defn- jurisdictions-section []
  (let [{:keys [requested covered covered-jurisdictions missing-jurisdictions]} (facts/coverage)]
    (section
     "Jurisdiction spec-basis catalog"
     (str "From <code>pipeline.facts/catalog</code> and its own honest "
          "<code>coverage</code> report: <span class=\"num\">" covered "</span> of "
          "<span class=\"num\">" requested "</span> requested jurisdictions have an official "
          "spec-basis. A jurisdiction not in this table has none &mdash; the advisor must not "
          "invent one, and the governor holds if it tries (see <code>batch-2</code>, "
          "jurisdiction ATL, above)."
          (when (seq missing-jurisdictions)
            (str " Missing: " (esc (str/join ", " missing-jurisdictions)) ".")))
     (table ["ISO3" "Authority" "Legal basis" "Provenance" "Required evidence"]
            (for [iso3 covered-jurisdictions
                  :let [{:keys [owner-authority legal-basis provenance required-evidence]}
                        (facts/spec-basis iso3)]]
              (row (code iso3)
                   (esc owner-authority)
                   (esc legal-basis)
                   (str "<a href=\"" (esc provenance) "\">" (esc provenance) "</a>")
                   (str "<ul><li>"
                        (str/join "</li><li>" (map esc required-evidence))
                        "</li></ul>")))))))

(defn- ledger-section [ledger]
  (section
   (str "Audit ledger (" (count ledger) " facts from this run)")
   (str "The append-only decision log <code>pipeline.store/ledger</code> holds after the run: "
        "every commit and every hold. Advisor traces, approval requests and approval grants "
        "live in the graph's <code>:audit</code> channel and are surfaced in the sections above.")
   (table ["#" "Fact" "Op" "Batch" "Disposition" "Basis" "Summary"]
          (map-indexed
           (fn [i {:keys [t op subject disposition basis summary]}]
             (row (str "<span class=\"num\">" (inc i) "</span>")
                  (let [cls (case t
                              :committed "ok"
                              :governor-hold "critical"
                              :approval-rejected "warn"
                              "muted")]
                    (str "<span class=\"" cls "\">" (esc (kw-name t)) "</span>"))
                  (code op)
                  (code subject)
                  (esc (kw-name disposition))
                  (if (seq basis)
                    (esc (str/join ", " (map kw-name basis)))
                    "<span class=\"muted\">&mdash;</span>")
                  (if summary (esc summary) "<span class=\"muted\">&mdash;</span>")))
           ledger))))

;; ----------------------------- document -----------------------------

(defn render
  "Renders the whole console from the result of `run-demo!`
  ({:db .. :audit ..}). Every value comes from that run."
  [{:keys [db audit]}]
  (let [ledger (vec (store/ledger db))
        holds  (hold-facts ledger)
        groups (hold-rule-groups ledger)]
    (str
     "<!DOCTYPE html>\n"
     "<html lang=\"en\"><head><meta charset=\"utf-8\">\n"
     "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n"
     "<title>cloud-itonami-isic-4950 &middot; Transport via pipeline &mdash; Operator Console</title>\n"
     "<style>\n" (jp-go-dds.skin/dds+skin) "\n</style></head>\n"
     "<body>\n"
     "<header class=\"bar\">\n"
     "  <h1>Transport via pipeline (ISIC 4950) &mdash; Operator Console</h1>\n"
     "</header>\n"
     "<p class=\"subtitle\">\n"
     "  <span class=\"badge\">read-only sample</span>\n"
     "  <span class=\"badge\">governor: :pipeline-integrity-governor</span>\n"
     "  <span class=\"badge\">phase " (esc phase/default-phase) " &middot; "
     (esc (:label (get phase/phases phase/default-phase))) "</span>\n"
     "  <span class=\"badge\">" (esc (count holds)) " HARD holds &middot; "
     (esc (count groups)) " distinct rules</span>\n"
     "</p>\n"
     "<p class=\"muted\">Generated at build time by <code>pipeline.render-html</code> "
     "(<code>clojure -M:dev:render-html</code>) from a real run of this repo's own actor stack "
     "&mdash; <code>pipeline.operation</code> on a langgraph-clj StateGraph, censored by "
     "<code>pipeline.governor</code>, gated by <code>pipeline.phase</code>, persisted to "
     "<code>pipeline.store</code>. Human approvals are real <code>interrupt-before</code> "
     "resumes. Nothing on this page is hand-typed; re-running the generator against the same "
     "seed produces a byte-identical document.</p>\n"
     "<main>\n"
     (batches-section db ledger)
     (ground-truth-section db)
     (action-gate-section audit)
     (holds-section ledger)
     (approval-section db audit)
     (records-section db)
     (jurisdictions-section)
     (ledger-section ledger)
     "</main>\n"
     "<footer>\n"
     "  <p>cloud-itonami-isic-4950 &middot; Transport via pipeline &middot; AGPL-3.0-or-later. "
     "This actor drafts records; it does not open a valve, start flow, or sign anything. "
     "<code>:batch/dispatch</code> and <code>:delivery/settle</code> are always a human "
     "pipeline controller's call.</p>\n"
     "</footer>\n"
     "</body></html>\n")))

(defn -main [& args]
  (let [out    (or (first args) "docs/samples/operator-console.html")
        result (run-demo!)
        ledger (vec (store/ledger (:db result)))
        holds  (hold-facts ledger)
        groups (hold-rule-groups ledger)]
    ;; Build-time invariant, not a convention: a console that shows no HARD
    ;; hold misrepresents the governor as a rubber stamp. Fail the build
    ;; rather than publish that.
    (when (zero? (count holds))
      (throw (ex-info "no HARD governor hold in scenario - console would misrepresent the governor"
                      {:ledger-facts (count ledger)})))
    (when (zero? (count (granted-approvals (:audit result))))
      (throw (ex-info "no approval granted in scenario - approval attribution cannot be measured"
                      {:audit-facts (count (:audit result))})))
    (spit out (render result))
    (println "wrote" out
             (str "(" (count ledger) " ledger facts, "
                  (count holds) " HARD holds over " (count groups) " distinct rules, "
                  (count (store/dispatch-history (:db result))) " dispatch records, "
                  (count (store/delivery-history (:db result))) " delivery records)"))))
