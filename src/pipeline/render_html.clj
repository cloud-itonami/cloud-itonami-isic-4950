(ns pipeline.render-html
  "Build-time HTML renderer for `docs/samples/operator-console.html`.

  This namespace does NOT describe the actor -- it RUNS it. `run-demo!`
  drives the repo's real stack (`pipeline.operation`'s langgraph
  StateGraph -> `pipeline.governor` -> `pipeline.store`) over the real
  `pipeline.store/demo-data` seed, and every row rendered below is read
  back out of the resulting store / ledger / registry drafts. Nothing on
  the page is a hand-typed copy of what the actor is believed to do.

  What is derived, and from where:

    - batch rows            `store/all-pipeline-batches`
    - HARD-hold rows        `:governor-hold` facts on `store/ledger`
                            (rule + the governor's own Japanese detail)
    - phase rollout table   `pipeline.phase/phases` (the actual data)
    - op gate table         `pipeline.phase/write-ops`,
                            `pipeline.governor/high-stakes`,
                            `pipeline.governor/confidence-floor`
    - jurisdiction table    `pipeline.facts/catalog` + `facts/coverage`
    - registry drafts       `store/dispatch-history` / `delivery-history`
    - approver attribution  a render-time scan of the store's four
                            registers -- see `approver-attribution`
    - audit ledger          `store/ledger`

  The page is deterministic: no clock, no randomness, no network, no
  counter that is not itself a count of real facts. Two consecutive runs
  against the same seed are byte-identical.

  BUILD-TIME INVARIANT (not a comment -- `-main` throws): a console that
  shows no real HARD hold is not evidence of a governor, and a console
  with no batches is not evidence of a store. Both are refused.

  Usage: `clojure -M:dev:render-html [out-file]`
  (default `docs/samples/operator-console.html`)."
  (:require [jp-go-dds.skin]
            [clojure.string :as str]
            [langgraph.graph :as g]
            [pipeline.facts :as facts]
            [pipeline.governor :as governor]
            [pipeline.operation :as op]
            [pipeline.phase :as phase]
            [pipeline.registry :as registry]
            [pipeline.store :as store]))

;; ----------------------------- the real run -----------------------------

(def ^:private controller
  "The human pipeline controller who resumes an escalated run. Phase 3 is
  `pipeline.phase/default-phase` -- read from the data, not typed."
  {:actor-id "op-1" :actor-role :pipeline-controller :phase phase/default-phase})

(def ^:private phase-1-controller
  "Same operator, earlier rollout phase -- used once, to show that the
  phase gate holds a write the governor itself would have cleared."
  (assoc controller :phase 1))

(defn- exec!
  "One OperationActor run. Appends {:label .. :thread .. :request ..
  :context .. :result ..} to `runs` so the renderer can read the graph's
  own `:audit` channel (which carries facts the store never sees)."
  ([actor runs label tid request] (exec! actor runs label tid request controller))
  ([actor runs label tid request ctx]
   (let [r (g/run* actor {:request request :context ctx} {:thread-id tid})]
     (conj runs {:label label :thread tid :request request :context ctx :result r}))))

(defn- resume!
  "Resume a paused (escalated) run with a human decision."
  [actor runs label tid status by]
  (let [r (g/run* actor {:approval {:status status :by by}}
                  {:thread-id tid :resume? true})]
    (conj runs {:label label :thread tid :request nil :context controller
                :approval {:status status :by by} :result r})))

(def ^:private scenario
  "The scenario, as data. `[:run label thread-id request context?]` issues
  one request; `[:resume label thread-id status by]` is a human resuming a
  run the actor paused at its approval gate. Every `:subject` here is a
  batch id that exists in `pipeline.store/demo-data` -- there are no
  invented entities, and no step asserts its own outcome: what each one
  produces is whatever the real governor decides."
  [;; --- batch-1: the whole lifecycle, in an order that matters ---------
   [:run "intake batch-1 (clean, no capital risk)" "t1"
    {:op :batch/intake :subject "batch-1"
     :patch {:id "batch-1" :product-grade "JIS-K-2202-Diesel-CR"}}]

   ;; dispatch attempted BEFORE the segment integrity assessment exists
   [:run "dispatch batch-1 before any integrity assessment" "t2"
    {:op :batch/dispatch :subject "batch-1"}]

   ;; the first assessment escalates and a human DECLINES it
   [:run "verify batch-1" "t3" {:op :segment/verify :subject "batch-1"}]
   [:resume "verify batch-1 declined" "t3" :rejected "controller-akita-2"]

   ;; re-requested, and approved
   [:run "verify batch-1 (re-requested)" "t4" {:op :segment/verify :subject "batch-1"}]
   [:resume "verify batch-1 approved" "t4" :approved "controller-akita-1"]

   [:run "dispatch batch-1" "t5" {:op :batch/dispatch :subject "batch-1"}]
   [:resume "dispatch batch-1 approved" "t5" :approved "controller-akita-1"]

   [:run "settle delivery batch-1" "t6" {:op :delivery/settle :subject "batch-1"}]
   [:resume "settle delivery batch-1 approved" "t6" :approved "controller-akita-1"]

   ;; the double-actuation guards
   [:run "dispatch batch-1 a second time" "t7" {:op :batch/dispatch :subject "batch-1"}]
   [:run "settle delivery batch-1 a second time" "t8" {:op :delivery/settle :subject "batch-1"}]

   ;; --- batch-2: a jurisdiction with no spec-basis ---------------------
   [:run "verify batch-2 (jurisdiction ATL)" "t9" {:op :segment/verify :subject "batch-2"}]

   ;; --- batch-3..7: one isolated integrity failure mode each -----------
   [:run "verify batch-3" "t10" {:op :segment/verify :subject "batch-3"}]
   [:resume "verify batch-3 approved" "t10" :approved "controller-akita-1"]
   [:run "dispatch batch-3" "t11" {:op :batch/dispatch :subject "batch-3"}]

   [:run "verify batch-4" "t12" {:op :segment/verify :subject "batch-4"}]
   [:resume "verify batch-4 approved" "t12" :approved "controller-akita-1"]
   [:run "dispatch batch-4" "t13" {:op :batch/dispatch :subject "batch-4"}]

   [:run "verify batch-5" "t14" {:op :segment/verify :subject "batch-5"}]
   [:resume "verify batch-5 approved" "t14" :approved "controller-akita-1"]
   [:run "dispatch batch-5" "t15" {:op :batch/dispatch :subject "batch-5"}]

   [:run "verify batch-6" "t16" {:op :segment/verify :subject "batch-6"}]
   [:resume "verify batch-6 approved" "t16" :approved "controller-akita-1"]
   [:run "dispatch batch-6" "t17" {:op :batch/dispatch :subject "batch-6"}]

   [:run "verify batch-7" "t18" {:op :segment/verify :subject "batch-7"}]
   [:resume "verify batch-7 approved" "t18" :approved "controller-akita-1"]
   [:run "dispatch batch-7" "t19" {:op :batch/dispatch :subject "batch-7"}]

   ;; --- the rollout phase gate, independent of the governor ------------
   [:run "verify batch-1 re-issued at phase 1" "t20"
    {:op :segment/verify :subject "batch-1"} phase-1-controller]])

(defn run-demo!
  "Drives one full pipeline-transport scenario through the real actor.

  batch-1 walks the whole lifecycle and is also used to show that the
  order of the ops matters: a dispatch attempted BEFORE the segment
  integrity assessment exists HARD-holds on `:evidence-incomplete`; the
  first assessment is then DECLINED by a human (an escalation a human
  may refuse -- the only disposition on this page a human controls); the
  re-requested assessment is approved; the dispatch and the delivery each
  escalate on `:actuation` (never auto at any phase) and are approved;
  and repeating either actuation HARD-holds on the dedicated
  `:dispatched?` / `:delivered?` guard.

  batch-2..batch-7 each isolate exactly ONE further HARD-hold rule,
  matching `pipeline.store/demo-data`'s own one-failure-mode-per-batch
  discipline: no spec-basis (ATL is deliberately absent from
  `pipeline.facts/catalog`), line pressure outside its declared safe
  window, a broken prior-segment POD chain, an unresolved product
  contamination flag, a stale ILI/pigging/hydrotest integrity
  assessment, and unconfirmed bonding-and-grounding.

  Finally the same `:segment/verify` that committed at phase 3 is
  re-issued at phase 1, where the rollout gate -- not the governor --
  holds it as `:phase-disabled`.

  Returns {:db store :runs [..]}."
  []
  (let [db (store/seed-db)
        actor (op/build db)]
    {:db db
     :runs (reduce (fn [runs step]
                     (case (first step)
                       :run    (let [[_ label tid request ctx] step]
                                 (exec! actor runs label tid request (or ctx controller)))
                       :resume (let [[_ label tid status by] step]
                                 (resume! actor runs label tid status by))))
                   []
                   scenario)}))

;; ----------------------------- derived views -----------------------------

(defn- holds
  "Every HARD `:governor-hold` fact the run actually wrote to the ledger."
  [db]
  (filterv #(= :governor-hold (:t %)) (store/ledger db)))

(defn- rejections [db]
  (filterv #(= :approval-rejected (:t %)) (store/ledger db)))

(defn- commits [db]
  (filterv #(= :committed (:t %)) (store/ledger db)))

(defn- audit-facts
  "All `:audit`-channel facts of a given `:t` across every run -- the graph
  emits some facts (notably `:approval-granted`, which carries the human
  approver's id) that the store's ledger never receives."
  [runs t]
  (for [r runs
        f (get-in r [:result :state :audit])
        :when (= t (:t f))]
    f))

(def ^:private approver-key-names
  "Key names that would carry a HUMAN APPROVER's identity. `:actor` is
  deliberately absent -- that is the requesting actor id (`op-1`), which
  is present on every committed fact whether or not anyone approved it,
  so counting it would manufacture an attribution that does not exist."
  #{"approved-by" "approved_by" "approver" "approved_by_id" "approved-by-id"})

(defn- approver-key? [k]
  (contains? approver-key-names (if (keyword? k) (name k) (str k))))

(defn- scan-register
  "Does any map in `maps` carry an approver key? Returns
  {:register .. :scanned n :found? bool :sample-keys ..}. `:scanned 0` is
  reported as-is and never as `found? false` prose -- an empty register
  proves nothing either way."
  [register maps]
  (let [maps (filterv map? maps)]
    {:register register
     :scanned  (count maps)
     :found?   (boolean (some #(some approver-key? (keys %)) maps))}))

(defn- approver-attribution
  "DERIVED, at render time, from the store this run actually produced --
  never a hardcoded claim about the code.

  `pipeline.operation`'s `:request-approval` node attaches the human
  approver at `[:payload :approved-by]` on the record it hands to
  `store/commit-record!`. Whether that survives depends entirely on which
  branch of `commit-record!` the record's `:effect` selects, so the only
  honest way to state it is to look: each of the store's four registers
  is scanned for an approver key, and the approver ids this run actually
  produced are read off the graph's own `:approval-granted` audit facts.

  If someone later fixes (or breaks) a branch of `commit-record!`, this
  page changes with it."
  [db runs]
  (let [batches (vec (store/all-pipeline-batches db))
        assessments (vec (keep #(store/segment-assessment-of db (:id %)) batches))
        drafts (vec (concat (store/dispatch-history db) (store/delivery-history db)))
        ledger (vec (store/ledger db))
        registers [(scan-register ":pipeline-batches (batch records)" batches)
                   (scan-register ":segment-assessments (integrity checklists)" assessments)
                   (scan-register "dispatch/delivery registry drafts" drafts)
                   (scan-register "append-only audit ledger" ledger)]]
    {:registers registers
     :approvers (vec (sort (distinct (keep :by (audit-facts runs :approval-granted)))))
     :decliners (vec (sort (distinct (keep :by (audit-facts runs :approval-rejected)))))
     :retaining (filterv :found? registers)
     :dropping  (filterv #(and (not (:found? %)) (pos? (:scanned %))) registers)}))

;; ----------------------------- html helpers -----------------------------

(defn- esc [v]
  (-> (str v)
      (str/replace "&" "&amp;")
      (str/replace "<" "&lt;")
      (str/replace ">" "&gt;")))

(defn- code [v] (str "<code>" (esc v) "</code>"))
(defn- kw [v] (if (keyword? v) (code v) (esc v)))
(defn- ok [s] (str "<span class=\"ok\">" s "</span>"))
(defn- warn [s] (str "<span class=\"warn\">" s "</span>"))
(defn- err [s] (str "<span class=\"err\">" s "</span>"))
(defn- crit [s] (str "<span class=\"critical\">" s "</span>"))
(defn- muted [s] (str "<span class=\"muted\">" s "</span>"))
(defn- yn [b] (if b (ok "yes") (err "no")))
(defn- audit-only [] "<strong>audit only &mdash; not retained in the record</strong>")

(defn- row [& cells]
  (str "        <tr>" (str/join (map #(str "<td>" % "</td>") cells)) "</tr>"))

(defn- table [headers rows]
  (str "    <table>\n"
       "      <thead><tr>" (str/join (map #(str "<th>" % "</th>") headers)) "</tr></thead>\n"
       "      <tbody>\n"
       (str/join "\n" rows) "\n"
       "      </tbody>\n"
       "    </table>\n"))

(defn- section [title lede & body]
  (str "  <section class=\"card\">\n"
       "    <h2>" title "</h2>\n"
       (when lede (str "    <p class=\"muted\">" lede "</p>\n"))
       (str/join body)
       "  </section>\n"))

;; ----------------------------- sections -----------------------------

(defn- summary-section [db runs]
  (let [hs (holds db)
        rule-freq (frequencies (mapcat :basis hs))]
    (section
     "This run"
     (str "Every number below is a count of facts this build actually produced by running "
          (code 'pipeline.operation) " over " (code 'pipeline.store/demo-data)
          " &mdash; not a claimed capability. No usage, revenue or performance metric appears anywhere on this page.")
     (table ["Measure" "Count"]
            [(row "actor graph runs (requests + resumes)" (str "<span class=\"num\">" (count runs) "</span>"))
             (row "append-only ledger facts" (str "<span class=\"num\">" (count (store/ledger db)) "</span>"))
             (row "committed ops" (str "<span class=\"num\">" (count (commits db)) "</span>"))
             (row (str "governor " (crit "HARD holds")) (str "<span class=\"num\">" (count hs) "</span>"))
             (row "distinct HARD rules exercised" (str "<span class=\"num\">" (count rule-freq) "</span>"))
             (row "escalations declined by a human" (str "<span class=\"num\">" (count (rejections db)) "</span>"))
             (row "pipeline batches in the SSoT" (str "<span class=\"num\">" (count (store/all-pipeline-batches db)) "</span>"))
             (row "committed dispatch drafts" (str "<span class=\"num\">" (count (store/dispatch-history db)) "</span>"))
             (row "committed delivery drafts" (str "<span class=\"num\">" (count (store/delivery-history db)) "</span>"))]))))

(defn- last-fact-for [ledger subject]
  (last (filter #(= subject (:subject %)) ledger)))

(defn- status-cell [ledger id]
  (let [f (last-fact-for ledger id)]
    (cond
      (nil? f) (muted "no activity")
      (= :committed (:t f)) (ok (str "committed &middot; " (esc (name (:op f)))))
      (= :approval-rejected (:t f)) (warn "declined by controller")
      (= :governor-hold (:t f))
      (if-let [rule (first (:basis f))]
        (crit (str "HARD hold &middot; " (esc (name rule))))
        (err (str "phase hold &middot; " (esc (name (:phase-reason f :held))))))
      :else (muted "in progress"))))

(defn- lifecycle-cell [{:keys [dispatched? delivered?]}]
  (cond
    delivered?  (ok "dispatched &amp; delivered")
    dispatched? (warn "dispatched, delivery not settled")
    :else       (muted "in intake")))

(defn- pressure-cell [{:keys [line-pressure-mpa-actual line-pressure-mpa-min line-pressure-mpa-max]}]
  ;; the SAME pure function the governor calls -- not a re-implementation
  (let [bad? (registry/line-pressure-out-of-range?
              line-pressure-mpa-actual line-pressure-mpa-min line-pressure-mpa-max)]
    (str "<span class=\"num\">" (esc line-pressure-mpa-actual) "</span> MPa "
         (muted (str "&nbsp;window [" (esc line-pressure-mpa-min) ", "
                     (esc line-pressure-mpa-max) "]"))
         " " (if bad? (crit "outside") (ok "inside")))))

(defn- flags-cell [{:keys [prior-segment-pod-confirmed? integrity-assessment-current?
                          contamination-flag-raised? contamination-flag-resolved?
                          bonding-grounding-confirmed?]}]
  (str/join
   "<br>"
   [(str "POD chain " (yn prior-segment-pod-confirmed?))
    (str "ILI/pigging/hydrotest current " (yn integrity-assessment-current?))
    (str "contamination "
         (cond (not contamination-flag-raised?) (ok "no flag")
               contamination-flag-resolved?     (ok "resolved")
               :else                            (err "unresolved")))
    (str "bonding &amp; grounding " (yn bonding-grounding-confirmed?))]))

(defn- batches-section [db]
  (let [ledger (vec (store/ledger db))
        batches (vec (store/all-pipeline-batches db))]
    (section
     "Pipeline batches (SSoT)"
     (str "Read back from " (code 'pipeline.store) " after the run. Terminals, product grade, "
          "volume, the declared safe pressure window and every custody/integrity flag come from "
          (code 'pipeline.store/demo-data) "; the dispatch and delivery numbers were minted by "
          (code 'pipeline.registry) " during the run and are jurisdiction-scoped sequences, not "
          "invented tracking numbers.")
     (table ["Batch" "Batch id" "Route" "Product grade" "Volume (bbl)" "Juris."
             "Line pressure" "Custody &amp; integrity" "Lifecycle" "Registry numbers" "Last op"]
            (for [{:keys [id batch-id origin-terminal destination-terminal product-grade
                          volume-barrels jurisdiction dispatch-number delivery-number] :as b} batches]
              (row (code id)
                   (esc batch-id)
                   (str (esc origin-terminal) " &rarr; " (esc destination-terminal))
                   (esc product-grade)
                   (str "<span class=\"num\">" (esc volume-barrels) "</span>")
                   (esc jurisdiction)
                   (pressure-cell b)
                   (flags-cell b)
                   (lifecycle-cell b)
                   (if (or dispatch-number delivery-number)
                     (str/join "<br>" (remove nil? [(when dispatch-number (code dispatch-number))
                                                    (when delivery-number (code delivery-number))]))
                     (muted "&mdash;"))
                   (status-cell ledger id)))))))

(defn- holds-section [db]
  (let [hs (holds db)
        rjs (rejections db)]
    (section
     "Governor HARD holds this run"
     (str "Each row is a real " (code :governor-hold) " fact on the append-only ledger, with the "
          "rule and the detail string the governor itself wrote. A HARD hold is un-overridable and "
          "never reaches a human approver &mdash; there is no button on this page that clears one. "
          "The one " (warn "phase hold") " below came from " (code 'pipeline.phase/gate)
          ", not the governor: an op the governor had cleared, held because the rollout phase does "
          "not enable that write yet.")
     (table ["Op" "Batch" "Rule" "Why the governor refused"]
            (concat
             (for [{:keys [op subject basis violations phase-reason phase]} hs]
               (row (code op)
                    (code subject)
                    (if-let [r (first basis)]
                      (crit (esc (name r)))
                      (err (str (esc (name (or phase-reason :held)))
                                (when phase (str " @ phase " phase)))))
                    (if-let [d (:detail (first violations))]
                      (esc d)
                      (muted (str "rollout phase " phase " does not enable this write ("
                                  (esc (name (or phase-reason :held))) ")")))))
             (for [{:keys [op subject]} rjs]
               (row (code op)
                    (code subject)
                    (warn "approver-rejected")
                    (str (muted "not a governor rule &mdash; ")
                         "the governor cleared this proposal and a human controller declined it at the approval gate."))))))))

(defn- rule-coverage-section [db]
  (let [freq (frequencies (mapcat :basis (holds db)))]
    (section
     "HARD rules actually exercised"
     (str "Derived by counting " (code :basis) " values across the run's " (code :governor-hold)
          " facts. A rule that did not fire is not listed &mdash; this table reports what was "
          "observed, not what is implemented.")
     (table ["Rule" "Times fired"]
            (for [[rule n] (sort-by (comp name key) freq)]
              (row (code rule) (str "<span class=\"num\">" n "</span>")))))))

(defn- gate-section []
  (let [p3 (get phase/phases phase/default-phase)]
    (section
     "Action gate &amp; rollout phases"
     (str "Read directly out of " (code 'pipeline.phase/phases) ", "
          (code 'pipeline.governor/high-stakes) " and " (code 'pipeline.governor/confidence-floor)
          " &mdash; this is the data the running actor consults, rendered, not a description of it. "
          "The confidence floor is " (code governor/confidence-floor) "; below it the governor escalates.")
     (table ["Op" (str "Writable at phase " phase/default-phase "?")
             (str "Auto-commit at phase " phase/default-phase "?") "Permanently high-stakes?"]
            (for [o (sort-by str phase/write-ops)]
              (row (code o)
                   (yn (contains? (:writes p3) o))
                   (if (contains? (:auto p3) o)
                     (ok "auto when governor-clean")
                     (warn "human approval required"))
                   (if (contains? governor/high-stakes o)
                     (crit "yes &middot; never auto at any phase")
                     (muted "no")))))
     (table ["Phase" "Label" "Writes enabled" "Auto-commit when clean"]
            (for [[n {:keys [label writes auto]}] (sort-by key phase/phases)]
              (row (str "<span class=\"num\">" n "</span>"
                        (when (= n phase/default-phase) (str " " (ok "current"))))
                   (esc label)
                   (if (seq writes) (str/join " " (map code (sort-by str writes))) (muted "none"))
                   (if (seq auto) (str/join " " (map code (sort-by str auto))) (muted "none"))))))))

(defn- jurisdiction-section [db]
  (let [seeded (vec (sort (distinct (map :jurisdiction (store/all-pipeline-batches db)))))
        cov (facts/coverage seeded)]
    (section
     "Jurisdiction spec-basis catalog"
     (str "From " (code 'pipeline.facts/catalog) ". A jurisdiction absent from this table has NO "
          "spec-basis: the advisor must not invent one and the governor HARD-holds if it tries. "
          "Coverage over the jurisdictions this run's batches actually carry: "
          "<strong>" (:covered cov) " of " (:requested cov) "</strong> covered"
          (when (seq (:missing-jurisdictions cov))
            (str ", missing " (str/join " " (map code (:missing-jurisdictions cov))))) ".")
     (table ["ISO3" "Authority" "Legal basis" "Required evidence" "Provenance" "In this run"]
            (concat
             (for [[iso3 {:keys [owner-authority legal-basis required-evidence provenance]}]
                   (sort-by key facts/catalog)]
               (row (code iso3)
                    (esc owner-authority)
                    (esc legal-basis)
                    (str/join "<br>" (map esc required-evidence))
                    (str "<a href=\"" (esc provenance) "\">" (esc provenance) "</a>")
                    (if (contains? (set seeded) iso3) (ok "yes") (muted "no"))))
             (for [iso3 (:missing-jurisdictions cov)]
               (row (code iso3)
                    (crit "no entry")
                    (crit "no entry")
                    (err "cannot be determined &mdash; never fabricated")
                    (muted "&mdash;")
                    (ok "yes")))))
     (str "    <p class=\"muted\">" (esc (:note cov)) "</p>\n"))))

(defn- drafts-section [db]
  (let [ds (vec (store/dispatch-history db))
        vs (vec (store/delivery-history db))]
    (section
     "Committed registry drafts"
     (str "Built by " (code 'pipeline.registry) " at commit time. Every certificate this actor "
          "produces is " (code "status: draft-unsigned") " with " (code "issued_by_registry: false")
          " &mdash; signing is the operator's act, not the actor's. Only batches that cleared both "
          "the governor and a human approver appear here.")
     (table ["Kind" "Record id" "Batch" "Jurisdiction" "Immutable"]
            (if (seq (concat ds vs))
              (for [r (concat ds vs)]
                (row (esc (get r "kind"))
                     (code (get r "record_id"))
                     (code (get r "pipeline_batch_id"))
                     (esc (get r "jurisdiction"))
                     (yn (get r "immutable"))))
              [(row (muted "no draft was committed in this run") "" "" "" "")])))))

(defn- approver-section [db runs]
  (let [{:keys [registers approvers decliners retaining dropping]} (approver-attribution db runs)]
    (section
     "Approver attribution &mdash; what the SSoT does and does not hold"
     (str "This section is DERIVED at render time, not asserted. "
          (code 'pipeline.operation) " attaches the human approver at "
          (code [:payload :approved-by]) " on the record it hands to "
          (code 'pipeline.store/commit-record!) ", but each branch of that function chooses for "
          "itself whether to read " (code :payload) ". So the page scans all four registers of the "
          "store this run produced and reports what it finds. If a branch is later fixed or broken, "
          "this table changes with it.")
     (table ["Store register" "Records scanned" "Approver key present?"]
            (for [{:keys [register scanned found?]} registers]
              (row (esc register)
                   (str "<span class=\"num\">" scanned "</span>")
                   (cond (zero? scanned) (muted "no records &mdash; nothing to conclude")
                         found?          (ok "yes")
                         :else           (err "no")))))
     (table ["From the graph's own audit channel" "Value"]
            [(row (str "controllers who approved (" (code :approval-granted) ")")
                  (if (seq approvers) (str/join " " (map code approvers)) (muted "none")))
             (row (str "controllers who declined (" (code :approval-rejected) ")")
                  (if (seq decliners) (str/join " " (map code decliners)) (muted "none")))])
     (str "    <p>"
          (cond
            (empty? approvers)
            "This run produced no human approval, so there is no approver to attribute."

            (and (seq retaining) (empty? dropping))
            (str (ok "Every non-empty register retains the approver.")
                 " The identity of the human who authorised each act is recoverable from the SSoT alone.")

            (seq retaining)
            (str (warn "Partial retention.")
                 " The approver survives into "
                 (str/join ", " (map #(str "<strong>" (esc (:register %)) "</strong>") retaining))
                 ", but is <strong>not</strong> present in "
                 (str/join ", " (map #(str "<strong>" (esc (:register %)) "</strong>") dropping))
                 ". Those branches of " (code 'pipeline.store/commit-record!)
                 " re-derive their patch from the store and never read " (code :payload)
                 ", so the approver is dropped on the way in. For those acts the approver above is "
                 (audit-only) ".")

            :else
            (str (err "The approver reaches no register.")
                 " Every value above is " (audit-only)
                 " &mdash; a reader of the SSoT alone cannot distinguish &ldquo;nobody approved&rdquo; "
                 "from &ldquo;the store dropped it&rdquo;."))
          "</p>\n"))))


(defn- ledger-section [db]
  (section
   "Append-only audit ledger"
   (str "The complete " (code 'pipeline.store/ledger) " this run produced, in commit order. "
        "Every proposal that committed, every proposal the governor refused, and every escalation a "
        "human declined &mdash; the trail a regulator or a custody counterparty would query if a "
        "dispatch or a delivery is later disputed.")
   (table ["#" "Fact" "Op" "Batch" "Disposition" "Basis / rules"]
          (map-indexed
           (fn [i {:keys [t op subject disposition basis]}]
             (row (str "<span class=\"num\">" (inc i) "</span>")
                  (case t
                    :committed (ok (code t))
                    :governor-hold (crit (code t))
                    :approval-rejected (warn (code t))
                    (kw t))
                  (code op)
                  (code subject)
                  (kw disposition)
                  (if (seq basis)
                    (str/join " " (map #(code (if (keyword? %) % (str %))) basis))
                    (muted "&mdash;"))))
           (store/ledger db)))))

;; ----------------------------- document -----------------------------

(defn render
  "Renders the whole document from a `run-demo!` result."
  [{:keys [db runs]}]
  (str
   "<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">\n"
   "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n"
   "<title>cloud-itonami-isic-4950 &middot; pipeline transport &mdash; operator console</title>\n"
   "<style>\n" (jp-go-dds.skin/dds+skin) "\n</style>\n"
   "</head>\n<body>\n"
   "<header class=\"bar\">\n"
   "  <h1>Transport via pipeline (ISIC 4950) &mdash; Operator Console</h1>\n"
   "  <p class=\"badge\">read-only sample &middot; governor-gated &middot; batch dispatch and delivery settlement are always a human pipeline controller&rsquo;s call</p>\n"
   "</header>\n<main>\n"
   (summary-section db runs)
   (batches-section db)
   (holds-section db)
   (rule-coverage-section db)
   (gate-section)
   (jurisdiction-section db)
   (drafts-section db)
   (approver-section db runs)
   (ledger-section db)
   "</main>\n<footer>\n"
   "Generated at build time by <code>pipeline.render-html</code> (<code>clojure -M:dev:render-html</code>) "
   "by driving the real <code>pipeline.operation</code> actor graph over the real "
   "<code>pipeline.store</code> seed. Deterministic &mdash; no clock, no randomness, no network. "
   "The build refuses to write this file if the run produces no HARD governor hold.\n"
   "</footer>\n</body>\n</html>\n"))

(defn -main [& args]
  (let [out (or (first args) "docs/samples/operator-console.html")
        {:keys [db] :as result} (run-demo!)
        hs (holds db)
        batches (store/all-pipeline-batches db)]
    ;; Build-time invariants. A console that shows no real HARD hold is not
    ;; evidence of a governor; a console with no batches is not evidence of
    ;; a store. Refuse to write either rather than emit a plausible page.
    (when (empty? hs)
      (throw (ex-info "no :governor-hold fact on the ledger -- refusing to write a console that shows no real HARD hold"
                      {:ledger-facts (count (store/ledger db))
                       :runs (count (:runs result))})))
    (when (empty? batches)
      (throw (ex-info "no pipeline batches in the store -- refusing to write a console with no entities"
                      {:ledger-facts (count (store/ledger db))})))
    (let [f (java.io.File. ^String out)]
      (when-let [p (.getParentFile f)] (.mkdirs p))
      (spit f (render result)))
    (println "wrote" out
             (str "(" (count batches) " batches, "
                  (count (store/ledger db)) " ledger facts, "
                  (count hs) " HARD holds over "
                  (count (frequencies (mapcat :basis hs))) " distinct rules, "
                  (count (:runs result)) " actor runs)"))))
