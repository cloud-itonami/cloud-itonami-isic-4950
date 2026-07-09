(ns pipeline.store
  "SSoT for the pipeline-transport actor, behind a `Store` protocol so
  the backend is a swap, not a rewrite -- the same seam every prior
  `cloud-itonami-isic-*` actor in this fleet uses.

    - `MemStore`     -- atom of EDN. The deterministic default for
                        dev/tests/demo (no deps).
    - `DatomicStore` -- backed by `langchain.db`, a Datomic-API-compatible
                        EAV store (datalog q / pull / upsert). Pure `.cljc`,
                        so it runs offline AND can be pointed at a real
                        Datomic Local or a kotoba-server pod by swapping
                        `langchain.db`'s `:db-api` (see langchain.kotoba-db).

  Both implement the same protocol and pass the same contract
  (test/pipeline/store_contract_test.clj), which is the whole point:
  the actor, the Pipeline Integrity Governor and the audit ledger never
  know which SSoT they run on.

  Unlike `retailops`/4711's own `order` entity (distinguished by
  `:kind`), this vertical's `dispatch` and `settle` actuation events
  apply SEQUENTIALLY to the SAME `pipeline-batch` -- a batch dispatch
  happens first (flow started into the line), delivery settlement
  happens later (custody transfer at the destination terminal), on the
  same batch record. This matches the repair-shop cluster's own
  `ticket` shape more closely (two real-world acts, in order, on one
  entity), with dedicated double-actuation-guard booleans
  (`:dispatched?`/`:delivered?`, never a `:status` value).

  The ledger stays append-only on every backend: 'which batch was
  screened for a line pressure outside its safe window, a broken prior-
  segment POD chain, an unresolved contamination flag, a stale integrity
  assessment, or unconfirmed bonding/grounding, which batch had been
  dispatched, which delivery was settled, on what jurisdictional basis,
  approved by whom' is always a query over an immutable log -- the audit
  trail a regulator, a custody counterparty, or an operator trusting a
  pipeline-transport actor needs, and the evidence an operator needs if
  a dispatch or a delivery is later disputed."
  (:require #?(:clj  [clojure.edn :as edn]
               :cljs [cljs.reader :as edn])
            [pipeline.registry :as registry]
            [langchain.db :as d]))

(defprotocol Store
  (pipeline-batch [s id])
  (all-pipeline-batches [s])
  (segment-assessment-of [s pipeline-batch-id] "committed segment-integrity assessment, or nil")
  (ledger [s])
  (dispatch-history [s] "the append-only batch-dispatch history (pipeline.registry drafts)")
  (delivery-history [s] "the append-only batch-delivery history (pipeline.registry drafts)")
  (next-dispatch-sequence [s jurisdiction] "next dispatch-number sequence for a jurisdiction")
  (next-delivery-sequence [s jurisdiction] "next delivery-number sequence for a jurisdiction")
  (pipeline-batch-already-dispatched? [s pipeline-batch-id] "has this batch already been dispatched?")
  (pipeline-batch-already-delivered? [s pipeline-batch-id] "has this batch's delivery already been settled?")
  (commit-record! [s record] "apply a committed op's record to the SSoT")
  (append-ledger! [s fact]   "append one immutable decision fact")
  (with-pipeline-batches [s batches] "replace/seed the batch directory (map id->batch)"))

;; ----------------------------- demo data -----------------------------

(defn demo-data
  "A small, self-contained pipeline-batch set covering both actuation
  lifecycles (dispatch, delivery) plus the governor's own pipeline-
  integrity checks, so the actor + tests run offline. Each violation
  batch isolates exactly ONE failure mode (the rest stay clean)
  following the 'exercise the failure mode directly, never only via a
  happy-path actuation' discipline every sibling governor's demo data
  establishes."
  []
  {:pipeline-batches
   {"batch-1" {:id "batch-1" :batch-id "PKT-2026-0001"
               :origin-terminal "Akita-Onsen Terminal" :destination-terminal "Refinery-Akita"
               :product-grade "JIS-K-2202-Diesel-CR" :volume-barrels 50000
               :line-pressure-mpa-actual 8.0
               :line-pressure-mpa-min 6.0 :line-pressure-mpa-max 10.0
               :prior-segment-pod-confirmed? true
               :integrity-assessment-current? true
               :contamination-flag-raised? false :contamination-flag-resolved? false
               :bonding-grounding-confirmed? true
               :dispatched? false :delivered? false
               :jurisdiction "JPN" :status :intake}
    "batch-2" {:id "batch-2" :batch-id "PKT-2026-0002"
               :origin-terminal "Atlantis Terminal" :destination-terminal "Atlantis Refinery"
               :product-grade "JIS-K-2202-Diesel-CR" :volume-barrels 50000
               :line-pressure-mpa-actual 8.0
               :line-pressure-mpa-min 6.0 :line-pressure-mpa-max 10.0
               :prior-segment-pod-confirmed? true
               :integrity-assessment-current? true
               :contamination-flag-raised? false :contamination-flag-resolved? false
               :bonding-grounding-confirmed? true
               :dispatched? false :delivered? false
               :jurisdiction "ATL" :status :intake}
    "batch-3" {:id "batch-3" :batch-id "PKT-2026-0003"
               :origin-terminal "Akita-Onsen Terminal" :destination-terminal "Refinery-Akita"
               :product-grade "JIS-K-2202-Diesel-CR" :volume-barrels 50000
               :line-pressure-mpa-actual 12.0
               :line-pressure-mpa-min 6.0 :line-pressure-mpa-max 10.0
               :prior-segment-pod-confirmed? true
               :integrity-assessment-current? true
               :contamination-flag-raised? false :contamination-flag-resolved? false
               :bonding-grounding-confirmed? true
               :dispatched? false :delivered? false
               :jurisdiction "JPN" :status :intake}
    "batch-4" {:id "batch-4" :batch-id "PKT-2026-0004"
               :origin-terminal "Akita-Onsen Terminal" :destination-terminal "Refinery-Akita"
               :product-grade "JIS-K-2202-Diesel-CR" :volume-barrels 50000
               :line-pressure-mpa-actual 8.0
               :line-pressure-mpa-min 6.0 :line-pressure-mpa-max 10.0
               :prior-segment-pod-confirmed? false
               :integrity-assessment-current? true
               :contamination-flag-raised? false :contamination-flag-resolved? false
               :bonding-grounding-confirmed? true
               :dispatched? false :delivered? false
               :jurisdiction "JPN" :status :intake}
    "batch-5" {:id "batch-5" :batch-id "PKT-2026-0005"
               :origin-terminal "Akita-Onsen Terminal" :destination-terminal "Refinery-Akita"
               :product-grade "JIS-K-2202-Diesel-CR" :volume-barrels 50000
               :line-pressure-mpa-actual 8.0
               :line-pressure-mpa-min 6.0 :line-pressure-mpa-max 10.0
               :prior-segment-pod-confirmed? true
               :integrity-assessment-current? true
               :contamination-flag-raised? true :contamination-flag-resolved? false
               :bonding-grounding-confirmed? true
               :dispatched? false :delivered? false
               :jurisdiction "JPN" :status :intake}
    "batch-6" {:id "batch-6" :batch-id "PKT-2026-0006"
               :origin-terminal "Akita-Onsen Terminal" :destination-terminal "Refinery-Akita"
               :product-grade "JIS-K-2202-Diesel-CR" :volume-barrels 50000
               :line-pressure-mpa-actual 8.0
               :line-pressure-mpa-min 6.0 :line-pressure-mpa-max 10.0
               :prior-segment-pod-confirmed? true
               :integrity-assessment-current? false
               :contamination-flag-raised? false :contamination-flag-resolved? false
               :bonding-grounding-confirmed? true
               :dispatched? false :delivered? false
               :jurisdiction "JPN" :status :intake}
    "batch-7" {:id "batch-7" :batch-id "PKT-2026-0007"
               :origin-terminal "Akita-Onsen Terminal" :destination-terminal "Refinery-Akita"
               :product-grade "JIS-K-2202-Diesel-CR" :volume-barrels 50000
               :line-pressure-mpa-actual 8.0
               :line-pressure-mpa-min 6.0 :line-pressure-mpa-max 10.0
               :prior-segment-pod-confirmed? true
               :integrity-assessment-current? true
               :contamination-flag-raised? false :contamination-flag-resolved? false
               :bonding-grounding-confirmed? false
               :dispatched? false :delivered? false
               :jurisdiction "JPN" :status :intake}}})

;; ----------------------------- shared commit logic -----------------------------

(defn- dispatch-batch!
  "Backend-agnostic `:batch/mark-dispatched` -- looks up the batch via the
  protocol and drafts the batch-dispatch record, and returns {:result ..
  :batch-patch ..} for the caller to persist."
  [s pipeline-batch-id]
  (let [b (pipeline-batch s pipeline-batch-id)
        seq-n (next-dispatch-sequence s (:jurisdiction b))
        result (registry/register-dispatch-record pipeline-batch-id (:jurisdiction b) seq-n)]
    {:result result
     :batch-patch {:dispatched? true
                   :dispatch-number (get result "dispatch_number")}}))

(defn- deliver-batch!
  "Backend-agnostic `:batch/mark-delivered` -- looks up the batch via the
  protocol and drafts the batch-delivery record, and returns {:result ..
  :batch-patch ..} for the caller to persist."
  [s pipeline-batch-id]
  (let [b (pipeline-batch s pipeline-batch-id)
        seq-n (next-delivery-sequence s (:jurisdiction b))
        result (registry/register-delivery-record pipeline-batch-id (:jurisdiction b) seq-n)]
    {:result result
     :batch-patch {:delivered? true
                   :delivery-number (get result "delivery_number")}}))

;; ----------------------------- MemStore (default) -----------------------------

(defrecord MemStore [a]
  Store
  (pipeline-batch [_ id] (get-in @a [:pipeline-batches id]))
  (all-pipeline-batches [_] (sort-by :id (vals (:pipeline-batches @a))))
  (segment-assessment-of [_ pipeline-batch-id] (get-in @a [:segment-assessments pipeline-batch-id]))
  (ledger [_] (:ledger @a))
  (dispatch-history [_] (:dispatches @a))
  (delivery-history [_] (:deliveries @a))
  (next-dispatch-sequence [_ jurisdiction] (get-in @a [:dispatch-sequences jurisdiction] 0))
  (next-delivery-sequence [_ jurisdiction] (get-in @a [:delivery-sequences jurisdiction] 0))
  (pipeline-batch-already-dispatched? [_ pipeline-batch-id] (boolean (get-in @a [:pipeline-batches pipeline-batch-id :dispatched?])))
  (pipeline-batch-already-delivered? [_ pipeline-batch-id] (boolean (get-in @a [:pipeline-batches pipeline-batch-id :delivered?])))
  (commit-record! [s {:keys [effect path value payload]}]
    (case effect
      :batch/upsert
      (swap! a update-in [:pipeline-batches (:id value)] merge value)

      :segment-assessment/set
      (swap! a assoc-in [:segment-assessments (first path)] payload)

      :batch/mark-dispatched
      (let [pipeline-batch-id (first path)
            {:keys [result batch-patch]} (dispatch-batch! s pipeline-batch-id)
            jurisdiction (:jurisdiction (pipeline-batch s pipeline-batch-id))]
        (swap! a (fn [state]
                   (-> state
                       (update-in [:dispatch-sequences jurisdiction] (fnil inc 0))
                       (update-in [:pipeline-batches pipeline-batch-id] merge batch-patch)
                       (update :dispatches registry/append result))))
        result)

      :batch/mark-delivered
      (let [pipeline-batch-id (first path)
            {:keys [result batch-patch]} (deliver-batch! s pipeline-batch-id)
            jurisdiction (:jurisdiction (pipeline-batch s pipeline-batch-id))]
        (swap! a (fn [state]
                   (-> state
                       (update-in [:delivery-sequences jurisdiction] (fnil inc 0))
                       (update-in [:pipeline-batches pipeline-batch-id] merge batch-patch)
                       (update :deliveries registry/append result))))
        result)
      nil)
    s)
  (append-ledger! [_ fact] (swap! a update :ledger conj fact) fact)
  (with-pipeline-batches [s batches] (when (seq batches) (swap! a assoc :pipeline-batches batches)) s))

(defn seed-db
  "A MemStore seeded with the demo batch set. The deterministic default."
  []
  (->MemStore (atom (assoc (demo-data)
                           :segment-assessments {}
                           :ledger [] :dispatch-sequences {} :dispatches []
                           :delivery-sequences {} :deliveries []))))

;; ----------------------------- DatomicStore (langchain.db) -----------------------------

(def ^:private schema
  "DataScript/Datomic-style schema: only constraint attrs are declared.
  Map/compound values (assessment payloads, ledger facts, dispatch/
  delivery records) are stored as EDN strings so `langchain.db`
  doesn't expand them into sub-entities -- the same convention every
  sibling actor's store uses."
  {:pipeline-batch/id                        {:db/unique :db.unique/identity}
   :segment-assessment/pipeline-batch-id     {:db/unique :db.unique/identity}
   :ledger/seq                               {:db/unique :db.unique/identity}
   :dispatch/seq                             {:db/unique :db.unique/identity}
   :delivery/seq                             {:db/unique :db.unique/identity}
   :dispatch-sequence/jurisdiction           {:db/unique :db.unique/identity}
   :delivery-sequence/jurisdiction           {:db/unique :db.unique/identity}})

(defn- enc [v] (pr-str v))
(defn- dec* [s] (when s (edn/read-string s)))

;; Every pipeline-batch field is stored as its own Datomic attr so a governor
;; pull reads the exact ground truth (no blob decode). Boolean fields
;; are coerced on read so a missing attr reads back as false (parity
;; with MemStore). [field-key tx-attr boolean?]
(def ^:private pipeline-batch-fields
  [[:id :pipeline-batch/id false]
   [:batch-id :pipeline-batch/batch-id false]
   [:origin-terminal :pipeline-batch/origin-terminal false]
   [:destination-terminal :pipeline-batch/destination-terminal false]
   [:product-grade :pipeline-batch/product-grade false]
   [:volume-barrels :pipeline-batch/volume-barrels false]
   [:line-pressure-mpa-actual :pipeline-batch/line-pressure-mpa-actual false]
   [:line-pressure-mpa-min :pipeline-batch/line-pressure-mpa-min false]
   [:line-pressure-mpa-max :pipeline-batch/line-pressure-mpa-max false]
   [:prior-segment-pod-confirmed? :pipeline-batch/prior-segment-pod-confirmed? true]
   [:integrity-assessment-current? :pipeline-batch/integrity-assessment-current? true]
   [:contamination-flag-raised? :pipeline-batch/contamination-flag-raised? true]
   [:contamination-flag-resolved? :pipeline-batch/contamination-flag-resolved? true]
   [:bonding-grounding-confirmed? :pipeline-batch/bonding-grounding-confirmed? true]
   [:dispatched? :pipeline-batch/dispatched? true]
   [:delivered? :pipeline-batch/delivered? true]
   [:jurisdiction :pipeline-batch/jurisdiction false]
   [:status :pipeline-batch/status false]
   [:dispatch-number :pipeline-batch/dispatch-number false]
   [:delivery-number :pipeline-batch/delivery-number false]])

(defn- batch->tx [b]
  (reduce (fn [tx [k attr _bool?]]
            (let [v (get b k)]
              (cond-> tx (some? v) (assoc attr v))))
          {:pipeline-batch/id (:id b)}
          pipeline-batch-fields))

(def ^:private pipeline-batch-pull (mapv second pipeline-batch-fields))

(defn- pull->batch [m]
  (when (:pipeline-batch/id m)
    (reduce (fn [b [k attr bool?]]
              (let [v (get m attr)]
                (cond
                  bool?        (assoc b k (boolean v))
                  (some? v)    (assoc b k v)
                  :else        b)))
            {:id (:pipeline-batch/id m)}
            pipeline-batch-fields)))

(defrecord DatomicStore [conn]
  Store
  (pipeline-batch [_ id]
    (pull->batch (d/pull (d/db conn) pipeline-batch-pull [:pipeline-batch/id id])))
  (all-pipeline-batches [_]
    (->> (d/q '[:find [?id ...] :where [?e :pipeline-batch/id ?id]] (d/db conn))
         (map #(pull->batch (d/pull (d/db conn) pipeline-batch-pull [:pipeline-batch/id %])))
         (sort-by :id)))
  (segment-assessment-of [_ pipeline-batch-id]
    (dec* (d/q '[:find ?p . :in $ ?bid
                :where [?a :segment-assessment/pipeline-batch-id ?bid] [?a :segment-assessment/payload ?p]]
              (d/db conn) pipeline-batch-id)))
  (ledger [_]
    (->> (d/q '[:find ?s ?f :where [?e :ledger/seq ?s] [?e :ledger/fact ?f]] (d/db conn))
         (sort-by first)
         (mapv (comp dec* second))))
  (dispatch-history [_]
    (->> (d/q '[:find ?s ?r :where [?e :dispatch/seq ?s] [?e :dispatch/record ?r]] (d/db conn))
         (sort-by first)
         (mapv (comp dec* second))))
  (delivery-history [_]
    (->> (d/q '[:find ?s ?r :where [?e :delivery/seq ?s] [?e :delivery/record ?r]] (d/db conn))
         (sort-by first)
         (mapv (comp dec* second))))
  (next-dispatch-sequence [_ jurisdiction]
    (or (d/q '[:find ?n . :in $ ?j
              :where [?e :dispatch-sequence/jurisdiction ?j] [?e :dispatch-sequence/next ?n]]
            (d/db conn) jurisdiction)
        0))
  (next-delivery-sequence [_ jurisdiction]
    (or (d/q '[:find ?n . :in $ ?j
              :where [?e :delivery-sequence/jurisdiction ?j] [?e :delivery-sequence/next ?n]]
            (d/db conn) jurisdiction)
        0))
  (pipeline-batch-already-dispatched? [s pipeline-batch-id]
    (boolean (:dispatched? (pipeline-batch s pipeline-batch-id))))
  (pipeline-batch-already-delivered? [s pipeline-batch-id]
    (boolean (:delivered? (pipeline-batch s pipeline-batch-id))))
  (commit-record! [s {:keys [effect path value payload]}]
    (case effect
      :batch/upsert
      (d/transact! conn [(batch->tx value)])

      :segment-assessment/set
      (d/transact! conn [{:segment-assessment/pipeline-batch-id (first path) :segment-assessment/payload (enc payload)}])

      :batch/mark-dispatched
      (let [pipeline-batch-id (first path)
            {:keys [result batch-patch]} (dispatch-batch! s pipeline-batch-id)
            jurisdiction (:jurisdiction (pipeline-batch s pipeline-batch-id))
            next-n (inc (next-dispatch-sequence s jurisdiction))]
        (d/transact! conn
                     [(batch->tx (assoc batch-patch :id pipeline-batch-id))
                      {:dispatch-sequence/jurisdiction jurisdiction :dispatch-sequence/next next-n}
                      {:dispatch/seq (count (dispatch-history s)) :dispatch/record (enc (get result "record"))}])
        result)

      :batch/mark-delivered
      (let [pipeline-batch-id (first path)
            {:keys [result batch-patch]} (deliver-batch! s pipeline-batch-id)
            jurisdiction (:jurisdiction (pipeline-batch s pipeline-batch-id))
            next-n (inc (next-delivery-sequence s jurisdiction))]
        (d/transact! conn
                     [(batch->tx (assoc batch-patch :id pipeline-batch-id))
                      {:delivery-sequence/jurisdiction jurisdiction :delivery-sequence/next next-n}
                      {:delivery/seq (count (delivery-history s)) :delivery/record (enc (get result "record"))}])
        result)
      nil)
    s)
  (append-ledger! [s fact]
    (d/transact! conn [{:ledger/seq (count (ledger s)) :ledger/fact (enc fact)}])
    fact)
  (with-pipeline-batches [s batches]
    (when (seq batches) (d/transact! conn (mapv batch->tx (vals batches)))) s))

(defn datomic-store
  "A DatomicStore (langchain.db backend) seeded from `data`
  ({:pipeline-batches ..}); empty when omitted."
  ([] (datomic-store {}))
  ([{:keys [pipeline-batches]}]
   (let [s (->DatomicStore (d/create-conn schema))]
     (with-pipeline-batches s pipeline-batches))))

(defn datomic-seed-db
  "A DatomicStore seeded with the demo batch set -- the Datomic-backed
  analog of `seed-db`, used to prove protocol parity."
  []
  (datomic-store (demo-data)))
