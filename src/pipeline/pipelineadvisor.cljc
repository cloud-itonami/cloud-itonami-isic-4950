(ns pipeline.pipelineadvisor
  "PipelineTransport advisor client -- the *contained intelligence node*
  for the pipeline-transport actor.

  It normalizes batch intake, drafts a per-jurisdiction pipeline-
  integrity / custody / bonding-grounding evidence checklist, drafts
  the batch-dispatch action, and drafts the batch-delivery (custody
  transfer) action. CRITICAL: it is a smart-but-untrusted advisor. It
  returns a *proposal* (with a rationale + the fields it cited), never
  a committed record or a real dispatch/delivery. Every output is
  censored downstream by `pipeline.governor` before anything touches
  the SSoT, and `:batch/dispatch`/`:delivery/settle` proposals NEVER
  auto-commit at any phase -- see README `Actuation`.

  Like every sibling actor's advisor, this is a deterministic mock so
  the actor graph runs offline and the governor contract is exercised
  end-to-end. In production this calls a real LLM (kotoba-llm or
  equivalent) with the same proposal shape.

  Proposal shape (all kinds):
    {:summary    str            ; human-facing draft / finding
     :rationale  str            ; why -- SCANNED by the spec-basis gate
     :cites      [kw|str ..]    ; facts/sources the LLM used -- SCANNED too
     :effect     kw             ; how a commit would mutate the SSoT
     :stake      kw|nil         ; :batch/dispatch | :delivery/settle | nil
     :confidence 0..1}"
  (:require #?(:clj  [clojure.edn :as edn]
               :cljs [cljs.reader :as edn])
            [clojure.string :as str]
            [pipeline.facts :as facts]
            [pipeline.registry :as registry]
            [pipeline.store :as store]
            [langchain.model :as model]))

(defn- normalize-intake
  "Directory upsert -- the LLM only normalizes/validates the patch; it
  does not invent the batch id, terminals, product grade or
  jurisdiction. High confidence, low stakes."
  [_db {:keys [patch]}]
  {:summary    (str "バッチ記録更新: " (pr-str (keys patch)))
   :rationale  "入力 patch の正規化のみ。新規事実の生成なし。"
   :cites      (vec (keys patch))
   :effect     :batch/upsert
   :value      patch
   :stake      nil
   :confidence 0.97})

(defn- assess-segment
  "Per-jurisdiction pipeline-integrity / custody / bonding-grounding
  evidence checklist draft. `:no-spec?` injects the failure mode we
  must defend against: proposing a checklist for a jurisdiction with
  NO official spec-basis in `pipeline.facts` -- the Pipeline Integrity
  Governor must reject this (never invent a jurisdiction's
  requirements)."
  [db {:keys [subject no-spec?]}]
  (let [b (store/pipeline-batch db subject)
        iso3 (if no-spec? "ATL" (:jurisdiction b))
        sb (facts/spec-basis iso3)]
    (if (nil? sb)
      {:summary    (str iso3 " の公式spec-basisが見つかりません")
       :rationale  "pipeline.facts に未登録の法域。要件を推測で作らない。"
       :cites      []
       :effect     :segment-assessment/set
       :value      {:jurisdiction iso3 :checklist [] :spec-basis nil}
       :stake      nil
       :confidence 0.9}
      {:summary    (str iso3 " (" (:owner-authority sb) ") 向け必要書類 "
                        (count (:required-evidence sb)) " 件を提案")
       :rationale  (str "公式ソース: " (:provenance sb) " / 法的根拠: " (:legal-basis sb))
       :cites      [(:legal-basis sb) (:provenance sb)]
       :effect     :segment-assessment/set
       :value      {:jurisdiction iso3
                    :checklist (:required-evidence sb)
                    :spec-basis (:provenance sb)
                    :legal-basis (:legal-basis sb)}
       :stake      nil
       :confidence 0.9})))

(defn- propose-dispatch
  "Draft the actual BATCH-DISPATCH action -- dispatching a real batch
  into a pressurized pipeline. ALWAYS `:stake :batch/dispatch` -- this
  is a REAL-WORLD act (an autonomous pipeline-pig/valve robot
  physically opens the segment and starts flow, or an operator does),
  never a draft the actor may auto-run. See README `Actuation`: no
  phase ever adds this op to a phase's `:auto` set (`pipeline.phase`);
  the governor also always escalates on `:batch/dispatch`. Two
  independent layers agree, deliberately."
  [db {:keys [subject]}]
  (let [b (store/pipeline-batch db subject)
        line-pressure-ok? (and b (not (registry/line-pressure-out-of-range?
                                        (:line-pressure-mpa-actual b)
                                        (:line-pressure-mpa-min b)
                                        (:line-pressure-mpa-max b))))
        pod-chain-ok? (and b (true? (:prior-segment-pod-confirmed? b)))
        contamination-clear? (and b (or (not (:contamination-flag-raised? b))
                                        (:contamination-flag-resolved? b)))
        integrity-current? (and b (true? (:integrity-assessment-current? b)))
        bonding-grounding-ok? (and b (true? (:bonding-grounding-confirmed? b)))]
    {:summary    (str subject " 向け出荷提案"
                      (when b (str " (product-grade=" (:product-grade b) ")")))
     :rationale  (if b
                   (str "line-pressure-in-window?=" line-pressure-ok?
                        " pod-chain-confirmed?=" pod-chain-ok?
                        " contamination-clear?=" contamination-clear?
                        " integrity-current?=" integrity-current?
                        " bonding-grounding-ok?=" bonding-grounding-ok?)
                   "batchが見つかりません")
     :cites      (if b [subject] [])
     :effect     :batch/mark-dispatched
     :value      {:pipeline-batch-id subject}
     :stake      :batch/dispatch
     :confidence (if (and line-pressure-ok? pod-chain-ok? contamination-clear?
                          integrity-current? bonding-grounding-ok?)
                   0.9 0.3)}))

(defn- propose-delivery
  "Draft the actual BATCH-DELIVERY action -- settling a real delivery
  (custody transfer at the destination terminal, volume / grade
  reconciliation). ALWAYS `:stake :delivery/settle` -- this is a
  REAL-WORLD act (real custody / real money moves between operator and
  consignee), never a draft the actor may auto-run. See README
  `Actuation`: no phase ever adds this op to a phase's `:auto` set
  (`pipeline.phase`); the governor also always escalates on
  `:delivery/settle`. Two independent layers agree, deliberately."
  [db {:keys [subject]}]
  (let [b (store/pipeline-batch db subject)
        dispatched? (and b (:dispatched? b))
        contamination-clear? (and b (or (not (:contamination-flag-raised? b))
                                        (:contamination-flag-resolved? b)))]
    {:summary    (str subject " 向け引渡提案"
                      (when b (str " (product-grade=" (:product-grade b) ")")))
     :rationale  (if b
                   (str "batch-dispatched?=" dispatched?
                        " contamination-clear?=" contamination-clear?)
                   "batchが見つかりません")
     :cites      (if b [subject] [])
     :effect     :batch/mark-delivered
     :value      {:pipeline-batch-id subject}
     :stake      :delivery/settle
     :confidence (if (and dispatched? contamination-clear?) 0.9 0.3)}))

(defn infer
  "Route a request to the right proposal generator.
  request: {:op kw :subject id ...op-specific...}"
  [db {:keys [op] :as request}]
  (case op
    :batch/intake       (normalize-intake db request)
    :segment/verify     (assess-segment db request)
    :batch/dispatch     (propose-dispatch db request)
    :delivery/settle    (propose-delivery db request)
    {:summary "未対応の操作" :rationale (str op) :cites []
     :effect :noop :stake nil :confidence 0.0}))

;; ----------------------------- Advisor protocol -----------------------------

(defprotocol Advisor
  (-advise [advisor store request] "store + request -> proposal map"))

(defn mock-advisor
  "The deterministic advisor (the `infer` logic above). Default everywhere."
  [] (reify Advisor (-advise [_ st req] (infer st req))))

(def ^:private system-prompt
  (str "あなたは地域パイプライン輸送事業者の出荷・引渡エージェントの助言者です。"
       "与えられた事実のみに基づき、提案を1つだけEDNマップで返します。"
       "説明や前置きは一切書かず、EDNだけを出力します。\n"
       "キー: :summary(人向けドラフト) :rationale(根拠/必ず事実から) "
       ":cites(使った事実キーのベクタ) "
       ":effect(:batch/upsert|:segment-assessment/set|:batch/mark-dispatched|"
       ":batch/mark-delivered) "
       ":stake(:batch/dispatch か :delivery/settle か nil) :confidence(0..1)。\n"
       "重要: 登録されていない法域の管路インテグリティ要件を絶対に創作してはいけません。"
       "spec-basisが無い場合は :cites を空にし confidence を上げないこと。"
       "管内圧力・POD連鎖・汚染フラグ・インテグリティ評価・接地の状態を偽って報告してはいけません。"))

(defn- facts-for [st {:keys [op subject]}]
  (case op
    :segment/verify  {:batch (store/pipeline-batch st subject)}
    :batch/dispatch  {:batch (store/pipeline-batch st subject)}
    :delivery/settle {:batch (store/pipeline-batch st subject)}
    {:batch (store/pipeline-batch st subject)}))

(defn- parse-proposal
  "Parse the model's EDN proposal defensively. Any parse/shape failure
  yields a safe low-confidence noop so the Pipeline Integrity Governor
  escalates/holds -- an LLM hiccup can never auto-dispatch a batch or
  auto-deliver."
  [content]
  (let [p (try (edn/read-string (str/trim (str content)))
               (catch #?(:clj Exception :cljs :default) _ nil))]
    (if (map? p)
      (-> p
          (update :cites #(vec (or % [])))
          (update :confidence #(if (number? %) (double %) 0.0))
          (update :effect #(or % :noop)))
      {:summary "LLM応答を解釈できませんでした" :rationale (str content)
       :cites [] :effect :noop :stake nil :confidence 0.0})))

(defn llm-advisor
  "An advisor backed by a `langchain.model/ChatModel` (real inference)."
  ([chat-model] (llm-advisor chat-model {}))
  ([chat-model gen-opts]
   (reify Advisor
     (-advise [_ st req]
       (let [msgs [{:role :system :content system-prompt}
                   {:role :user :content (str "操作: " (:op req)
                                              "\n対象: " (:subject req)
                                              "\n事実: " (pr-str (facts-for st req)))}]
             resp (model/-generate chat-model msgs gen-opts)]
         (parse-proposal (:content resp)))))))

(defn trace
  "Decision-grounded audit record -- persisted to the :audit channel."
  [request proposal]
  {:t          :pipelineadvisor-proposal
   :op         (:op request)
   :subject    (:subject request)
   :summary    (:summary proposal)
   :rationale  (:rationale proposal)
   :cites      (:cites proposal)
   :confidence (:confidence proposal)})
