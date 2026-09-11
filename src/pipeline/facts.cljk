(ns pipeline.facts
  "Per-jurisdiction pipeline-integrity regulatory catalog -- the G2-style
  spec-basis table the Pipeline Integrity Governor checks every
  `:segment/verify` proposal against ('did the advisor cite an OFFICIAL
  public source for this jurisdiction's pipeline-integrity / custody /
  bonding-and-grounding requirements, or did it invent one?').

  Each entry below is a REAL jurisdiction with a REAL petroleum-pipeline
  safety regime: Japan's METI jurisdiction over the petroleum-pipeline
  business and high-pressure gas safety, the US Pipeline and Hazardous
  Materials Safety Administration (PHMSA, 49 C.F.R. Parts 190-199), the
  UK HSE / OPRED Pipeline Safety Regulations 1996, and the Norwegian
  Petroleum Safety Authority's Framework and Pipeline Regulations. The
  required-evidence set (pipeline-integrity assessment via ILI/pigging/
  hydrotest, batch custody / proof-of-delivery record, bonding-and-
  grounding confirmation) mirrors the integrity, custody-chain and
  static-electricity evidence a pipeline regulator actually demands
  before a batch is dispatched into a line.

  Coverage is reported HONESTLY (see `coverage`), the same discipline
  every sibling actor's `facts` namespace uses: a jurisdiction not in
  this table has NO spec-basis, full stop -- the advisor must not
  fabricate one, and the governor holds if it tries.")

(def catalog
  "iso3 -> requirement map. `:required-evidence` is the pipeline-integrity
  / custody / bonding-and-grounding evidence set (ILI/pigging/hydrotest
  integrity assessment, batch custody/POD record, bonding-grounding
  confirmation); `:legal-basis` / `:owner-authority` / `:provenance` are
  the G2 citation the governor requires before any `:segment/verify`
  proposal can commit."
  {"JPN" {:name "JPN"
          :owner-authority "経済産業省 (METI)"
          :legal-basis "石油パイプライン事業法; 高圧ガス保安法"
          :provenance "https://www.meti.go.jp/policy/oilgas/"
          :required-evidence ["pipeline-integrity assessment (ILI/pigging/hydrotest)"
                              "batch custody/POD record"
                              "bonding-grounding confirmation"]}
   "USA" {:name "USA"
          :owner-authority "Pipeline and Hazardous Materials Safety Administration (PHMSA)"
          :legal-basis "Pipeline Safety (49 C.F.R. Parts 190-199)"
          :provenance "https://www.phmsa.dot.gov/pipeline"
          :required-evidence ["pipeline-integrity assessment (ILI/pigging/hydrotest)"
                              "batch custody/POD record"
                              "bonding-grounding confirmation"]}
   "GBR" {:name "GBR"
          :owner-authority "HSE / OPRED"
          :legal-basis "Pipeline Safety Regulations 1996"
          :provenance "https://www.hse.gov.uk/pipelines/"
          :required-evidence ["pipeline-integrity assessment (ILI/pigging/hydrotest)"
                              "batch custody/POD record"
                              "bonding-grounding confirmation"]}
   "NOR" {:name "NOR"
          :owner-authority "Petroleum Safety Authority Norway (PSA)"
          :legal-basis "Framework Regulations; Pipeline Regulations"
          :provenance "https://www.ptil.no/en/regulations/"
          :required-evidence ["pipeline-integrity assessment (ILI/pigging/hydrotest)"
                              "batch custody/POD record"
                              "bonding-grounding confirmation"]}})

(defn spec-basis
  "The jurisdiction's requirement map, or nil -- nil means NO spec-basis,
  and the governor must hold any proposal that tries to dispatch a batch
  or settle a delivery on it."
  [iso3]
  (get catalog iso3))

(defn coverage
  "Honest coverage report: how many of the requested jurisdictions
  actually have a spec-basis entry. Never report a missing jurisdiction
  as covered."
  ([] (coverage (keys catalog)))
  ([iso3s]
   (let [have (filter catalog iso3s)
         missing (remove catalog iso3s)]
     {:requested (count iso3s)
      :covered (count have)
      :covered-jurisdictions (vec (sort have))
      :missing-jurisdictions (vec (sort missing))
      :note (str "cloud-itonami-isic-4950 R0: " (count catalog)
                 " jurisdictions seeded with an official spec-basis. "
                 "This is a starting catalog, not a survey of all ~194 "
                 "jurisdictions -- extend `pipeline.facts/catalog`, "
                 "never fabricate a jurisdiction's requirements.")})))

(defn required-evidence-satisfied?
  "Does `submitted` (a set/coll of evidence keywords or strings) satisfy
  every evidence item listed for `iso3`? Missing spec-basis -> never
  satisfied."
  [iso3 submitted]
  (when-let [{:keys [required-evidence]} (spec-basis iso3)]
    (let [need (count required-evidence)
          have (count (filter (set submitted) required-evidence))]
      (= need have))))

(defn evidence-checklist [iso3]
  (:required-evidence (spec-basis iso3) []))
