# Business Model: Community Pipeline Transport

## Classification
- Repository: `cloud-itonami-isic-4950`
- ISIC Rev.5: `4950` — transport of petroleum/products via pipeline
- Domain: `transport/pipeline`
- Social impact: safety, environmental protection, transparency
- Governor: `:pipeline-integrity-governor`
- License: AGPL-3.0-or-later

## Scope
This actor covers batch intake through per-jurisdiction pipeline-integrity /
custody / bonding-grounding regulatory assessment, batch dispatch (starting
flow of a real batch into a pressurized line), and delivery settlement
(custody transfer at the destination terminal, volume / grade reconciliation)
for a community pipeline operator. It does **not**, by itself, hold any
operating authority required to run a pipeline-transport business in a given
jurisdiction, perform the actual physical pigging / valve operation, or
optimize batch scheduling / throughput (optimization is a follow-up slice,
not this R0). Whoever deploys a live instance supplies the jurisdiction-
specific operating authority, the real pipeline-pig/valve-robot and SCADA /
pipeline-accounting integrations, and bears that jurisdiction's liability --
the software supplies the governed, spec-cited, audited execution scaffold so
the operator does not have to build the compliance layer from scratch.

## Customer
- regional and community pipeline operators and terminal operators
- independent operators leaving closed SCADA / pipeline-control SaaS
- product-pipeline joint operators running common-carrier lines
- custody counterparties and regulators who need an auditable, spec-cited
  batch record

## Offer
- batch intake and directory management
- per-jurisdiction pipeline-integrity / custody / bonding-grounding
  regulatory assessment with an official spec-basis citation
- batch dispatch (starting flow) gated on full evidence and a clean
  line-pressure / POD-chain / contamination / integrity / bonding envelope
- delivery settlement (custody transfer, volume / grade reconciliation) with
  double-delivery prevention
- evidence checklisting (pipeline-integrity ILI/pigging/hydrotest
  assessment, batch custody/POD record, bonding-grounding confirmation)
- contamination-flag and exception workflows
- role-based access and immutable audit ledger

## Revenue
- self-host setup fee
- managed hosting subscription per operator / line
- support retainer with SLA
- SCADA and pipeline-accounting integration

## The `:pipeline-integrity-governor` Decision Rule

This blueprint's `:itonami.blueprint/governor` is `:pipeline-integrity-governor`.
It is the single authority that stands between "a batch could be dispatched
into the line" and "a batch is allowed to flow," and between "a delivery
could be settled" and "it is allowed to settle." Every rule it enforces is
traceable to the domain (Community Pipeline Transport, ISIC 4950) and to the
three `:social-impact` tags in `blueprint.edn` (`:safety`, `:environmental-
protection`, `:transparency`).

This is the rule the companion contract test (`test/pipeline/governor_contract_
test.clj`) encodes end-to-end: the PipelineTransport advisor never dispatches
a batch or settles a delivery the Pipeline Integrity Governor would reject,
`:batch/dispatch` and `:delivery/settle` NEVER auto-commit at any phase,
`:batch/intake` (no direct capital risk) MAY auto-commit when clean, and every
decision (commit OR hold) leaves exactly one ledger fact.

**Authorizes a batch dispatch (`:batch/dispatch`) or delivery settlement
(`:delivery/settle`) only when ALL of the following hold:**

1. **An official spec-basis citation exists for the jurisdiction** -- the
   governor will not authorize any `:segment/verify`, `:batch/dispatch`, or
   `:delivery/settle` proposal whose jurisdiction has no entry in the
   `pipeline.facts` catalog (`:no-spec-basis`). This is the direct enforcement
   of `:transparency`: a jurisdiction whose pipeline-integrity / custody /
   bonding-grounding requirements cannot be traced to an OFFICIAL public source
   is never guessed. The advisor must not fabricate a jurisdiction's
   requirements.
2. **The jurisdiction's required evidence is fully on file** -- for a dispatch
   or delivery the batch's jurisdiction must have been assessed with a complete
   pipeline-integrity / custody / bonding-grounding evidence checklist on
   record: the ILI/pigging/hydrotest integrity assessment, the batch custody /
   POD record, and the bonding-grounding confirmation (`:evidence-incomplete`).
   This protects `:safety` and `:environmental-protection`: a line that cannot
   prove integrity, custody and static-electricity readiness never flows.
3. **The measured line pressure stays inside its declared safe window
   `[min, max]`** -- the governor INDEPENDENTLY re-verifies the batch's own
   recorded line pressure against its two-sided safe operating envelope
   (`pipeline.registry/line-pressure-out-of-range?`, the aerospace two-sided-
   tolerance discipline applied to a pressurized pipeline). Below `min` risks
   slack-line / cavitation / column separation; above `max` risks overpressure
   and line rupture (`:line-pressure-out-of-range`).
4. **The prior-segment POD (proof-of-delivery) chain is confirmed** -- the
   governor INDEPENDENTLY re-verifies that custody was proved on the prior
   segment (`:prior-segment-pod-confirmed?`), the freight 4920 POD-chain
   discipline applied to pipeline custody. A batch whose upstream custody chain
   is broken is never dispatched (`:pod-chain-integrity-broken`).
5. **No unresolved product contamination flag is open on the batch** -- an
   interface-mixing / grade contamination flag raised and not yet resolved is
   a hard hold. Product of unknown grade never enters the line
   (`:product-contamination-flag-unresolved`).
6. **The line's integrity assessment is current** -- the governor INDEPENDENTLY
   re-verifies that the integrity assessment (ILI / pigging / hydrotest
   interval) is current (`:integrity-assessment-current?`), the PHMSA recurring
   ILI duty. A line whose integrity assessment interval has expired never flows
   (`:integrity-assessment-stale`).
7. **Bonding and grounding are confirmed** -- the governor INDEPENDENTLY
   re-verifies that bonding-and-grounding was confirmed before flow
   (`:bonding-grounding-confirmed?`). Static-electricity accumulation during
   product flow is an ignition source; unconfirmed bonding/grounding never
   dispatches (`:bonding-grounding-unconfirmed`).
8. **The batch has not already been dispatched, and the delivery has not
   already been settled** -- a double dispatch of the same batch is refused off
   a dedicated `:dispatched?` fact, and a double delivery off a dedicated
   `:delivered?` fact (never a `:status` value), the double-actuation guard
   every sibling actor in this fleet enforces (`:already-dispatched` /
   `:already-delivered`).

**Rejects (HOLD, un-overridable, never even reaches a human) when any of the
above fail.** A proposal with no spec-basis, incomplete evidence, a line
pressure outside its window, a broken POD chain, an unresolved contamination
flag, a stale integrity assessment, unconfirmed bonding/grounding, or a double
dispatch/delivery is held at the governor node -- a human approver cannot
override these, by construction.

**Always escalates to a human (never auto-commits) for `:batch/dispatch` and
`:delivery/settle`**, even when every check above is clean. Dispatching a real
batch into a pressurized line (starting flow against a live pressure envelope)
and settling a real delivery (real custody / real money moving between
operator and consignee) are the two real-world actuation events this actor
performs; both are always a human pipeline controller's call. This is enforced
by TWO independent layers that agree on purpose: the governor's confidence /
actuation SOFT gate (a `:batch/dispatch` / `:delivery/settle` stake always
escalates) and `pipeline.phase`'s phase table, which never puts either op in
any phase's `:auto` set. The `:environmental-protection` tag is enforced
upstream of the governor, in the segment-verification evidence step -- the
governor's job is dispatch/delivery authorization integrity, not throughput
optimization.

## Required Technologies

`blueprint.edn`'s `:itonami.blueprint/required-technologies` for this business,
and what each one is actually load-bearing for here (not a generic capability
list):

| Technology | What it is FOR in Community Pipeline Transport |
|---|---|
| `:robotics` | The autonomous pipeline-pig/valve robot that performs the physical batch dispatch and segment verification (opening a line segment to flow, and eventually closing it). The governor never dispatches hardware itself: a dispatch-clearing action must have cleared the same sign-off a human pipeline controller would need (see Robotics Premise). |
| `:identity` | Operator, pipeline-controller, and custody-counterparty identity plus role-based access, so the governor's sign-off is tied to *who* authorized a dispatch or delivery, not just *that* someone did. |
| `:forms` | Structured intake for batch booking, per-jurisdiction evidence capture (ILI/pigging/hydrotest integrity assessment, batch custody/POD record, bonding-grounding confirmation), and contamination-flag submission -- the data the Decision Rule above actually evaluates comes in through these forms. |
| `:dmn` | Encodes the `:pipeline-integrity-governor` Decision Rule itself (spec-basis, evidence completeness, line-pressure window, POD chain, contamination flag, integrity currency, bonding/grounding, the double-actuation guards, the actuation gate) as an evaluable decision table rather than code buried in application logic -- this is what makes the governor auditable and swappable per-deployment. |
| `:bpmn` | Orchestrates the intake -> verify -> dispatch -> settle -> audit loop end-to-end (see `docs/operator-guide.md`) across batch intake, segment verification, batch dispatch, and delivery settlement, including the contamination-flag escalation gate. |
| `:audit-ledger` | The immutable record of every assessment, dispatch, delivery, contamination flag, and hold -- this is what "an auditable, spec-cited batch record for every dispatch and delivery" (Trust Controls, below) actually means in practice, and the evidence an operator needs if a dispatch or delivery is later disputed by a custody counterparty or regulator. |
| `:optimization` | Batch scheduling and throughput optimization -- selects the dispatch sequence for a line. This R0 build deliberately scopes optimization OUT (see README `Business-process coverage`); the capability is correctly marked required, the integration is a follow-up slice. |

There is NO bespoke `:pipeline` capability library in this stack (unlike the
freight sibling's `:logistics`): the pipeline-integrity range check (line-
pressure two-sided window) is a self-contained pure function in
`pipeline.registry`, on top of the generic robotics/identity/forms/dmn/bpmn/
audit-ledger stack (see Capability layer).

## Trust Controls
- a jurisdiction with no official spec-basis can never be verified, dispatched,
  or delivered against
- a dispatch never starts with incomplete pipeline-integrity / custody /
  bonding-grounding evidence
- a dispatch never starts outside the line-pressure window, with a broken POD
  chain, an unresolved contamination flag, a stale integrity assessment, or
  unconfirmed bonding/grounding
- contamination flags cannot be silently suppressed
- the same batch can never be dispatched or delivered twice
- a dispatch or delivery never auto-commits; both always need a human pipeline
  controller
- every dispatch and delivery (commit OR hold) leaves exactly one immutable
  ledger fact
- batch, line, and custody data stays outside Git

## Implementation notes (`:implemented`)

The Decision Rule above is implemented faithfully by `pipeline.governor` as
nine HARD checks (a human approver cannot override them) plus one SOFT gate:

- `spec-basis-violations` -- the spec-basis check above, evaluated on every
  `:segment/verify`, `:batch/dispatch`, and `:delivery/settle`.
- `evidence-incomplete-violations` -- the evidence-completeness check above,
  for `:batch/dispatch` / `:delivery/settle`.
- `line-pressure-out-of-range-violations` -- the two-sided line-pressure
  window above, an honest reapplication of the aerospace two-sided-tolerance
  discipline to a pressurized pipeline; evaluated unconditionally on every
  `:batch/dispatch`.
- `pod-chain-integrity-broken-violations` -- the prior-segment POD-chain
  check above, an honest reapplication of the freight 4920 POD-chain
  discipline to pipeline custody; evaluated unconditionally on every
  `:batch/dispatch`.
- `product-contamination-flag-unresolved-violations` -- the open-
  contamination-flag check above (interface mixing / grade contamination);
  evaluated unconditionally on every `:batch/dispatch`.
- `integrity-assessment-stale-violations` -- the integrity-currency check
  above (the PHMSA recurring ILI/pigging/hydrotest duty); evaluated
  unconditionally on every `:batch/dispatch`.
- `bonding-grounding-unconfirmed-violations` -- the bonding/grounding check
  above (static-electricity ignition during flow); evaluated unconditionally
  on every `:batch/dispatch`.
- `already-dispatch-violations` / `already-delivery-violations` -- the
  double-actuation guards above, off dedicated `:dispatched?` / `:delivered?`
  booleans (never a `:status` value), the same discipline every sibling
  governor's guards establish.
- the confidence floor / actuation SOFT gate -- low confidence, OR a
  `:batch/dispatch` / `:delivery/settle` stake, escalates to a human; and
  `pipeline.phase` independently never auto-commits either op at any phase.

`:batch/dispatch` and `:delivery/settle` are the two real-world actuation
events (`#{:batch/dispatch :delivery/settle}`), applied SEQUENTIALLY to the
SAME batch (dispatch first, delivery later) rather than the retail sibling's
`:kind`-distinguished alternative-action shape -- the same sequential
dual-actuation shape the repair-shop and quarrying clusters use. Neither ever
auto-commits at any phase. Batch scheduling and throughput optimization (the
`:optimization` line above) is a follow-up slice, not in this R0 build -- see
README `Business-process coverage`.

## Capability layer

Unlike `cloud-itonami-isic-4920` (which wraps a pre-existing bespoke
capability library `kotoba-lang/logistics`), this vertical is SELF-CONTAINED:
there is no `kotoba-lang/pipeline` to delegate pipeline-integrity validation
to. The line-pressure two-sided range check lives as a pure function in
`pipeline.registry` and is re-verified independently by the governor, rather
than wrapping an external capability library's own validated function -- the
same 'reuse a capability's own validated function' discipline, here applied
to this vertical's OWN pure registry functions.

## Jurisdiction coverage (honest)

`pipeline.facts/catalog` currently seeds 4 jurisdictions with an official
spec-basis, each a REAL regime: Japan (METI, 石油パイプライン事業法 / 高圧ガス
保安法), the United States (PHMSA, 49 C.F.R. Parts 190-199), the United Kingdom
(HSE / OPRED, Pipeline Safety Regulations 1996), and Norway (Petroleum Safety
Authority, Framework Regulations; Pipeline Regulations). This is a starting
catalog to prove the governor contract end-to-end, not a claim of global
coverage (4 of ~194 jurisdictions worldwide). Adding a jurisdiction is
additive: one map entry in `pipeline.facts/catalog`, citing a real official
source -- never fabricate a jurisdiction's requirements to make coverage look
bigger.

## Maturity

`:implemented` -- PipelineTransport advisor + Pipeline Integrity Governor run
as real, tested code (`clojure -M:dev:test`: 37 tests / 193 assertions, 0
failures; lint clean), promoted from the originally-published `:blueprint`-
tier scaffold, following the SAME governed-actor architecture as the other
prior actors across this fleet, with its own distinct, independently-named
governor and its own self-contained pipeline-integrity range check. See
`docs/adr/0001-architecture.md` for the history and design.

## Robotics Premise

`blueprint.edn` sets `:itonami.blueprint/robotics true`. In this domain an
autonomous pipeline-pig/valve robot performs the physical batch dispatch and
segment verification (opening a line segment to flow, and eventually closing
it), under the actor, gated by the independent **Pipeline Integrity
Governor**. The governor never dispatches hardware itself: a dispatch-clearing
action must have cleared the same sign-off a human pipeline controller would
need. A robot may turn the valve, but only after the governor (every HARD
check clean) and a human controller both agree it is safe to -- the same
operating-state-machine-gated-by-governor premise every cloud-itonami vertical
restates (ADR-2607011000): the blueprint declares `:robotics true`, the README
names the robot that performs the physical act, and the Pipeline Integrity
Governor is the independent gate that robot's command must pass.
