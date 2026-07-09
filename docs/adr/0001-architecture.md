# ADR-0001: PipelineTransport advisor ⊣ Pipeline Integrity Governor architecture

## Status

Accepted. `cloud-itonami-isic-4950` promoted from `:blueprint` to
`:implemented` in the `kotoba-lang/industry` registry.

## Context

`cloud-itonami-isic-4950` publishes an OSS business blueprint for community
pipeline transport (batch intake, per-jurisdiction pipeline-integrity /
custody / bonding-grounding regulatory assessment, batch dispatch, and delivery
settlement). Like every prior actor in this fleet, the blueprint alone is not
an implementation: this ADR records the governed-actor architecture that
promotes it to real, tested code, following the same langgraph StateGraph +
independent Governor + Phase 0->3 rollout pattern established by
`cloud-itonami-isic-6511` (life insurance) and applied across 91 prior
siblings, most recently `cloud-itonami-isic-0610` (community crude petroleum
extraction).

Like `cloud-itonami-isic-0610` (crude) and `cloud-itonami-isic-0810`
(quarrying), this vertical has NO bespoke domain capability library in
`kotoba-lang` to wrap (verified: no `kotoba-lang/pipeline`-style repo exists,
and `kotoba-lang/robotics` is the generic cross-cutting robotics contract
every cloud-itonami vertical already uses, not a domain-specific library for
this vertical). This build therefore uses self-contained domain logic -- the
same pattern the majority of this fleet's actors use, and the explicit
differentiator from `cloud-itonami-isic-4920` (which wraps a pre-existing
`kotoba-lang/logistics` library). The pipeline-integrity range check (line-
pressure two-sided window) lives as a pure function in `pipeline.registry`
and is re-verified independently by the governor.

This blueprint's own `:itonami.blueprint/governor` keyword,
`:pipeline-integrity-governor`, is grep-verified UNIQUE fleet-wide -- no
naming-collision precedent question, a fresh independent build.

## Decision

### Decision 1: fresh governor identity, no reuse precedent needed

`:pipeline-integrity-governor` is grep-verified unique across every
`blueprint.edn` in this fleet. This build follows the SAME governed-actor
architecture as every prior actor, but with its own distinct governor
identity.

### Decision 2: self-contained domain logic (no `kotoba-lang/pipeline` to wrap)

Unlike `cloud-itonami-isic-4920` (freight, which delegates tracking-number
validation to a real, pre-existing `kotoba-lang/logistics` capability
library), this pipeline-transport vertical has NO pre-existing pipeline
capability library to delegate pipeline-integrity validation to. The one
physical range check (line-pressure two-sided window) is therefore a pure
function defined in `pipeline.registry` and called directly by
`pipeline.governor` -- the SAME 'reuse a capability's own validated function'
discipline `retailops.governor`'s ean13 check establishes for a capability
library, here applied to this vertical's OWN pure registry functions rather
than a separate library. No literal code is shared with any sibling (different
domain), but the discipline is the same.

### Decision 3: dual-actuation shape, SEQUENTIAL on the SAME `pipeline-batch` entity

Unlike the retail sibling's `order` entity (distinguished by `:kind`,
alternative sale-or-reorder actions), this vertical's `dispatch` and `settle`
actuation events apply SEQUENTIALLY to the SAME `pipeline-batch` -- a batch
dispatch happens first (flow started into the line), delivery settlement
happens later (custody transfer at the destination terminal), on the same
batch record. This matches the repair-shop cluster's `ticket`, the quarrying
cluster's `extraction`, and the crude cluster's `well` shape (two real-world
acts, in order, on one entity). `high-stakes` is `#{:batch/dispatch
:delivery/settle}`; neither ever auto-commits at any phase.

### Decision 4: the pipeline-integrity physical range-check suite -- honest reapplications of established fleet disciplines

The physical range check the governor runs on every `:batch/dispatch` is an
honest reapplication of an established fleet discipline to a pipeline-
transport value, documented as such rather than claimed as a novel invention
(the same convention `cloud-itonami-isic-0162`'s Decision 3 establishes for
`dose-matches-claim?`):

- `line-pressure-out-of-range?` reapplies the **aerospace two-sided-tolerance**
  discipline to a pressurized pipeline: the batch's measured line pressure
  must stay inside its declared safe window `[min, max]`. Below `min` risks
  slack-line / cavitation / column separation (and incomplete product sweep);
  above `max` risks overpressure and line rupture.

It returns `true` when the value is provably OUTSIDE the safe envelope; the
conservative integrity choice, missing data is a violation (cannot verify
safe to dispatch). It is evaluated UNCONDITIONALLY on every `:batch/dispatch`.
No new unconditional-evaluation ordinals are claimed: this check is a
discipline-reapplication, documented per Decision 3 of
`cloud-itonami-isic-0162`.

### Decision 5: the four boolean ground-truth checks -- honest reapplications of established fleet disciplines

The remaining four `:batch/dispatch` HARD checks are direct reads of the
batch's own recorded boolean ground truth, each an honest reapplication of an
established fleet discipline rather than a novel concept:

- `pod-chain-integrity-broken?` reapplies the **freight 4920 POD-chain
  discipline** to pipeline custody: custody must have been proved on the prior
  segment (`:prior-segment-pod-confirmed?`) before a batch flows.
- `product-contamination-flag-unresolved?` reuses the SAME
  open-flag-unresolved discipline the crude sibling's
  `integrity-flag-unresolved?` check (and the freight sibling's
  delivery-exception-unresolved check) establish -- an open interface-mixing /
  grade contamination flag cannot be silently suppressed to force a dispatch.
- `integrity-assessment-stale?` reapplies the **recurring-inspection-interval
  duty** discipline (the PHMSA recurring ILI/pigging/hydrotest duty) -- a line
  whose integrity assessment interval has expired never flows.
- `bonding-grounding-unconfirmed?` reapplies the **measured-control-confirmed
  before flow** discipline -- static-electricity accumulation during product
  flow is an ignition source, so bonding/grounding must be confirmed
  (`:bonding-grounding-confirmed?`) before dispatch.

Each is evaluated UNCONDITIONALLY on every `:batch/dispatch`. No new
unconditional-evaluation ordinals are claimed.

### Decision 6: dedicated double-actuation-guard booleans

`:dispatched?` / `:delivered?` are dedicated booleans on the `pipeline-batch`
record, never a single `:status` value -- the same discipline every prior
governor's guards establish, informed by `cloud-itonami-isic-6492`'s real
status-lifecycle bug (ADR-2607071320).

### Decision 7: Store protocol, MemStore + DatomicStore parity

`pipeline.store/Store` is implemented by both `MemStore` (atom-backed, default
for dev/tests/demo) and `DatomicStore` (`langchain.db`-backed), proven to
satisfy the same contract in `test/pipeline/store_contract_test.clj`. The
ledger stays append-only on every backend: which batch was screened for a line
pressure outside its window, a broken POD chain, an unresolved contamination
flag, a stale integrity assessment, or unconfirmed bonding/grounding, which
batch had been dispatched, which delivery was settled, on what jurisdictional
basis, approved by whom -- always a query over an immutable log.

### Decision 8: Phase 0->3 with `:batch/dispatch`/`:delivery/settle` NEVER auto

`pipeline.phase`'s phase table puts `:batch/intake` (no direct capital risk)
in phase 3's `:auto` set as its only member; `:batch/dispatch` and
`:delivery/settle` are deliberately ABSENT from every phase's `:auto` set,
including phase 3 -- a permanent structural fact. `pipeline.governor`'s
high-stakes gate enforces the same invariant independently: two layers agree
that actuation is always a human pipeline controller's call.

### Decision 9: mock + LLM advisor pair

`pipeline.pipelineadvisor` provides a deterministic `mock-advisor` (default,
runs offline) and an `llm-advisor` backed by a `langchain.model/ChatModel`.
The LLM advisor's EDN proposal is parsed defensively: any parse/shape failure
yields a safe low-confidence noop so the governor escalates/holds -- an LLM
hiccup can never auto-dispatch a batch or auto-deliver.

## Alternatives considered

- **Wrapping a bespoke `kotoba-lang/pipeline` capability library.**
  Considered and explicitly ruled out: no such library exists, and
  `kotoba-lang/robotics` is generic, not pipeline-specific. Forcing a false
  capability-library integration would be dishonest; this build correctly
  uses self-contained domain logic instead.
- **A `:kind`-distinguished entity** (matching the retail sibling's `order`
  shape). Rejected: dispatch and delivery happen SEQUENTIALLY on the SAME
  batch in this domain, not as alternative actions -- the repair-shop /
  quarrying / crude cluster's sequential shape is the honest match here.
- **Claiming genuinely-new unconditional-evaluation ordinals for the physical
  range check.** Rejected: the check reapplies an established fleet discipline
  (aerospace two-sided-tolerance) to a new domain, and the four boolean
  ground-truth checks reuse established fleet disciplines (freight POD-chain,
  open-flag-unresolved, recurring-inspection-interval, measured-control-
  confirmed). Per `cloud-itonami-isic-0162` Decision 3's convention, these are
  documented as honest discipline-reapplications, not claimed as novel
  inventions -- the same honesty discipline that forbids fabricating coverage
  also forbids over-claiming novelty.
- **Building batch scheduling / throughput optimization in this R0.**
  Rejected in favor of a scoped R0 slice (the `:optimization` capability is
  correctly marked required, the integration is a follow-up), consistent with
  this fleet's 'extending coverage is additive' convention.

## Consequences

- 92nd actor in this fleet (91 implemented before the crude sibling; this
  build follows immediately).
- Establishes the pipeline-integrity physical range check and boolean
  ground-truth suite as honest reapplications of established fleet disciplines
  (two-sided-tolerance, freight POD-chain, open-flag-unresolved, recurring-
  inspection-interval, measured-control-confirmed) to pipeline transport -- no
  genuinely-new-concept check, all discipline-reuse documented as such per
  `cloud-itonami-isic-0162` Decision 3.
- `MemStore` || `DatomicStore` parity is proven by
  `test/pipeline/store_contract_test.clj`.
- 37 tests / 193 assertions pass; lint is clean; the demo
  (`clojure -M:dev:run`) walks one clean dispatch + delivery lifecycle, plus
  eight HARD-hold scenarios (no spec-basis, line pressure, POD chain,
  contamination flag, integrity assessment, bonding/grounding, double
  dispatch, double delivery), end-to-end.
- `blueprint.edn` required no field-sync fixes (already correct) -- only the
  `:maturity` flip itself.

## References

- `cloud-itonami-isic-6511/docs/adr/0001-architecture.md` (origin of the
  general governed-actor architecture pattern)
- `cloud-itonami-isic-4920/docs/adr/0001-architecture.md` (freight sibling;
  contrast: wraps a pre-existing `kotoba-lang/logistics` capability library;
  origin of the POD-chain discipline this build reapplies)
- `cloud-itonami-isic-0610/docs/adr/0001-architecture.md` (crude sibling;
  closest structural analog -- self-contained domain logic, sequential
  dual-actuation on one entity)
- `cloud-itonami-isic-0162/docs/adr/0001-architecture.md` (origin of the
  'honest reapplication, documented as such' convention this build follows
  for its physical range check and boolean ground-truth checks)
- 石油パイプライン事業法; 高圧ガス保安法 (Japan, METI)
- Pipeline Safety, 49 C.F.R. Parts 190-199 (US, PHMSA)
- Pipeline Safety Regulations 1996 (UK, HSE / OPRED)
- Framework Regulations; Pipeline Regulations (Norway, Petroleum Safety
  Authority)
