# cloud-itonami-isic-4950

Open Business Blueprint for **ISIC Rev.5 4950**: Transport via
pipeline -- batch intake, per-jurisdiction pipeline-integrity /
custody / bonding-grounding regulatory assessment, batch dispatch
into a pressurized line, and delivery settlement at the destination
terminal for a community pipeline operator.

This repository publishes a pipeline-transport actor -- batch intake,
per-jurisdiction pipeline-integrity regulatory assessment, batch
dispatch and delivery settlement -- as an OSS business that any
qualified operator can fork, deploy, run, improve and sell, so a
regional pipeline operator never surrenders pipeline-integrity and
custody-accounting data to a closed SCADA/pipeline-control SaaS.

Built on this workspace's
[`langgraph`](https://github.com/kotoba-lang/langgraph)
StateGraph runtime (portable `.cljc`, supervised superstep loop,
interrupts, Datomic/in-mem checkpoints) -- the same actor pattern as
every prior actor in this fleet -- here it is **PipelineTransport
advisor ⊣ Pipeline Integrity Governor**. This blueprint's own
`:itonami.blueprint/governor` keyword, `:pipeline-integrity-governor`,
is a UNIQUE keyword fleet-wide (grep-verified: no other blueprint
declares it) -- a fresh, independent build.

**Unlike `cloud-itonami-isic-4920` (which wraps a pre-existing
bespoke capability library `kotoba-lang/logistics`), this vertical is
SELF-CONTAINED**: there is no `kotoba-lang/pipeline` to delegate
pipeline-integrity validation to, so the line-pressure two-sided range
check lives as a pure function in `pipeline.registry` and is re-verified
independently by the governor, rather than wrapping an external
capability library's own validated function.

> **Why an actor layer at all?** An LLM is great at drafting a batch
> summary, normalizing records, and reading a pressure gauge -- but it
> has **no notion of which jurisdiction's pipeline-safety law is
> official, no license to dispatch a real batch into a pressurized line
> or settle a real delivery, and no way to know on its own whether the
> measured line pressure actually lies inside the declared safe window,
> whether custody was actually proved on the prior segment, whether a
> product contamination flag has actually been resolved, whether the
> line's integrity assessment (ILI/pigging/hydrotest) is actually
> current, or whether bonding-and-grounding was actually confirmed**.
> Letting it dispatch a batch or settle a delivery directly invites
> fabricated regulatory citations, a batch flowing with a line pressure
> outside its safe envelope or a broken custody chain, and a dispatch
> into a line whose static-electricity ignition controls were never
> confirmed -- exposing the crew and the environment to a real
> rupture / fire risk and the operator to real liability, for whoever
> runs it. This project seals the PipelineTransport advisor into a
> single node and wraps it with an independent **Pipeline Integrity
> Governor**, a human **approval workflow**, and an immutable **audit
> ledger**.

## Scope: what this actor does and does not do

This actor covers batch intake through pipeline-integrity / custody /
bonding-grounding regulatory assessment, batch dispatch and delivery
settlement. It does **not**, by itself, hold any operating authority
required to run a pipeline-transport business in a given jurisdiction,
and it does not claim to. It also does not perform the actual physical
pigging/valve operation itself, or optimize batch scheduling /
throughput (the blueprint's own `:optimization` technology) -- that is
a follow-up slice, not in this R0. Whoever deploys and operates a live
instance (a qualified pipeline controller/operator) supplies any
jurisdiction-specific operating authority, the real pipeline-pig/valve-
robot dispatch integration and the real SCADA / pipeline-accounting
integrations, and bears that jurisdiction's liability -- the software
supplies the governed, spec-cited, audited execution scaffold so that
operator does not have to build the compliance layer from scratch.

### Actuation

**Dispatching a real batch into a pressurized line and settling a real
delivery are never autonomous, at any phase, by construction.** Two
independent layers enforce this (`pipeline.governor`'s `:batch/dispatch`/
`:delivery/settle` high-stakes gate and `pipeline.phase`'s phase table,
which never puts either op in any phase's `:auto` set) -- see
`pipeline.phase`'s docstring and `test/pipeline/phase_test.clj`'s
`batch-dispatch-never-auto-at-any-phase`/`delivery-settle-never-auto-at-
any-phase`. The actor may draft, check and recommend; a human pipeline
controller is always the one who actually dispatches a batch into the
line or settles a delivery. Grounded in pipeline-safety doctrine (the
same discipline every regulator in `pipeline.facts` codifies: a real
dispatch and a real delivery settlement are human sign-off acts) -- a
genuine DUAL-actuation shape, applied SEQUENTIALLY to the SAME batch
(dispatch first, delivery settlement later), unlike `retailops`/4711's
own `:kind`-distinguished alternative-action shape.

## The core contract

```
batch intake + jurisdiction facts (pipeline.facts, spec-cited)
        |
        v
   ┌───────────────────────┐   proposal      ┌───────────────────────┐
   │ PipelineTransport     │ ─────────────▶ │ Pipeline Integrity     │  (independent system)
   │ advisor (sealed)      │  + citations    │ spec-basis · evidence- │
   └───────────────────────┘                 │ incomplete · line-     │
          │                 commit ◀┼ pressure-out-of-range (NEW, │
          │                         │ two-sided tolerance) · pod- │
    record + ledger        escalate ┼ chain-integrity-broken (NEW, │
          │              (ALWAYS for│ freight POD-chain discipline)│
          │       :batch/dispatch/  │ · product-contamination-flag-│
          │       :delivery/settle) │ unresolved · integrity-      │
          │                         │ assessment-stale · bonding-  │
          ▼                          │ grounding-unconfirmed ·      │
      human approval                 │ already-dispatched · already-│
                                      │ delivered                    │
                                      └───────────────────────┘
```

**The PipelineTransport advisor never dispatches a batch or settles a
delivery the Pipeline Integrity Governor would reject, and never does
so without a human sign-off.** Hard violations (fabricated regulatory
requirements; unsupported evidence; a line pressure outside the safe
window; a broken prior-segment POD chain; an unresolved product
contamination flag; a stale integrity assessment; unconfirmed bonding/
grounding; a double dispatch/delivery) force **hold** and *cannot* be
approved past; a clean dispatch/delivery proposal still always routes
to a human.

## Run

```bash
clojure -M:dev:run     # walk one clean dispatch + delivery lifecycle, plus six HARD-hold cases, through the actor
clojure -M:dev:test    # governor contract · phase invariants · store parity · registry conformance · facts coverage
clojure -M:lint        # clj-kondo (errors fail; CI mirrors this)
```

## Robotics premise

All cloud-itonami verticals are designed on the premise that a **robot
performs the physical domain work**. Here an autonomous pipeline-pig or
valve robot performs the physical batch dispatch and segment
verification (opening a line segment to flow, and eventually closing
it), under the actor, gated by the independent **Pipeline Integrity
Governor**. The governor never dispatches hardware itself: a dispatch-
clearing action must have cleared the same sign-off a human pipeline
controller would need. This restates the fleet-wide robotics premise
three ways (ADR-2607011000): the blueprint declares `:robotics true`,
the README names the robot that performs the physical act, and the
Pipeline Integrity Governor is the independent gate that robot's
command must pass -- a robot may turn the valve, but only after the
governor and a human controller both agree it is safe to.

## Open business

This repository is not only source code. It is a public, forkable
business model:

| Layer | What is open |
|---|---|
| OSS core | Actor runtime, Pipeline Integrity Governor, dispatch/delivery draft records, audit ledger |
| Business blueprint | Customer, offer, pricing, unit economics, sales motion |
| Operator playbook | How to fork, license, deploy and support the service in a jurisdiction |
| Trust controls | Governance, security reporting, actuation invariant, audit requirements |

See [`docs/business-model.md`](docs/business-model.md) and
[`docs/operator-guide.md`](docs/operator-guide.md) to start this as an
open business on itonami.cloud, and
[`docs/adr/0001-architecture.md`](docs/adr/0001-architecture.md) for the
full architecture and decision record.

## Capability layer

This blueprint resolves its technology stack via
[`kotoba-lang/industry`](https://github.com/kotoba-lang/industry) (ISIC
`4950`). Unlike the freight sibling, this vertical is NOT backed by a
separate bespoke domain capability lib: the pipeline-integrity range
check (line-pressure two-sided window) is a self-contained pure
function in `pipeline.registry`, on top of the generic
robotics/identity/forms/dmn/bpmn/audit-ledger stack.

## Layout

| File | Role |
|---|---|
| `src/pipeline/store.cljc` | **Store** protocol -- `MemStore` ‖ `DatomicStore` (`langchain.db`) + append-only audit ledger + dispatch AND delivery history (dual history). The double-actuation guard checks dedicated `:dispatched?`/`:delivered?` booleans rather than a `:status` value |
| `src/pipeline/registry.cljc` | Dispatch/delivery draft records, plus the self-contained pipeline-integrity range-check pure function (`line-pressure-out-of-range?`) the governor re-verifies against -- no external capability library to delegate to |
| `src/pipeline/facts.cljc` | Per-jurisdiction pipeline-integrity / custody / bonding-grounding catalog with an official spec-basis citation per entry, honest coverage reporting |
| `src/pipeline/pipelineadvisor.cljc` | **PipelineTransport advisor** -- `mock-advisor` ‖ `llm-advisor`; intake/segment-verification/dispatch/delivery proposals |
| `src/pipeline/governor.cljc` | **Pipeline Integrity Governor** -- 7 HARD checks (spec-basis · evidence-incomplete · line-pressure-out-of-range, the aerospace two-sided-tolerance discipline · pod-chain-integrity-broken, the freight 4920 POD-chain discipline · product-contamination-flag-unresolved · integrity-assessment-stale · bonding-grounding-unconfirmed) + 2 double-actuation guards + 1 soft (confidence/actuation gate) |
| `src/pipeline/phase.cljc` | **Phase 0→3** -- read-only → assisted intake → assisted verify → supervised (dispatch/delivery always human; batch intake is the ONLY auto-eligible op, no direct capital risk) |
| `src/pipeline/operation.cljc` | **OperationActor** -- langgraph StateGraph |
| `src/pipeline/sim.cljc` | demo driver |
| `test/pipeline/*_test.clj` | governor contract · phase invariants · store parity · registry conformance · facts coverage |

## Business-process coverage (honest)

This actor covers batch intake through pipeline-integrity / custody /
bonding-grounding regulatory assessment, batch dispatch and delivery
settlement -- the core governed lifecycle:

| Covered | Not covered (out of scope for this R0) |
|---|---|
| Batch intake + per-jurisdiction evidence checklisting, HARD-gated on an official spec-basis citation (`:batch/intake`/`:segment/verify`) | Real SCADA/pipeline-pig-robot integration, batch scheduling and throughput optimization |
| Batch dispatch, HARD-gated on full evidence, an in-window line pressure, a confirmed prior-segment POD chain, no unresolved contamination flag, a current integrity assessment, confirmed bonding/grounding, plus a double-dispatch guard (`:batch/dispatch`) | |
| Delivery settlement, HARD-gated on full evidence and no double-delivery (`:delivery/settle`) | |
| Immutable audit ledger for every intake/verification/dispatch/delivery decision | |

Extending coverage is additive: add the next gate (e.g. a batch-
scheduling optimization check) as its own governed op with its own
HARD checks and tests, following the SAME "an independent governor
re-verifies against the actor's own records before any real-world act"
pattern this repo's flagship ops already establish.

## Jurisdiction coverage (honest)

`pipeline.facts/coverage` reports how many requested jurisdictions
actually have an official spec-basis in `pipeline.facts/catalog` --
currently 4 seeded (JPN, USA, GBR, NOR) out of ~194 jurisdictions
worldwide. This is a starting catalog to prove the governor contract
end-to-end, not a claim of global coverage. Adding a jurisdiction is
additive: one map entry in `pipeline.facts/catalog`, citing a real
official source -- never fabricate a jurisdiction's requirements to
make coverage look bigger.

## Maturity

`:implemented` -- PipelineTransport advisor + Pipeline Integrity
Governor run as real, tested code (see `Run` above), promoted from the
originally-published `:blueprint`-tier scaffold, following the SAME
governed-actor architecture as the other prior actors across this
fleet, with its own distinct, independently-named governor and its own
self-contained pipeline-integrity range check. See
`docs/adr/0001-architecture.md` for the history and design.

## License

Code and implementation templates are AGPL-3.0-or-later.
