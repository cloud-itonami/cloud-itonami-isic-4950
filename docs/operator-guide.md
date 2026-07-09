# Operator Guide

## First Deployment
1. Register operators, lines/segments, terminals, batches, and pipeline
   controllers.
2. Import batch, line-integrity, and custody/delivery history.
3. Seed the per-jurisdiction spec-basis catalog (`pipeline.facts`) for the
   jurisdictions you actually operate in, citing real official sources only.
4. Run read-only spec-basis validation per jurisdiction.
5. Configure contamination-flag escalation and pipeline-accounting accounts.
6. Publish a dry-run delivery settlement and audit export.

## Minimum Production Controls
- spec-basis validation before any verification, dispatch, or delivery
- full pipeline-integrity / custody / bonding-grounding evidence
  (ILI/pigging/hydrotest assessment, batch custody/POD record,
  bonding-grounding confirmation) before any dispatch
- line-pressure window, POD-chain, contamination-flag, integrity-currency and
  bonding-grounding checks before any dispatch
- contamination-flag escalation gate
- audit export for every dispatch, delivery, and hold
- backup manual dispatch and delivery-settlement process

## A Day in the Life: Intake → Verify → Dispatch → Settle → Audit

Community Pipeline Transport (ISIC 4950, `cloud-itonami-isic-4950`) runs on
the same intake / advise / govern / decide / commit-or-hold loop as every
itonami blueprint, but here the loop is concrete: a regional pipeline operator
needs to bring a batch (say, a diesel-grade batch moving from the
Akita-Onsen Terminal to Refinery-Akita) from intake through segment-integrity
assessment to a batch dispatch and a delivery settlement. Walking through one
batch, end to end:

1. **Intake.** The operator books the batch through `:forms`: batch id,
   origin and destination terminals, product grade, volume, jurisdiction, and
   the batch's own physical record (measured line pressure with its safe
   window [min, max], prior-segment POD-confirmed flag, integrity-assessment-
   current flag, contamination flag and its resolved flag, bonding-grounding-
   confirmed flag). This creates a batch record at `:batch/intake` status. The
   PipelineTransport advisor only normalizes the patch; it does not invent the
   batch id, terminals, product grade, jurisdiction, or any physical value.
2. **Verify.** The PipelineTransport advisor drafts a per-jurisdiction
   pipeline-integrity / custody / bonding-grounding evidence checklist
   (`:segment/verify`) from `pipeline.facts`, citing the jurisdiction's
   official spec-basis (owner authority, legal basis, provenance) and listing
   the required evidence (ILI/pigging/hydrotest integrity assessment, batch
   custody/POD record, bonding-grounding confirmation). The
   `:pipeline-integrity-governor` sign-off gate must clear: it checks the
   jurisdiction actually has an official spec-basis on file (never invent one).
   A jurisdiction with no spec-basis is a HARD hold at the governor node -- it
   never even reaches a human. This verification always escalates to a human
   for approval; it is never auto.
3. **Dispatch.** Before the batch can flow, the `:pipeline-integrity-governor`
   sign-off gate runs the full HARD check set against the batch's own ground
   truth: the spec-basis exists, the evidence checklist is complete, the
   measured line pressure is inside `[min, max]`, the prior-segment POD chain
   is confirmed, no contamination flag is open, the integrity assessment is
   current, bonding/grounding is confirmed, and the batch has not already been
   dispatched. Any failure is a HARD hold that a human cannot override. If
   every check is clean, the proposal STILL always escalates to a human
   pipeline controller -- a `:batch/dispatch` never auto-commits at any phase.
   On approval, the dispatch record is drafted (`<JURISDICTION>-DISPATCH-000001`)
   and the batch's `:dispatched?` flag is set.
4. **Settle.** Once the batch has actually been dispatched and delivered to
   the destination terminal, the delivery is settled (`:delivery/settle`):
   custody transfer, volume / grade reconciliation. The governor re-checks the
   spec-basis, the evidence completeness, and that this batch's delivery has
   not already been settled. As with the dispatch, a clean delivery STILL
   always escalates to a human pipeline controller -- `:delivery/settle` never
   auto-commits. On approval the delivery record is drafted
   (`<JURISDICTION>-DELIVERY-000001`) and the batch's `:delivered?` flag is set.
5. **Audit.** The assessment, the dispatch sign-off, the dispatch record, the
   delivery sign-off, and the delivery record are all appended to the
   `:audit-ledger` -- immutable and exportable, so a custody or volume dispute
   can be traced back to the exact spec-basis citation, evidence checklist,
   and controller sign-off that authorized the dispatch and delivery. If
   something is wrong with the batch (a pressure anomaly, a product interface
   / grade contamination concern, a stale integrity assessment), that gets
   raised as a contamination flag / integrity flag and routed through the
   escalation gate instead of being silently suppressed -- a dispatch for that
   batch then waits on governor sign-off of the flag's resolution.

Any deviation from this loop is exactly what the Trust Controls in
`docs/business-model.md` exist to catch: a batch verified against a fabricated
spec-basis, a dispatch started with incomplete evidence or outside the line-
pressure window, a contamination flag suppressed to force a dispatch through,
or a delivery posted without a human sign-off.

## Feel the Decision Gate: `clojure -M:dev:run`

This vertical has no companion playable prototype yet (unlike the freight
sibling's `itonami/freight-dispatch` game). The fastest hands-on way to feel
why the `:pipeline-integrity-governor` gate exists is the bundled demo, which
walks one clean batch through intake → verify → dispatch → settle (each
dispatch/delivery pausing for human approval) and then exercises every
HARD-hold failure mode in isolation:

- a jurisdiction with no official spec-basis → HOLD (`:no-spec-basis`),
- a measured line pressure outside the safe window → HOLD
  (`:line-pressure-out-of-range`),
- a broken prior-segment POD chain → HOLD (`:pod-chain-integrity-broken`),
- an unresolved product contamination flag → HOLD
  (`:product-contamination-flag-unresolved`),
- a stale integrity assessment → HOLD (`:integrity-assessment-stale`),
- unconfirmed bonding/grounding → HOLD (`:bonding-grounding-unconfirmed`),
- a double dispatch of the same batch → HOLD (`:already-dispatched`),
- a double delivery of the same batch → HOLD (`:already-delivered`).

Each HOLD settles at the governor node and never reaches a human approver --
the same failure mode the audit ledger is built to catch and the minimum
production controls above are built to prevent. It is not a substitute for
those controls, but it is the fastest way for a new operator (or a reviewer)
to feel, hands-on, why the gate exists before touching a real deployment.

## Certification
Certified operators must prove spec-basis-grounded verification, evidence-
backed dispatch readiness (line pressure, POD chain, contamination flag,
integrity currency, bonding/grounding), and human review for every dispatch-
and delivery-affecting action.
