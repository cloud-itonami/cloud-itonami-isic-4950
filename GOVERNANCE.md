# Governance

`cloud-itonami-isic-4950` is an OSS open-business blueprint for community
pipeline transport.

## Maintainers
Maintainers may merge changes that preserve these invariants:
- batches whose jurisdiction has no official spec-basis can never be verified,
  dispatched, or delivered.
- the Pipeline Integrity Governor remains independent of the advisor.
- hard policy violations (a line pressure outside the safe window, a broken
  POD chain, an unresolved contamination flag, a stale integrity assessment,
  unconfirmed bonding/grounding, double-dispatch/delivery) cannot be
  overridden by human approval.
- every verify, dispatch, delivery, and hold path is auditable.
- customer, line, and custody data stays outside Git.

## Decision Records
Architecture decisions live in `docs/adr/`. Changes to the trust model, storage contract, public business model, operator certification or license should add or update an ADR.

## Operator Governance
Anyone may fork and operate independently. itonami.cloud certification is a separate trust mark and should require security, audit and data-flow review.

Certified operators can lose certification for:
- bypassing dispatch or delivery policy checks
- mishandling customer, line, or custody data
- misrepresenting certification status
- failing to respond to security incidents
