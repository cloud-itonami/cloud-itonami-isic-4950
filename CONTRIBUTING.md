# Contributing

`cloud-itonami-isic-4950` accepts contributions to the OSS blueprint,
governor/policy tests, documentation and operator model.

## Development
This repo holds the business blueprint, the self-contained
`pipeline.registry` pipeline-integrity range check, and the operator
contracts. There is no separate `kotoba-lang/pipeline` capability library --
the domain logic lives here.

```bash
clojure -M:dev:test
clojure -M:lint
```

## Rules
- Do not commit real customer, line, batch, or custody data.
- Keep verify, dispatch, and delivery behind the Pipeline Integrity Governor.
- Treat pipeline workflows as high-risk: add tests for line-pressure, POD
  chain, contamination, integrity currency, bonding/grounding, custody and
  audit logging.
- Document any new business-model or operator assumption in `docs/`.

## Pull Requests
PRs should describe: what behavior changed, which policy invariant is
affected, how it was tested, whether operator or certification docs need
updates.
