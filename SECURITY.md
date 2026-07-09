# Security Policy

This project handles pipeline-integrity and custody workflows. Treat
vulnerabilities as potentially high impact even when the demo data is
synthetic.

## Do Not Disclose Publicly

Report privately before opening public issues for:

- credential exposure
- real operator, custody-counterparty, or customer data exposure
- authorization bypass
- Pipeline Integrity Governor bypass
- audit-ledger tampering
- over-disclosure in reports or exports
- tenant isolation failures

## Reporting

Use GitHub private vulnerability reporting when available for the repository.
If that is unavailable, contact the repository maintainers through the
cloud-itonami organization before publishing details.

Include:

- affected commit or version
- reproduction steps
- expected and actual behavior
- impact on pipeline-integrity enforcement, custody data, or audit logging
- suggested fix, if known

## Production Guidance

- Store secrets outside Git.
- Keep real line, batch, and custody data outside this repository.
- Run governor/phase/store policy tests before deployment.
- Export and review audit logs regularly.
- Use least privilege for operators and service accounts.
