# Security Policy

## Reporting a Vulnerability

Please **do not** open a public GitHub issue for security vulnerabilities.

Instead, use [GitHub Security Advisories](https://github.com/edgesentinel/edgesentinel/security/advisories/new)
to report privately. We aim to acknowledge reports within 5 business days.

Include, where possible:

- A description of the vulnerability and its impact.
- Steps to reproduce, or a minimal proof of concept.
- The edgesentinel version and Python version affected.

## Supported Versions

edgesentinel follows semantic versioning. Until 1.0.0, only the latest minor
release receives security fixes.

## Design Considerations

edgesentinel persists application data locally (SQLite journal, operation
payloads) on the edge device it runs on. Relevant threat-model notes:

- **Persisted payloads may be sensitive.** edgesentinel does not encrypt the
  SQLite database at rest -- treat `data_dir` as sensitive local storage
  and rely on filesystem/disk encryption if your device requires it.
- **Never pass secrets as plain operation payloads** you don't want
  persisted in the clear. A redaction mechanism for logging is planned
  (see the project roadmap); it does not yet exist and no component should
  be assumed to redact secrets on your behalf.
- **The CLI is local-only.** It reads the local SQLite database directly
  and has no network listener.
- edgesentinel has no default network egress. Any network calls belong to the
  optional integrations you explicitly configure (MQTT, HTTP), not the
  core runtime.
