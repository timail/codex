# Fork maintenance

This fork adds opt-in, bounded retries for transient rate-limit errors. Read
[docs/fork-maintenance.md](docs/fork-maintenance.md) before changing the patch or
updating upstream.

- Keep the upstream diff small. Reuse existing retry, cancellation, configuration,
  and error-classification paths. Avoid unrelated cleanup or new dependencies.
- Keep extended waits opt-in. Preserve normal retry budgets and keep shared API
  classification fixes separate. Never broaden retries to authentication
  failures or known exhausted quota.
- Test retry timing with virtual time. Check cancellation, the deadline,
  `Retry-After`, and the normal retry path before accepting a change.
- Use the vendored [Ponytail](.agents/skills/ponytail/SKILL.md) instructions for
  implementation and [Ponytail Review](.agents/skills/ponytail-review/SKILL.md)
  for review. Read callers and tests, and report only concrete findings.
- Keep upstream updates manual and reviewable. Preserve the fork CI guards and
  upstream license. Do not introduce automated merges or release publishing.
- Follow the surrounding upstream conventions and any more specific `AGENTS.md`.
