# Maintaining this fork

This is a small patch series over [openai/codex](https://github.com/openai/codex).
The initial upstream base is
[`18352dc4e8757d652e4102a17abc76601847b8da`](https://github.com/openai/codex/commit/18352dc4e8757d652e4102a17abc76601847b8da).
The current upstream base is
[`d1d1ba0b837d34f314b7f469896e8babbc91f949`](https://github.com/openai/codex/commit/d1d1ba0b837d34f314b7f469896e8babbc91f949)
(19 November 2026). It includes the 79 upstream commits after the archive base,
including immediate SSE error delivery, scoped API spend-limit errors and the
latest sandbox permission fixes. The archive series was first reapplied to its
exact base and its tree matched `28441e415f5c859378a68aa0c5d49f365951e2fb`.
Both bases are development revisions, not a promise of stable-release compatibility.

At publication, the latest stable release was
[`rust-v0.170.0`](https://github.com/openai/codex/releases/tag/rust-v0.170.0).
The public advisory [GHSA-w5fx-fh39-j5rw](https://github.com/openai/codex/security/advisories/GHSA-w5fx-fh39-j5rw)
was fixed in 0.39.0; that fix predates this base. Reviewing public advisories and
release notes is not a guarantee that every vulnerability is known.
Keep the retry change separate from repository documentation and CI changes so
that either can be reviewed or replayed independently.

## Retry contract

- The opt-in feature is `long_rate_limit_retries`. It is disabled by default.
- Preserve the normal retry path first. A transient HTTP 429 or structured SSE
  rate-limit error can enter the additional retry period when that path would
  stop. A plain HTTP 429 without retry advice can reach that point immediately.
  Keep known exhausted-quota and usage-limit handling.
- Apply the extension to model-response sampling and foreground compaction,
  including legacy/local compaction used by compatible providers. A fully
  successful response ends the current outage; partial stream progress and
  steering do not restart its deadline.
- Wait at least 30 minutes before each additional attempt, and stop at an
  absolute deadline 12 hours after the additional retry period begins. A larger
  server `Retry-After` may reduce the number of attempts; never shorten it to
  squeeze another request into the deadline.
- Keep cancellation responsive while waiting. The feature cannot make a provider
  increase a limit or guarantee that a request will eventually succeed.
- Keep the interval and deadline fixed unless a real use case requires more
  configuration. Test the timing with a virtual clock.

The shared API parser also recognizes additional LiteLLM error forms. That
classification fix applies with the feature disabled; the additional retry
window requires the feature. The window limits when new attempts may start.
A response already in flight may finish.

## Review and update upstream

1. Review upstream releases and security advisories weekly and before updating
   your installed binary. This is a manual maintenance cadence, not an auto-merge.
   Add an `upstream` remote pointing to `https://github.com/openai/codex.git` if
   needed, then fetch its branches and tags. Review upstream release notes and
   security fixes. Prefer a suitable stable release for the next base; take a
   development revision only when the patch needs it, and record its exact SHA.
2. Create an update branch and a backup reference for the current fork. Replay
   the small fork commits on the selected revision. Resolve conflicts by reading
   the current retry and error flow, including every caller of changed helpers.
   Use `git range-diff` to review how the fork patch changed during the update.
3. Run the checks in [fork-ci.yml](../.github/workflows/fork-ci.yml), then review
   the candidate with `$ponytail-review`. Check disabled behavior, transient 429,
   quota errors, `Retry-After`, expiry, success, and cancellation. Broaden testing
   only for upstream changes that affect those paths.
4. Review newly added or changed upstream workflows before enabling Actions.
   Preserve the repository guards described below. Merge the update manually
   after the regression checks pass; do not automatically merge upstream.
5. Record the new upstream base here and keep the original Apache-2.0
   [license](../LICENSE) and notices intact. Remove the fork patch if upstream
   provides equivalent behavior.

## CI scope

`fork-ci.yml` provides one Linux job for formatting and focused retry, API,
protocol, and feature tests. It uses read-only repository permissions, a pinned checkout
action, a time limit, and concurrency cancellation. It caches Cargo dependency
sources using the same pinned cache action as upstream. It needs no repository
secrets and does not publish releases. It also tests the portable launcher and
builds the real CLI to verify flag overrides, resume parsing, and shared configuration. A successful run does not establish
macOS/Windows support or validate a live provider.

Run the same checks locally from `codex-rs`:

```shell
export RUST_MIN_STACK=8388608
cargo fmt --all -- --check
cargo test --locked -p codex-features --lib
cargo test --locked -p codex-protocol --lib rate_limit_recovery_exhaustion_ignores_retry_advice
cargo test --locked -p codex-api --lib
cargo test --locked -p codex-core --lib responses_retry::tests
cargo test --locked -p codex-core --lib config::schema::tests::config_schema_matches_fixture
cargo test --locked -p codex-core --test long_rate_limit_retries -- --nocapture
cargo build --locked -p codex-cli --bin codex --target-dir target
python3 ../scripts/test-codex-prolong.py target/debug/codex
```

The integration cases must actually run against their loopback servers. If the
environment sets `CODEX_SANDBOX_NETWORK_DISABLED`, upstream test helpers skip
them; that is not integration validation. The CI job fails on that guard instead
of accepting a skipped run. Keep the guard intact in restricted environments.

The first run can still download large git dependencies, including V8 submodules:
Cargo resolves the workspace and its patches even for selected `-p` packages.
The source cache speeds up later runs; it cannot remove the first download.
`setup-rusty-v8` controls build-time binary artifacts and does not avoid this
earlier source fetch. Keep `Cargo.lock` intact and wait for the actual tests to
finish before treating CI as validated.

The inherited automatic entry points in `blocking-ci.yml`, `postmerge-ci.yml`,
`rust-ci-full.yml`, `v8-canary.yml`, `rust-release.yml`, and
`rusty-v8-release.yml` are restricted to `openai/codex`. Existing upstream issue
and Python-release automation already has repository guards. Reusable workflows
remain in place to minimize the upstream diff. Some upstream workflows still
offer manual dispatch and may require OpenAI-specific runners or credentials;
use **Fork CI** for this fork.

After creating the GitHub fork, enable Actions only after these guards are on
its default branch. Run **Fork CI** and make its **Retry regression** job a
required check before merging changes. A local YAML check is not a completed
GitHub Actions run.

## Ponytail provenance

The repository includes only the original `ponytail` and `ponytail-review`
instruction files from
[`DietrichGebert/ponytail@96608e078b2d5ea607c5c3b8b5a54d1c1fba6846`](https://github.com/DietrichGebert/ponytail/tree/96608e078b2d5ea607c5c3b8b5a54d1c1fba6846).
They are unchanged copies of `skills/ponytail/SKILL.md` and
`skills/ponytail-review/SKILL.md`, under `.agents/skills`. Their original
[MIT license](../.agents/skills/PONYTAIL-LICENSE) is included alongside them.
No lifecycle hooks, executables, or global installation are needed.

Use `$ponytail` while implementing and `$ponytail-review` before merging. Apply
the review to the diff and connected code: correctness, concrete failure cases,
security, expected load, meaningful tests, and unnecessary complexity. The
review skill reports findings; it does not change code.

To update these skills, review a new upstream commit, copy those same two files
and `LICENSE`, verify their contents and license, and update the pin above in the
same commit. Keep the upstream files intact rather than maintaining a rewritten
skill with the same name.
