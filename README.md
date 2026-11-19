# Codex Prolong

A small, unofficial fork of [openai/codex](https://github.com/openai/codex).
After ordinary retries stop on a transient HTTP 429 or structured SSE rate limit,
`codex-prolong` retries at least 30 minutes apart for up to 12 additional hours.
It respects longer `Retry-After`, supports cancellation, and keeps the same turn
and completed tool results. Generation and foreground compaction are covered;
known authentication, quota, budget and policy errors remain terminal.

## Install alongside Codex (Linux)

Ubuntu/Debian build dependencies (other distributions need equivalent packages):

```sh
sudo apt-get update
sudo apt-get install -y build-essential pkg-config libssl-dev libcap-dev \
  libasound2-dev cmake clang python3 git curl ca-certificates
```

Use an existing [rustup](https://rust-lang.github.io/rustup/installation/index.html)
installation, or download and inspect its official installer before running it:

```sh
curl --proto '=https' --tlsv1.2 -fsS https://sh.rustup.rs -o rustup-init.sh
less rustup-init.sh
sh rustup-init.sh --profile minimal --no-modify-path --default-toolchain none
```

Build with the toolchain pinned in `codex-rs/rust-toolchain.toml`:

```sh
git clone --branch main --single-branch https://github.com/timail/codex.git codex-prolong-src
cd codex-prolong-src/codex-rs
"$HOME/.cargo/bin/cargo" build --locked --release -p codex-cli --bin codex --target-dir target
cd ..
python3 scripts/test-codex-prolong.py codex-rs/target/release/codex
install -d "$HOME/.local/lib/codex-prolong" "$HOME/.local/bin"
install -m 755 codex-rs/target/release/codex "$HOME/.local/lib/codex-prolong/codex"
install -m 755 scripts/codex-prolong "$HOME/.local/bin/codex-prolong"
"$HOME/.local/bin/codex-prolong" features list
"$HOME/.local/bin/codex-prolong" resume --last
```

The build downloads substantial dependencies. The explicit `--target-dir` fixes
the binary path even if `CARGO_TARGET_DIR` is set. If `~/.local/bin` is already on
your `PATH`, use `codex-prolong` directly; otherwise use its full path above.
No `PATH` change or replacement of the existing `codex` is needed.

The wrapper runs the separate binary with `--enable long_rate_limit_retries`
and forwards all arguments, including `exec` and `resume`. Both commands use the
same ordinary `~/.codex/config.toml`, authentication and sessions. An existing
`CODEX_HOME` override is passed through unchanged; use the same value for both
commands and ensure that directory exists. Existing LiteLLM settings and token
sources are reused. Keep the feature out of the shared config: the wrapper enables
it only for its own process. Use `codex-prolong --disable long_rate_limit_retries`
to disable the extra waiting for one invocation.

## Update and verify

In a clean checkout, run `git pull --ff-only origin main`, then repeat the build,
smoke test and two `install -m 755` commands above. `features list` must show
`long_rate_limit_retries` as `true` through the wrapper. Run the local 429 tests
and follow the manual upstream/security update procedure in
[maintenance](docs/fork-maintenance.md); they use virtual time and loopback
servers, without real provider tokens or a 12-hour wait.

[Upstream README](docs/upstream-README.md) is preserved for reference.
[Apache-2.0](LICENSE); the two vendored Ponytail skills retain their MIT license.
