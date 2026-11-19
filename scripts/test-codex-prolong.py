#!/usr/bin/env python3
"""Test the wrapper; optionally pass a built codex binary to test CLI/config too."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

WRAPPER = Path(__file__).with_name("codex-prolong")
FEATURE = "long_rate_limit_retries"


def run(command, env, cwd):
    return subprocess.run(command, env=env, cwd=cwd, text=True, capture_output=True, timeout=30)


def feature_value(output, name):
    return next(line.split()[-1] for line in output.splitlines() if line.split()[:1] == [name])


with tempfile.TemporaryDirectory(prefix="codex-prolong ") as directory:
    root = Path(directory)
    home = root / "home with spaces"
    binary = home / ".local/lib/codex-prolong/codex"
    binary.parent.mkdir(parents=True)
    wrapper = home / ".local/bin/codex-prolong"
    wrapper.parent.mkdir(parents=True)
    shutil.copy2(WRAPPER, wrapper)
    # A private child environment: never read or modify the user's real Codex data.
    env = dict(os.environ, HOME=str(home))
    env.pop("CODEX_HOME", None)
    binary.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "print(json.dumps({'argv': sys.argv[1:], 'home': os.environ.get('CODEX_HOME'), "
        "'path': os.environ.get('PATH')}))\n"
        "sys.exit(23)\n"
    )
    binary.chmod(0o755)
    for custom_home in [None, str(root / "custom codex home")]:
        if custom_home is not None:
            env["CODEX_HOME"] = custom_home
        for args in [[], ["resume", "--last"], ["exec", "resume", "session-id", "a b", "", "*", "$HOME"],
                     ["--disable", FEATURE, "features", "list"]]:
            result = run([str(wrapper), *args], env, root)
            assert result.returncode == 23, result.stderr
            assert json.loads(result.stdout) == {
                "argv": ["--enable", FEATURE, *args],
                "home": custom_home,
                "path": env.get("PATH"),
            }
    print("Wrapper: arguments, resume, exit status, PATH and CODEX_HOME preserved.")

    if len(sys.argv) > 1:
        binary.unlink()
        binary.symlink_to(Path(sys.argv[1]).resolve(strict=True))
        for custom_home in [None, str(root / "custom codex home")]:
            env.pop("CODEX_HOME", None)
            if custom_home is not None:
                env["CODEX_HOME"] = custom_home
            config_home = Path(custom_home) if custom_home else home / ".codex"
            config_home.mkdir(exist_ok=True)
            config = config_home / "config.toml"
            content = '[features]\nlong_rate_limit_retries = false\nshell_tool = false\n'
            config.write_text(content)
            for command, enabled in [([str(binary)], "false"), ([str(wrapper)], "true"),
                                     ([str(wrapper), "--disable", FEATURE], "false")]:
                result = run([*command, "features", "list"], env, root)
                assert result.returncode == 0, result.stderr
                assert feature_value(result.stdout, FEATURE) == enabled, result.stdout
                assert feature_value(result.stdout, "shell_tool") == "false", result.stdout
            for args in [["resume", "--help"], ["exec", "resume", "--help"]]:
                result = run([str(wrapper), *args], env, root)
                assert result.returncode == 0, result.stderr
                assert "resume" in result.stdout.lower(), result.stdout
            assert config.read_text() == content, "CLI changed the shared config"
        print("Built CLI: flag defaults, disable override, shared config and resume parsing verified.")
