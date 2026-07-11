# Project agent memory

This file is the project's committed home for project-intrinsic agent knowledge: build, test, release, architecture, and sharp-edge notes that should travel with the code.

- Add durable project-specific notes here as they are discovered through real work.
- `lib/` is gitignored except `lib/requirements.txt`; `.build.sh` vendors everything else at build time.
- `splunklib`'s version is pinned in `.build.sh` (the `pip install ... splunk-sdk` line), not in `lib/requirements.txt`: splunk-sdk is sdist-only (no wheels), so it's installed in a separate pip call without the `--only-binary=:all:`/`--platform` constraints used for the requests/certifi/urllib3 stack.
- CI (`.github/workflows/validate.yml`) calls the reusable build+AppInspect workflow from `Bre77/splunk_nats@main`, credential-free (no publish step). This is a plain modular-input TA (no SplunkUI/React bundle), so `use_ucc_gen: false`; ucc-gen apps set it `true`.

## Maintaining this file

Keep this file for knowledge useful to almost every future agent session in this project.
Do not repeat what the codebase already shows; point to the authoritative file or command instead.
Prefer rewriting or pruning existing entries over appending new ones.
When updating this file, preserve this bar for all agents and keep entries concise.
