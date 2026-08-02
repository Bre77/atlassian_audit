# Project agent memory

This file is the project's committed home for project-intrinsic agent knowledge: build, test, release, architecture, and sharp-edge notes that should travel with the code.

- Add durable project-specific notes here as they are discovered through real work.
- `lib/` is gitignored except `lib/requirements.txt`; `.build.sh` vendors everything else at build time.
- This app runs on `splunk_input_runtime` (https://github.com/Bre77/splunk-input-runtime), not `splunklib`/`splunk-sdk`. It's pinned by commit in `.build.sh` (the first `pip install ... splunk_input_runtime @ https://...` line), not in `lib/requirements.txt`: it ships only as a GitHub archive (no wheels, no PyPI release yet), so - like `splunk-sdk` before it - it's installed in a separate pip call without the `--only-binary=:all:`/`--platform` constraints used for the requests/certifi/urllib3 stack in `lib/requirements.txt`. Bump both the commit and the recorded `sha256:` comment together when the runtime releases a new version; never point at a branch.
- Credential handling goes through `self.context.credentials.protect_input_fields(...)` (see `bin/atlassian_audit.py`), not manual `storage_passwords` list/delete/create calls. This preserves the credential identity `(owner=nobody, app=atlassian_audit, realm=<stanza name>, username=key)` that existing installs already have - do not change that tuple without a captain-level decision; it strands users' stored secrets.
- The runner (`Script.run_script`) owns `EventWriter.close()`; app code must not call it explicitly.
- `Bre77/SplunkUI-devcontainer`'s `test-harness/verify-splunklib-app.sh` and `test-harness/credential-continuity-gate.sh` are app-agnostic but assume a single `lib/requirements.txt` fully declares the app's dependency (they `pip install --target lib -r lib/requirements.txt`, never running the app's own `.build.sh`). Because this app's runtime/SDK pin lives in `.build.sh` instead, point the harness at a scratch copy of the app whose `lib/requirements.txt` has that pin appended, not at the repo checkout directly.
- CI (`.github/workflows/validate.yml`) calls the reusable build+AppInspect workflow from `Bre77/splunk_nats@main`, credential-free (no publish step). This is a plain modular-input TA (no SplunkUI/React bundle), so `use_ucc_gen: false` and `build_command: "./.build.sh"` - AppInspect scans the `.spl` that `.build.sh` produces, not a raw tar of the repo, since the repo root mixes app content with repo tooling. `package_glob: "../*.spl"` matches where `.build.sh` writes its output (one directory above `app_dir`, per its `cd ..` before `tar`).
- Version is recorded in two places that must stay in sync: `package.json` and `[launcher] version =` in `default/app.conf` (there is no `[id]` stanza in this app's `app.conf`).

## Maintaining this file

Keep this file for knowledge useful to almost every future agent session in this project.
Do not repeat what the codebase already shows; point to the authoritative file or command instead.
Prefer rewriting or pruning existing entries over appending new ones.
When updating this file, preserve this bar for all agents and keep entries concise.
