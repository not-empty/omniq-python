# omniq (Python SDK) — v1

Python client for **OmniQ Core (Redis + Lua)**.

## How scripts are managed

This SDK expects a `scripts/` folder containing the Lua scripts from **omniq-core**.

Recommended: add **omniq-core** as a Git submodule pinned to a release tag:

```bash
git submodule add https://github.com/<ORG>/omniq-core.git scripts
cd scripts && git checkout v1.0.0 && cd ..
git add .gitmodules scripts
git commit -m "Pin omniq-core scripts to v1.0.0"
```

## Install (editable)

```bash
pip install -e .
```

## Quick start

Publish:

```bash
python examples/publish.py
```

Consume:

```bash
python examples/consumer.py
```

## Notes

- Payload MUST be a dict/list (structured JSON). Raw strings are rejected by `publish()`.
- `consume()` starts a heartbeat thread and uses token-gated ACKs.
