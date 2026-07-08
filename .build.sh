#!/bin/bash
cd "${0%/*}"
OUTPUT="${1:-atlassian_audit.spl}"
chmod -R u=rwX,go= *
chmod -R u-x+X *
chmod -R u=rwx,go= bin/*
# splunk-sdk (provides splunklib) is sdist-only and pure Python, so install it
# without wheel/platform constraints.
python3.9 -m pip install --upgrade -t lib --no-dependencies "splunk-sdk>=2.1.1,<3"
# Vendor the HTTP/TLS stack (requests + certifi + urllib3 + idna + charset-normalizer)
# as Linux cp39 wheels so the add-on is self-contained on Splunk 10 indexers/HFs
# (OpenSSL 3.0 / Python 3.9) regardless of the build host's OS.
python3.9 -m pip install --upgrade -t lib -r lib/requirements.txt \
    --platform manylinux2014_x86_64 --python-version 3.9 --only-binary=:all:
# Strip bytecode caches (any depth) and pip console-script shims so the package
# is clean for AppInspect / Splunkbase.
find lib bin -type d -name __pycache__ -prune -exec rm -rf {} +
rm -rf lib/bin
cd ..
tar -cpzf $OUTPUT --exclude=.* --overwrite atlassian_audit 
