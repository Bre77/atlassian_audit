#!/bin/bash
cd "${0%/*}"
OUTPUT="${1:-atlassian_audit.spl}"
chmod -R u=rwX,go= *
chmod -R u-x+X *
chmod -R u=rwx,go= bin/*
# splunk_input_runtime 1.0.0, pinned to commit fcc942fa00b91b6ca56e35040b26b57cf2189f83.
# Archive sha256:0977db3c1199c5a8c02f1e3a5e52142a5f87d6969b2902d6d36242eee460ba8d
# It ships only as this GitHub archive (no wheels, no PyPI release yet), so install it
# without wheel/platform constraints, same as splunk-sdk was installed before it.
python3.9 -m pip install --upgrade -t lib --no-dependencies "splunk_input_runtime @ https://github.com/Bre77/splunk-input-runtime/archive/fcc942fa00b91b6ca56e35040b26b57cf2189f83.zip"
# Vendor the HTTP/TLS stack (requests + certifi + urllib3 + idna + charset-normalizer)
# as Linux cp39 wheels so the add-on is self-contained on Splunk 10 indexers/HFs
# (OpenSSL 3.0 / Python 3.9) regardless of the build host's OS.
python3.9 -m pip install --upgrade -t lib -r lib/requirements.txt \
    --platform manylinux2014_x86_64 --python-version 3.9 --only-binary=:all:
# charset-normalizer's mypyc speedup is x86_64-only and fails AppInspect's
# AArch64 check; deleting it falls back to charset-normalizer's pure-Python
# implementation, same as splunk_nats does for its native .so chains.
find lib/charset_normalizer -name '*.so' -delete
find lib -maxdepth 1 -name '*mypyc*.so' -delete
# Strip bytecode caches (any depth) and pip console-script shims so the package
# is clean for AppInspect / Splunkbase.
find lib bin -type d -name __pycache__ -prune -exec rm -rf {} +
rm -rf lib/bin
# pip-installed wheels can ship files with execute bits set; strip them the
# same way as the rest of the package now that lib/ is fully populated.
chmod -R u=rwX,go= lib
chmod -R u-x+X lib
cd ..
tar -cpzf $OUTPUT --exclude=.* --overwrite atlassian_audit 
