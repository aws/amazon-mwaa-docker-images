#!/bin/bash
#
# Vendor the Monaco editor locally instead of loading it from cdn.jsdelivr.net.
#
# Background:
#   Apache Airflow 3.2.0/3.2.1 ship a UI that lazy-loads the Monaco editor runtime from
#   https://cdn.jsdelivr.net/npm/monaco-editor@0.52.2/min/vs at runtime, via @monaco-editor/loader's default CDN
#   base URL. This breaks the JSON/code editor surfaces (DAG Code, XComs, Rendered Templates, Connection Edit
#   extra, Trigger-with-config, Audit Log JSON) for restricted-egress (PRIVATE_ONLY) customers, and loads
#   unverified third-party JS into authenticated sessions for everyone else. Fixed upstream in Airflow 3.2.2
#   (apache/airflow#66647). This script backports the effect onto 3.2.1 by serving the identical Monaco bytes
#   from the Airflow webserver's own origin.
#
#   REMOVE this script on versions higher than 3.2.1.
#

set -euo pipefail

# shellcheck source=images/airflow/3.2.1/bootstrap/common.sh
source /bootstrap/common.sh

MONACO_VERSION="0.52.2"
MONACO_TARBALL_URL="https://registry.npmjs.org/monaco-editor/-/monaco-editor-${MONACO_VERSION}.tgz"
# sha512 (hex) of monaco-editor-0.52.2.tgz as published on the npm registry.
MONACO_TARBALL_SHA512="18441611999f90e18b75ddd72bcaf2adf5b3dc020ff18ca655788f1c475ec2b52aee6874aab2ab7c72cd09771b07ab139cc2e7399df3b5288a71c885524209c1"

# Where the Airflow webserver serves the UI dist. The dist is served under the /static/ prefix (the app's own
# bundle loads from /static/assets/index-<hash>.js), so ui/dist/ maps to the /static/ URL space. The
# package.json "homepage": "/ui" is only the client-side router base, not a file-serving prefix: requests under
# /ui/* that are not real files hit the SPA catch-all and return index.html. Monaco therefore goes physically in
# ui/dist/assets/monaco/<v>/ and is served at /static/assets/monaco/<v>/.
STATIC_SUBPATH="assets/monaco/${MONACO_VERSION}"
NEW_BASE="/static/${STATIC_SUBPATH}/min/vs"
OLD_BASE="https://cdn.jsdelivr.net/npm/monaco-editor@${MONACO_VERSION}/min/vs"

# Locate the installed Airflow UI dist directory.
UI_DIST="$(python3 -c 'import os, airflow; print(os.path.join(os.path.dirname(airflow.__file__), "ui", "dist"))')"
test -d "${UI_DIST}" || { echo "ERROR: Airflow UI dist not found at ${UI_DIST}" >&2; exit 1; }

DEST="${UI_DIST}/${STATIC_SUBPATH}"

# 1) Download Monaco from the npm registry and verify integrity.
TMP_TARBALL="$(mktemp /tmp/monaco-editor.XXXXXX.tgz)"
curl -fsSL "${MONACO_TARBALL_URL}" -o "${TMP_TARBALL}"
echo "${MONACO_TARBALL_SHA512}  ${TMP_TARBALL}" | sha512sum -c -

# 2) Extract the AMD distribution (min/vs) and the MIT LICENSE into the dist, stripping the leading "package/".
#    Note: we use Python's tarfile (not `tar`) because the AL2023 base ships no `tar` and this step runs as the
#    unprivileged airflow user (cannot install one); Python 3 is always present (it runs Airflow) and handles
#    gzip internally.
mkdir -p "${DEST}"
python3 - "${TMP_TARBALL}" "${DEST}" <<'PY'
import sys, tarfile

tarball, dest = sys.argv[1], sys.argv[2]
with tarfile.open(tarball, "r:gz") as tar:
    selected = []
    for member in tar.getmembers():
        # npm tarballs wrap everything under a leading "package/" directory.
        if "/" not in member.name:
            continue
        rel = member.name.split("/", 1)[1]  # strip the leading "package/"
        if rel == "LICENSE" or rel == "min" or rel.startswith("min/"):
            member.name = rel
            selected.append(member)
    # filter="data" sanitizes member paths (blocks absolute/.. traversal). The filter arg exists since Python
    # 3.12 (and backport releases) and becomes the default in 3.14; we set it explicitly. Harmless here.
    tar.extractall(path=dest, members=selected, filter="data")
print(f"Extracted {len(selected)} Monaco members to {dest}")
PY
rm -f "${TMP_TARBALL}"

# 3) Pre-flight: locate the CDN base URL literal in the bundle. It normally appears (the app ships the jsDelivr
#    default), but a re-run of this script will already have rewritten it, so we also accept an already-rewritten
#    bundle as a no-op. Only a bundle with neither the old nor the new base is a genuine surprise; fail loudly
#    there rather than silently ship an inert patch.
mapfile -t MATCHES < <(grep -rl "${OLD_BASE}" "${UI_DIST}" || true)
if [ "${#MATCHES[@]}" -eq 0 ]; then
  if grep -rq "${NEW_BASE}" "${UI_DIST}"; then
    echo "Monaco CDN base already rewritten to ${NEW_BASE}; skipping rewrite."
  else
    echo "ERROR: neither '${OLD_BASE}' nor '${NEW_BASE}' found in ${UI_DIST}; bundle structure changed?" >&2
    exit 1
  fi
fi

# 4) The fix: rewrite the CDN base URL being used to the local path in each file that contains it. (No-op when
#    MATCHES is empty, i.e. the bundle was already rewritten by a prior run.)
for f in "${MATCHES[@]}"; do
  sed -i "s|${OLD_BASE}|${NEW_BASE}|g" "${f}"
done

# 5) Post-flight verification:
#      - No cdn.jsdelivr.net references remain anywhere in the UI dist.
#      - A representative set of vendored Monaco assets exists at the expected local paths (a sanity check that
#        the tree extracted and the rewrite landed, not a per-page completeness guarantee): the AMD loader, the
#        editor core JS and CSS, the worker bootstrap, a language contribution, and a font.
if grep -rq "cdn.jsdelivr.net" "${UI_DIST}"; then
  echo "ERROR: cdn.jsdelivr.net still referenced in ${UI_DIST} after rewrite" >&2; exit 1
fi
for rel in \
    "min/vs/loader.js" \
    "min/vs/editor/editor.main.js" \
    "min/vs/editor/editor.main.css" \
    "min/vs/base/worker/workerMain.js" \
    "min/vs/basic-languages/python/python.js" \
    "min/vs/base/browser/ui/codicons/codicon/codicon.ttf" ; do
  test -f "${DEST}/${rel}" || { echo "ERROR: expected Monaco asset missing: ${DEST}/${rel}" >&2; exit 1; }
done

# 6) Give Monaco's Web Workers an absolute base URL to load their code from.
#    Why it's needed: Monaco runs language features (e.g. JSON validation) in background Web Workers that fetch
#      additional Monaco files at runtime, relative to a base URL. The old absolute CDN base worked; our
#      same-origin path-absolute base (${NEW_BASE}) can't be resolved inside a Worker, so those files (e.g.
#      jsonWorker.js) fail to load and JSON/language features throw (the editor itself still renders).
#    Fix: MonacoEnvironment is the global hook Monaco reads to spawn workers, and the Airflow UI never sets it.
#      Define its getWorkerUrl to boot the worker from an absolute base (location.origin + the local path) built
#      at runtime.
WORKER_ENV_REL="assets/monaco/monaco-worker-env.js"
# baseUrl must be the directory that CONTAINS vs (".../min"), because Monaco resolves worker sub-modules by
# appending "vs/..." to it; pointing it at vs itself yields ".../min/vs/vs/..." (a 404). importScripts loads the
# worker bootstrap from the vs dir explicitly.
WORKER_MIN_URLPATH="${NEW_BASE%/vs}"
cat > "${UI_DIST}/${WORKER_ENV_REL}" <<EOF
// Injected by 002-vendor-monaco-editor.sh (Airflow 3.2.1 Monaco CDN backport).
// REMOVE together with that script on versions higher than 3.2.1.
(function () {
  var minBase = self.location.origin + "${WORKER_MIN_URLPATH}";
  var vsBase = self.location.origin + "${NEW_BASE}";
  self.MonacoEnvironment = {
    getWorkerUrl: function () {
      var code =
        "self.MonacoEnvironment={baseUrl:'" + minBase + "'};" +
        "importScripts('" + vsBase + "/base/worker/workerMain.js');";
      return URL.createObjectURL(new Blob([code], { type: "text/javascript" }));
    }
  };
})();
EOF

# Reference the shim from index.html as a classic (non-module) script so it runs before the deferred app module
# and sets MonacoEnvironment in time.
INDEX_HTML="${UI_DIST}/index.html"
test -f "${INDEX_HTML}" || { echo "ERROR: index.html not found at ${INDEX_HTML}" >&2; exit 1; }
python3 - "${INDEX_HTML}" "${WORKER_ENV_REL}" <<'PY'
import sys

index_path, worker_rel = sys.argv[1], sys.argv[2]
with open(index_path, "r", encoding="utf-8") as fh:
    html = fh.read()
tag = f'<script src="./static/{worker_rel}"></script>'
if worker_rel in html:
    print("index.html already references the Monaco worker-env shim; skipping")
else:
    marker = '<script type="module"'
    i = html.find(marker)
    if i == -1:
        sys.exit("ERROR: module script tag not found in index.html")
    html = html[:i] + tag + "\n    " + html[i:]
    with open(index_path, "w", encoding="utf-8") as fh:
        fh.write(html)
    print("Injected Monaco worker-env shim into index.html")
PY

# Post-flight for the worker-env shim.
test -f "${UI_DIST}/${WORKER_ENV_REL}" || { echo "ERROR: worker-env shim missing: ${UI_DIST}/${WORKER_ENV_REL}" >&2; exit 1; }
grep -q "monaco-worker-env.js" "${INDEX_HTML}" || { echo "ERROR: index.html does not reference the worker-env shim" >&2; exit 1; }

echo "Monaco ${MONACO_VERSION} vendored at ${DEST}; CDN base rewritten to ${NEW_BASE}; worker-env shim installed."
