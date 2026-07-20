"""
cloud_run/main.py

Triggered by the GitHub Actions workflow on every push to main.
Given a before/after commit SHA, it:
  1. Clones the repo and diffs the two commits.
  2. Pulls out changed .sql files (resources/sql/) and their paired
     Beam/Spark consumer code (src/beam/).
  3. Dry-runs the SQL at both commits (before vs after) — free,
     deterministic cost signal, no Gemini involved.
  4. Sends the changed SQL + consumer code + schema to Gemini
     (via a Vertex AI context cache) asking for a rewrite that
     preserves business logic.
  5. Dry-runs Gemini's suggested rewrite too.
  6. Posts a comment on the PR/commit with all three numbers.

This is a hackathon-scoped skeleton: no retries, minimal error
handling, --allow-unauthenticated on the Cloud Run service for
simplicity. Tighten both before using it on anything real.
"""
import os
import subprocess
import tempfile
import shutil
import datetime
import logging

import flask
from google.cloud import bigquery
from google import genai
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sql-review-bot")

app = flask.Flask(__name__)

PROJECT_ID = os.environ["PROJECT_ID"]
LOCATION = os.environ.get("LOCATION", "us-central1")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")  # PAT with repo scope, set as Cloud Run secret
SQL_DIR = "resources/sql"
BEAM_DIR = "src/beam"

genai_client = genai.Client(vertexai=True, project=PROJECT_ID, location=LOCATION)
bq_client = bigquery.Client(project=PROJECT_ID)


def run_git(*args, cwd):
    result = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed: {result.stderr}")
    return result.stdout


def clone_and_diff(repo_clone_url: str, before_sha: str, after_sha: str):
    """Clone the repo, return list of changed .sql files with old/new content."""
    tmp_dir = tempfile.mkdtemp()
    run_git("clone", repo_clone_url, tmp_dir, cwd="/tmp")

    changed_files = run_git(
        "diff", "--name-only", before_sha, after_sha, cwd=tmp_dir
    ).splitlines()

    changed_sql = [f for f in changed_files if f.startswith(SQL_DIR) and f.endswith(".sql")]

    results = []
    for path in changed_sql:
        old_content = run_git("show", f"{before_sha}:{path}", cwd=tmp_dir) \
            if _file_exists_at(tmp_dir, before_sha, path) else None
        new_content = run_git("show", f"{after_sha}:{path}", cwd=tmp_dir)
        results.append({"path": path, "old": old_content, "new": new_content})

    beam_context = ""
    beam_full_path = os.path.join(tmp_dir, BEAM_DIR)
    if os.path.isdir(beam_full_path):
        for fname in os.listdir(beam_full_path):
            if fname.endswith(".py"):
                with open(os.path.join(beam_full_path, fname)) as f:
                    beam_context += f"\n--- {fname} ---\n{f.read()}"

    shutil.rmtree(tmp_dir, ignore_errors=True)
    return results, beam_context


def _file_exists_at(repo_dir, sha, path):
    result = subprocess.run(
        ["git", "cat-file", "-e", f"{sha}:{path}"], cwd=repo_dir, capture_output=True
    )
    return result.returncode == 0


def dry_run_bytes(sql_text: str) -> int:
    if not sql_text or not sql_text.strip():
        return 0
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    job = bq_client.query(sql_text, job_config=job_config)
    return job.total_bytes_processed


def build_schema_manifest() -> str:
    # Hackathon-scoped: hardcoded. Real version pulls INFORMATION_SCHEMA.COLUMNS.
    # Expanded with real detail (not padding) so it clears the Vertex AI
    # context-caching minimum token floor (1024 tokens for this model).
    return """
    ==================================================
    BANK-WIDE / DEMO DATA ARCHITECTURE BLUEPRINT
    ==================================================

    [TABLE: grid_data.grid_readings]
    Columns:
      - station_id STRING: unique identifier for the reporting station,
        joins to station_metadata.station_id.
      - timestamp TIMESTAMP: UTC timestamp of the reading, one row per
        station per minute in production volumes.
      - frequency_hz FLOAT: measured grid frequency in Hz. Nominal value
        is 50.0Hz; readings below 49.9Hz indicate under-frequency load
        shedding conditions and are the primary signal consumed by
        downstream pipelines.
      - voltage FLOAT: measured line voltage.
      - region STRING: geographic region the station belongs to
        (e.g. 'west', 'east').
    Partitioning: NOT partitioned, NOT clustered in this demo dataset.
    In a production deployment this table would typically be partitioned
    by DATE(timestamp) and clustered by region, since most consumer
    queries filter on both.

    [TABLE: grid_data.station_metadata]
    Columns:
      - station_id STRING: unique identifier, primary join key.
      - station_name STRING: human-readable station name.
      - region STRING: geographic region.
      - capacity_mw FLOAT: rated capacity in megawatts.
    Partitioning: NOT partitioned, NOT clustered. Small dimension table,
    full scans are cheap regardless of query shape.

    [GOLDEN QUERY EXAMPLE 1 — column pruning]
    Bad:
      SELECT * FROM grid_readings r JOIN station_metadata m
      ON r.station_id = m.station_id WHERE r.region = 'west'
    Good (when only station_id, frequency_hz, region are consumed
    downstream):
      SELECT r.station_id, r.frequency_hz, r.region
      FROM grid_readings r JOIN station_metadata m
      ON r.station_id = m.station_id WHERE r.region = 'west'
    Rationale: BigQuery is columnar — bytes scanned is driven by which
    columns are referenced, not by row filtering or join order. Dropping
    unused columns from the SELECT list is the only lever available on
    an unpartitioned table like this one.

    [GOLDEN QUERY EXAMPLE 2 — predicate placement]
    On a partitioned table (not applicable to this demo dataset, but
    relevant if this schema is extended later): filtering on the
    partition column before a join enables partition pruning and can
    reduce bytes scanned dramatically. On unpartitioned tables, moving
    a WHERE clause earlier has no cost effect in BigQuery's columnar
    model — only column selection does.

    [REVIEW POLICY]
    Any suggested rewrite must preserve exact join semantics and exact
    filter conditions. Only column pruning is considered a safe,
    automatic-suggestion-eligible optimization. Anything else (join
    reordering, aggregation changes, subquery restructuring) should be
    flagged for human review rather than auto-suggested.
    """


CACHE_DISPLAY_NAME = "grid_schema_beam_cache"


def find_existing_cache():
    """Look for a live, non-expired cache with our display name."""
    try:
        for cache in genai_client.caches.list():
            if cache.display_name == CACHE_DISPLAY_NAME:
                logger.info(f"Found existing cache: {cache.name} (expires {cache.expire_time})")
                return cache.name
    except Exception as e:
        logger.warning(f"Cache lookup failed ({e}), will attempt to create a new one")
    return None


def get_or_create_cache(schema_manifest: str, beam_context: str):
    existing = find_existing_cache()
    if existing:
        logger.info(f"Reusing existing cache: {existing}")
        return existing

    combined = schema_manifest + "\n\n[DOWNSTREAM CONSUMER CODE]\n" + beam_context
    approx_tokens = len(combined) // 4
    logger.info(f"No existing cache found. Creating new one, manifest ~{approx_tokens} tokens (~{len(combined)} chars)")
    try:
        cache = genai_client.caches.create(
            model="gemini-2.5-flash",
            config={
                "contents": [combined],
                "ttl": "86400s",
                "display_name": CACHE_DISPLAY_NAME,
            },
        )
        logger.info(f"Context cache created successfully: {cache.name}")
        return cache.name
    except Exception as e:
        logger.warning(f"Cache creation failed ({e}), falling back to inline context")
        return None


def ask_gemini_for_rewrite(old_sql: str, new_sql: str, cache_name, schema_manifest, beam_context) -> str:
    prompt = f"""You are reviewing a BigQuery SQL change in a PR.

PREVIOUS QUERY:
{old_sql or "(new file, no previous version)"}

NEW QUERY (as submitted in this PR):
{new_sql}

Task: Suggest a rewrite of the NEW QUERY that reduces the amount of data
scanned, using the schema and downstream consumer code as ground truth
for which columns are actually required. Do not drop any column that
the downstream code in [DOWNSTREAM CONSUMER CODE] actually reads —
preserving business logic is a hard constraint. Return ONLY the
rewritten SQL, no explanation, no markdown fences.
"""

    if cache_name:
        logger.info(f"Calling Gemini WITH cached context: {cache_name}")
        response = genai_client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config={"cached_content": cache_name},
        )
    else:
        logger.info("Calling Gemini WITHOUT cache (inline context fallback)")
        full_prompt = f"[SCHEMA]\n{schema_manifest}\n\n[DOWNSTREAM CODE]\n{beam_context}\n\n{prompt}"
        response = genai_client.models.generate_content(
            model="gemini-2.5-flash",
            contents=full_prompt,
        )
    return response.text.strip()


def post_github_comment(repo_owner: str, repo_name: str, commit_sha: str, body: str):
    if not GITHUB_TOKEN:
        print("No GITHUB_TOKEN set, skipping comment post. Body was:\n", body)
        return
    url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/commits/{commit_sha}/comments"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github+json"}
    resp = requests.post(url, json={"body": body}, headers=headers)
    resp.raise_for_status()


@app.route("/review", methods=["POST"])
def review():
    payload = flask.request.get_json()
    repo_clone_url = payload["repo_clone_url"]
    repo_owner = payload["repo_owner"]
    repo_name = payload["repo_name"]
    before_sha = payload["before_sha"]
    after_sha = payload["after_sha"]

    changed, beam_context = clone_and_diff(repo_clone_url, before_sha, after_sha)
    if not changed:
        return flask.jsonify({"status": "no_sql_changes"})

    schema_manifest = build_schema_manifest()
    cache_name = get_or_create_cache(schema_manifest, beam_context)

    comment_sections = []
    for change in changed:
        old_bytes = dry_run_bytes(change["old"]) if change["old"] else None
        new_bytes = dry_run_bytes(change["new"])

        rewrite = ask_gemini_for_rewrite(
            change["old"], change["new"], cache_name, schema_manifest, beam_context
        )
        rewrite_bytes = dry_run_bytes(rewrite)

        section = f"### `{change['path']}`\n"
        if old_bytes is not None:
            section += f"- Previous version: {old_bytes:,} bytes scanned\n"
        section += f"- This PR's version: {new_bytes:,} bytes scanned\n"
        section += f"- Gemini-suggested rewrite: {rewrite_bytes:,} bytes scanned\n\n"
        section += f"<details><summary>Suggested rewrite</summary>\n\n```sql\n{rewrite}\n```\n</details>"
        comment_sections.append(section)

    comment_body = "## SQL Cost Review\n\n" + "\n\n---\n\n".join(comment_sections)
    post_github_comment(repo_owner, repo_name, after_sha, comment_body)

    return flask.jsonify({"status": "ok", "changed_files": [c["path"] for c in changed]})


@app.route("/", methods=["GET"])
def health():
    return "ok"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))