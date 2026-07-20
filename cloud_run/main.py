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
import json
import subprocess
import tempfile
import shutil
import datetime
import logging
import re
import hashlib

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
if GITHUB_TOKEN:
    logger.info(f"GITHUB_TOKEN loaded (length {len(GITHUB_TOKEN)}, starts with {GITHUB_TOKEN[:4]}...)")
else:
    logger.warning("GITHUB_TOKEN is NOT set in environment")

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
    if not sql_text:
        return 0

    sql_text = extract_sql(sql_text)

    if not sql_text.strip():
        return 0

    try:
        job_config = bigquery.QueryJobConfig(
            dry_run=True,
            use_query_cache=False
        )

        job = bq_client.query(sql_text, job_config=job_config)

        return job.total_bytes_processed

    except Exception as e:
        logger.exception(f"Dry run failed:\n{sql_text}")
        logger.exception(e)

        return 0


def _extract_tables(sql_text: str):
    """
    Extract fully-qualified table names from SQL enclosed in backticks.
    Example:
    `project.dataset.table`
    """
    return list(set(re.findall(r'`([^`]+)`', sql_text)))


def build_schema_manifest(sql_text: str) -> str:
    tables = _extract_tables(sql_text)
    logger.info(f"Extracted tables: {tables}")

    manifest = []

    for table in tables:
        try:
            parts = table.split(".")

            if len(parts) == 3:
                project, dataset, table_name = parts
            elif len(parts) == 2:
                project = PROJECT_ID
                dataset, table_name = parts
            else:
                logger.warning(f"Skipping unsupported table reference: {table}")
                continue

            logger.info(f"Fetching INFORMATION_SCHEMA for {project}.{dataset}.{table_name}")

            column_query = f"""
            SELECT
                column_name,
                data_type,
                is_partitioning_column,
                clustering_ordinal_position
            FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name='{table_name}'
            ORDER BY ordinal_position
            """

            columns = list(bq_client.query(column_query).result())

            table_query = f"""
            SELECT
                table_name,
                row_count,
                size_bytes
            FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLE_STORAGE`
            WHERE table_name='{table_name}'
            """

            try:
                table_info = list(bq_client.query(table_query).result())
            except Exception as e:
                logger.warning(f"Could not fetch TABLE_STORAGE for {table}: {e}")
                table_info = []

            manifest.append(f"\n===== TABLE : {table} =====")

            if table_info:
                t = table_info[0]
                manifest.append(f"Rows : {t.row_count}")
                manifest.append(f"Storage : {t.size_bytes} bytes")

            partition_cols = []
            clustering_cols = []

            for c in columns:
                manifest.append(f"- {c.column_name} ({c.data_type})")

                if c.is_partitioning_column == "YES":
                    partition_cols.append(c.column_name)

                if c.clustering_ordinal_position:
                    clustering_cols.append(c.column_name)

            manifest.append(
                f"Partition Columns : {partition_cols if partition_cols else 'None'}"
            )

            manifest.append(
                f"Cluster Columns : {clustering_cols if clustering_cols else 'None'}"
            )

        except Exception:
            logger.exception(f"Failed to discover schema for {table}")

    return "\n".join(manifest)


def find_existing_cache(display_name):
    """Look for a live, non-expired cache with our display name."""
    try:
        for cache in genai_client.caches.list():
            if cache.display_name == display_name:
                logger.info(f"Found existing cache: {cache.name} (expires {cache.expire_time})")
                return cache.name
    except Exception as e:
        logger.warning(f"Cache lookup failed ({e}), will attempt to create a new one")
    return None


def get_or_create_cache(schema_manifest: str, beam_context: str):
    PROMPT_VERSION = "v1"
    combined = (
        PROMPT_VERSION
        + schema_manifest
        + "\n\n[DOWNSTREAM]\n"
        + beam_context
    )

    cache_hash = hashlib.sha256(
        combined.encode("utf-8")
    ).hexdigest()[:12]

    display_name = f"grid-schema-{cache_hash}"
    existing = find_existing_cache(display_name)
    if existing:
        logger.info(f"Reusing existing cache: {existing}")
        return existing

    
    approx_tokens = len(combined) // 4
    logger.info(f"No existing cache found. Creating new one, manifest ~{approx_tokens} tokens (~{len(combined)} chars)")
    try:
        cache = genai_client.caches.create(
            model="gemini-2.5-flash",
            config={
                "contents": [combined],
                "ttl": "86400s",
                "display_name": display_name,
            },
        )
        logger.info(f"Context cache created successfully: {cache.name}")
        return cache.name
    except Exception as e:
        logger.warning(f"Cache creation failed ({e}), falling back to inline context")
        return None


def ask_gemini_for_rewrite(old_sql: str, new_sql: str, cache_name, schema_manifest, beam_context, original_bytes: int) -> str:
    prompt = f"""
You are a Senior Google BigQuery Performance Engineer responsible for reviewing production SQL used in enterprise data pipelines.

Your objective is to optimize the supplied SQL while preserving business logic exactly.

You are provided with:

1. BigQuery table schemas
2. Downstream Apache Beam pipeline code
3. Original SQL query
4. Original query bytes scanned (BigQuery Dry Run)

========================================================
BIGQUERY SCHEMA
========================================================

{schema_manifest}

========================================================
DOWNSTREAM BEAM PIPELINE
========================================================

{beam_context}

========================================================
ORIGINAL SQL
========================================================

{new_sql}

========================================================
ORIGINAL BYTES SCANNED
========================================================

{original_bytes}

========================================================
PRIMARY OBJECTIVE
========================================================

Reduce BigQuery cost while producing identical business results.

Preserve:

- business logic
- joins
- filtering behaviour
- aggregations
- ordering
- window functions
- duplicate handling
- NULL semantics
- timestamp semantics
- numeric precision

Never make an optimization that changes the output consumed by downstream Beam code.

========================================================
HARD CONSTRAINT — JOIN PRESERVATION
========================================================

The optimized SQL MUST contain the exact same JOIN clauses as the
original query — same tables joined, same join type (INNER/LEFT/etc),
same ON condition, same number of joins.

Do NOT:
- remove a join, even if you believe the joined columns are unused
- replace a join with a subquery, EXISTS clause, or IN clause
- replace a join with a window function or aggregation that
  approximates the same result
- merge two joins into one, even if you believe it's equivalent

If you believe a join is unnecessary, do NOT act on that belief.
Instead, note it in "recommendations" as a suggestion for human
review. This is a hard rule with no exceptions, including cases
where removing the join would reduce bytes scanned.

========================================================
OPTIMIZATION CHECKLIST
========================================================

Review the SQL carefully.

Column Usage
------------
Determine:

- projected columns never consumed
- unused aliases
- unused computed columns
- unused CASE expressions
- unused window outputs

Only remove columns if they are unused by BOTH SQL and downstream Beam code.

--------------------------------------------------------

Predicate Optimization
----------------------

Look for:

- predicate pushdown
- partition pruning
- redundant predicates
- constant expressions
- unnecessary filters

--------------------------------------------------------

Join Optimization
-----------------

never remove JOIN.

Determine:

- unnecessary joins
- duplicate joins
- inefficient join ordering
- opportunities for broadcast joins
- clustering opportunities

Never remove joins if business logic changes.

--------------------------------------------------------

Projection Optimization
-----------------------

Prefer explicit projection instead of SELECT *.

Remove:

- unused expressions
- unused aliases
- unused calculated columns

--------------------------------------------------------

Window Functions
----------------

Review every window function.

Determine:

- whether output is consumed
- whether computation is necessary

Never alter partitioning or ordering semantics.

--------------------------------------------------------

CTE Review
----------

Identify:

- dead CTEs
- pass-through CTEs
- mergeable CTEs
- redundant nesting

--------------------------------------------------------

Aggregation Review
------------------

Review:

- DISTINCT
- GROUP BY
- HAVING
- QUALIFY

Only simplify when semantics remain identical.

--------------------------------------------------------

Partition Recommendations
-------------------------

Recommend whether the table should be partitioned.

Explain why.

--------------------------------------------------------

Clustering Recommendations
--------------------------

Recommend clustering columns.

Explain expected benefit.

--------------------------------------------------------

Performance Review
------------------

Identify opportunities to reduce:

- bytes scanned
- shuffle
- execution stages
- intermediate data
- slot usage

--------------------------------------------------------

Business Logic Validation
-------------------------

Verify:

- joins preserved
- filters preserved
- aggregations preserved
- ordering preserved
- timestamps preserved
- NULL handling preserved
- duplicate handling preserved
- numeric precision preserved

--------------------------------------------------------

Downstream Compatibility
------------------------

Verify:

- required columns remain
- names unchanged
- types compatible
- Beam pipeline will continue to work

========================================================
SELF REVIEW
========================================================

Before generating your answer verify:

✓ Business logic preserved

✓ No required columns removed

✓ Join cardinality unchanged

✓ Window functions preserved

✓ Aggregations preserved

✓ Downstream Beam compatibility maintained

If any answer is NO, reject the optimization.

========================================================
COST ANALYSIS
========================================================

The original bytes scanned are already provided.

DO NOT invent values for optimized bytes.

Estimate ONLY the percentage cost reduction that is likely based on your optimization strategy.

The application will later calculate the actual optimized bytes using a BigQuery Dry Run.

========================================================
OUTPUT FORMAT
========================================================

Return ONLY valid JSON.

{{
  "business_logic": {{
    "status": "PASS or FAIL",
    "reason": "Explain your reasoning."
  }},

  "optimized_sql": "Complete optimized SQL",

  "summary": "Short summary of the optimization.",

  "changes": [
    {{
      "change": "...",
      "reason": "..."
    }}
  ],

  "recommendations": [
    "Recommendation 1",
    "Recommendation 2"
  ],

  "estimated_cost_analysis": {{
    "original_bytes": "{original_bytes}",
    "expected_cost_reduction": "Estimated percentage reduction only",
    "reason": "Explain why the optimization is expected to reduce cost."
  }}
}}

Return ONLY JSON.

Do not include markdown.

Do not wrap the SQL in triple backticks.

Do not include any explanation outside the JSON object.
"""

    if cache_name:
        logger.info(f"Calling Gemini WITH cached context: {cache_name}")
        response = genai_client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config={
                "cached_content": cache_name,
                "temperature": 0,
            },
        )
    else:
        logger.info("Calling Gemini WITHOUT cache (inline context fallback)")
        full_prompt = (
            f"[SCHEMA]\n{schema_manifest}\n\n"
            f"[DOWNSTREAM CODE]\n{beam_context}\n\n"
            f"{prompt}"
        )
        response = genai_client.models.generate_content(
            model="gemini-2.5-flash",
            contents=full_prompt,
            config={
                "temperature": 0,
            },
        )

    return response.text.strip()

def extract_sql(response_text: str) -> str:
    """
    Extract SQL from a Gemini markdown response.
    """

    if not response_text:
        return ""

    match = re.search(r"```sql\s*(.*?)```", response_text, re.DOTALL | re.IGNORECASE)

    if match:
        return match.group(1).strip()

    match = re.search(r"```\s*(.*?)```", response_text, re.DOTALL)

    if match:
        return match.group(1).strip()

    return response_text.strip()

def post_github_comment(repo_owner: str, repo_name: str, commit_sha: str, body: str):
    if not GITHUB_TOKEN:
        logger.warning("No GITHUB_TOKEN set, skipping comment post.")
        logger.info(f"Comment body was:\n{body}")
        return
    url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/commits/{commit_sha}/comments"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github+json"}
    logger.info(f"Posting comment to {url}")
    try:
        resp = requests.post(url, json={"body": body}, headers=headers)
        resp.raise_for_status()
        logger.info(f"Comment posted successfully, id={resp.json().get('id')}")
    except requests.exceptions.HTTPError as e:
        logger.error(f"GitHub comment post failed: {e} — response body: {resp.text}")
        raise


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

    comment_sections = []
    for change in changed:
        old_bytes = dry_run_bytes(change["old"]) if change["old"] else None
        new_bytes = dry_run_bytes(change["new"])

        schema_manifest = build_schema_manifest(change["new"])
        cache_name = get_or_create_cache(schema_manifest, beam_context)

        rewrite = ask_gemini_for_rewrite(
            change["old"],
            change["new"],
            cache_name,
            schema_manifest,
            beam_context,
            new_bytes,
        )

        # -----------------------------
        # Parse Gemini JSON
        # -----------------------------
        try:
            rewrite = rewrite.strip()
            if rewrite.startswith("```json"):
                rewrite = rewrite[7:]

            if rewrite.startswith("```"):
                rewrite = rewrite[3:]

            if rewrite.endswith("```"):
                rewrite = rewrite[:-3]

            rewrite = rewrite.strip()
            rewrite_json = json.loads(rewrite)
        except json.JSONDecodeError as e:
            logger.error(f"Gemini returned invalid JSON:\n{rewrite}")
            raise e

        optimized_sql = rewrite_json["optimized_sql"]
        summary = rewrite_json.get("summary", "")
        changes = rewrite_json.get("changes", [])
        recommendations = rewrite_json.get("recommendations", [])
        business_logic = rewrite_json.get("business_logic", {})

        # -----------------------------
        # Dry run ONLY the optimized SQL
        # -----------------------------
        rewrite_bytes = dry_run_bytes(optimized_sql)

        # -----------------------------
        # Build GitHub comment
        # -----------------------------
        section = f"## `{change['path']}`\n\n"

        if old_bytes is not None:
            section += f"**Previous:** {old_bytes:,} bytes scanned\n\n"

        section += f"**Current:** {new_bytes:,} bytes scanned\n\n"
        section += f"**Gemini Rewrite:** {rewrite_bytes:,} bytes scanned\n\n"

        section += "### Business Logic\n"

        section += (
            f"- Status: **{business_logic.get('status', 'UNKNOWN')}**\n"
            f"- Reason: {business_logic.get('reason', '')}\n\n"
        )

        section += "### Summary\n"
        section += summary + "\n\n"

        if changes:
            section += "### Changes Applied\n"

            for item in changes:
                section += (
                    f"- **{item['change']}**\n"
                    f"  - {item['reason']}\n"
                )

            section += "\n"

        if recommendations:
            section += "### Recommendations\n"

            for rec in recommendations:
                section += f"- {rec}\n"

            section += "\n"

        section += (
            "<details>\n"
            "<summary><b>Optimized SQL</b></summary>\n\n"
            "```sql\n"
            f"{optimized_sql}\n"
            "```\n"
            "</details>\n"
        )

        comment_sections.append(section)

    comment_body = "## SQL Cost Review\n\n" + "\n\n---\n\n".join(comment_sections)
    post_github_comment(repo_owner, repo_name, after_sha, comment_body)

    return flask.jsonify({"status": "ok", "changed_files": [c["path"] for c in changed]})


@app.route("/", methods=["GET"])
def health():
    return "ok"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))