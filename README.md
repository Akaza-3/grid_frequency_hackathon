# SQL Cost Review Bot — hackathon test repo

## Layout
```
resources/sql/        SQL queries (what a "PR" would change)
src/beam/              Beam pipeline — ground truth for which columns
                        are actually consumed downstream
cloud_run/              The review service: diffs commits, dry-runs SQL,
                        calls Gemini, posts a PR comment
.github/workflows/      Fires the Cloud Run service on push to main
```

## One-time GCP setup

```bash
export PROJECT_ID=project-ff7c2ef5-8d88-401a-b86

gcloud config set project $PROJECT_ID
gcloud services enable run.googleapis.com bigquery.googleapis.com \
  aiplatform.googleapis.com dataflow.googleapis.com \
  artifactregistry.googleapis.com

# Bucket for Beam temp files (name must be globally unique)
gsutil mb -l us-central1 gs://${PROJECT_ID}-beam-temp
```

## Deploy the Cloud Run service

From the `cloud_run/` directory:

```bash
cd cloud_run

gcloud run deploy sql-review-bot \
  --source . \
  --project $PROJECT_ID \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars PROJECT_ID=$PROJECT_ID,LOCATION=us-central1 \
  --set-secrets GITHUB_TOKEN=github-token:latest \
  --memory 512Mi \
  --timeout 300
```

`--allow-unauthenticated` is a shortcut for the hackathon — anyone with
the URL can trigger a review. Fine for a demo; tighten before this
touches anything real (require an OIDC token from the GitHub Action
instead).

The `GITHUB_TOKEN` secret needs a GitHub Personal Access Token with
`repo` scope, so the bot can post commit comments:

```bash
echo -n "ghp_yourtokenhere" | gcloud secrets create github-token --data-file=-
gcloud secrets add-iam-policy-binding github-token \
  --member="serviceAccount:$(gcloud run services describe sql-review-bot \
    --region us-central1 --format='value(spec.template.spec.serviceAccountName)')" \
  --role="roles/secretmanager.secretAccessor"
```

Grab the deployed URL:
```bash
gcloud run services describe sql-review-bot --region us-central1 \
  --format='value(status.url)'
```

## Wire up the GitHub side

1. Push this repo to GitHub.
2. Repo Settings → Secrets and variables → Actions → New repository secret:
   - Name: `CLOUD_RUN_URL`
   - Value: the URL from the previous step (no trailing slash)
3. Push a change to `resources/sql/grid_readings_query.sql` on `main`
   and check the Actions tab, then the commit's comments on GitHub.

## Testing locally before pushing

Test the Beam pipeline in isolation:
```bash
pip install 'apache-beam[gcp]'
python3 src/beam/frequency_pipeline.py
```

Test the Cloud Run service locally before deploying:
```bash
cd cloud_run
pip install -r requirements.txt
export PROJECT_ID=project-ff7c2ef5-8d88-401a-b86
export GOOGLE_APPLICATION_CREDENTIALS=~/.config/gcloud/application_default_credentials.json
python3 main.py
# in another terminal:
curl -X POST localhost:8080/review -H "Content-Type: application/json" -d '{
  "repo_clone_url": "https://github.com/YOUR_USER/YOUR_REPO.git",
  "repo_owner": "YOUR_USER",
  "repo_name": "YOUR_REPO",
  "before_sha": "<older commit sha>",
  "after_sha": "<newer commit sha>"
}'
```
