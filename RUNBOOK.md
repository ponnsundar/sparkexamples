# Deployment Runbook — Spark Databricks Project

## What Gets Deployed

| Component | Description |
|---|---|
| `notebooks/example_pipeline.py` | Main ETL pipeline |
| `exercises/01–43_*.py` | 43 Spark workshop exercises (SQL, Streaming, MLlib) |
| `src/` | Shared transformations and utilities |
| 2 Databricks Jobs | `example_pipeline` and `spark_exercises` |

Everything lands under `/Workspace/Users/<you>/spark_databricks_project/` in your Databricks workspace.

---

## Prerequisites

### 1. Databricks Workspace Access

You need:
- A Databricks workspace URL (e.g. `https://my-workspace.cloud.databricks.com`)
- A personal access token (PAT)

To generate a PAT:
1. Log into your Databricks workspace
2. Click your profile icon (top-right) → **Settings**
3. Go to **Developer** → **Access tokens**
4. Click **Generate new token**, give it a name, click **Generate**
5. Copy the token — you won't see it again

### 2. Install Databricks CLI (v0.200+)

```bash
# macOS
brew tap databricks/tap
brew install databricks

# Or via curl (macOS / Linux)
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
```

Verify:
```bash
databricks --version
# Should show 0.200.0 or higher
```

### 3. Configure Authentication

**Option A — Environment variables (recommended for CI):**
```bash
export DATABRICKS_HOST=https://my-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=dapi_your_token_here
```

**Option B — CLI profile (recommended for local dev):**
```bash
databricks configure --token
# Enter your workspace URL and token when prompted
```

This creates `~/.databrickscfg` with your credentials.

---

## Deployment Steps

### Step 1: Validate the Bundle

Checks that `databricks.yml` is valid and your auth works:

```bash
databricks bundle validate
```

Expected output:
```
Name: spark_databricks_project
Target: default
Workspace:
  Host: https://my-workspace.cloud.databricks.com
  ...
Validation OK!
```

**If this fails:**
- `401 Unauthorized` → your token is wrong or expired, regenerate it
- `Cannot resolve host` → check your `DATABRICKS_HOST` URL
- YAML parse errors → run `cat -A databricks.yml` to check for tab characters (use spaces only)

### Step 2: Deploy

```bash
databricks bundle deploy
```

This will:
1. Sync `src/`, `notebooks/`, and `exercises/` to your workspace
2. Create (or update) two Databricks jobs: `Example Spark Pipeline` and `Spark Workshop Exercises`

Expected output:
```
Uploading bundle files to /Workspace/Users/<you>/spark_databricks_project/...
Deploying resources...
Updating deployment state...
Deployment complete!
```

**Or use the script:**
```bash
./scripts/deploy.sh
```

### Step 3: Verify in Databricks UI

1. Open your Databricks workspace in a browser
2. Go to **Workspace** → **Users** → **\<your username\>** → **spark_databricks_project**
   - You should see `exercises/`, `notebooks/`, and `src/` folders
3. Go to **Workflows** → **Jobs**
   - You should see `Example Spark Pipeline` and `Spark Workshop Exercises`

---

## Running Jobs

### Run all 43 exercises:
```bash
databricks bundle run spark_exercises
```

### Run the main pipeline:
```bash
databricks bundle run example_pipeline
```

### Run a single exercise by number:
```bash
./scripts/run_exercise.sh 01   # Split with variable delimiter
./scripts/run_exercise.sh 29   # Exam assessment report
./scripts/run_exercise.sh 43   # Email classification (MLlib)
```

### Check job run status:
```bash
databricks jobs list --output json
databricks runs list --job-id <JOB_ID> --limit 5
```

Or just check the **Workflows → Job Runs** page in the Databricks UI.

---

## Alternative: Upload Notebooks Only (No Jobs)

If you just want the notebooks in your workspace without creating jobs:

```bash
pip install databricks-sdk
export DATABRICKS_HOST=https://my-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=dapi_your_token_here

python scripts/upload_notebooks.py
```

Notebooks will appear under `/Workspace/Users/<you>/spark_databricks_project/exercises/`.
You can then open and run them interactively from the Databricks UI.

---

## CI/CD: Automated Deployment via GitHub Actions

The repo includes `.github/workflows/deploy.yml` that deploys on every push to `main`.

### Setup (one-time):

1. Go to your GitHub repo → **Settings** → **Secrets and variables** → **Actions**
2. Add two repository secrets:

| Secret Name | Value |
|---|---|
| `DATABRICKS_HOST` | `https://my-workspace.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | `dapi_your_token_here` |

3. Push to `main` — the workflow validates and deploys automatically.

You can also trigger it manually from **Actions** → **Deploy to Databricks** → **Run workflow**.

---

## Updating the Deployment

After making changes to any exercise, notebook, or source file:

```bash
databricks bundle deploy
```

DABs handles incremental sync — only changed files are uploaded.

---

## Tearing Down

To remove everything deployed by the bundle:

```bash
databricks bundle destroy
```

This deletes the synced files and the two jobs from your workspace. Confirm with `y` when prompted.

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `databricks: command not found` | Install the CLI (see Prerequisites step 2) |
| `Error: authentication required` | Set `DATABRICKS_HOST` + `DATABRICKS_TOKEN` or run `databricks configure --token` |
| `401 Unauthorized` | Token expired — generate a new PAT |
| `Error: cluster node type not available` | Change `cluster_node_type` in `databricks.yml` to a type available in your workspace (e.g. `Standard_DS3_v2` for Azure, `m5.large` for AWS) |
| `Error: bundle validation failed` | Check YAML syntax — no tabs, proper indentation |
| Job runs fail with `ModuleNotFoundError` | The exercises are self-contained; if running `example_pipeline`, ensure `src/` was synced (check workspace file browser) |
| Streaming exercises time out | Expected — exercises 41 and 42 use `time.sleep()` and stop themselves; increase job timeout if needed |
