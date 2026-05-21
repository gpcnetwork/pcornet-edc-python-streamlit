# PCORnet CDM v7.0 — Data Quality Dashboard

A **Streamlit-in-Snowflake** application that runs EDC data quality checks against PCORnet Common Data Model (CDM) v7.0. Covers all five EDC report sections — from descriptive summaries to data persistence.

## Prerequisites

- Python ≥ 3.11
- [uv](https://docs.astral.sh/uv/) (package manager)
- [Snowflake CLI (`snow`)](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index) — for deployment

---

## Local Setup

### 1. Configure Snowflake credentials

Copy the secrets template and fill in your Snowflake connection details:

```bash
cp .streamlit/secrets-copy.toml .streamlit/secrets.toml
```

Then edit `.streamlit/secrets.toml`:

```toml
[connections.snowflake]
account = "your-account-identifier"   # e.g. xy12345.us-east-1
user = "your.email@example.com"
authenticator = "externalbrowser"     # SSO — no password needed
role = "YOUR_ROLE"
warehouse = "YOUR_WAREHOUSE"
database = "YOUR_DATABASE"
schema = "YOUR_SCHEMA"
```

> `secrets.toml` is gitignored and must never be committed.

### 2. Install dependencies

```bash
uv venv
source .venv/bin/activate
uv sync
```

Run the app locally (uses `DQ_APP_MODE=local`, bypasses Snowpark session):

```bash
make local
```

---

## Snowflake Deployment

Deploy the app to Snowflake (registers the Streamlit app and uploads artifacts):

```bash
make deploy
```

Push an update to an already-deployed app:

```bash
make update
```

Print the app URL:

```bash
make url
```
