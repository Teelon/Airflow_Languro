# Languro Airflow Pipelines

This project manages the data orchestration for Languro, specifically the generation of high-quality verb conjugation audio using Gemini 2.5 TTS and storage in Cloudflare R2.

## 🚀 Quick Start

### 1. Prerequisites
- **Docker Desktop**: Must be running.
- **Astro CLI**: The easiest way to run Airflow locally. [Install it here](https://www.astronomer.io/docs/astro/cli/install-cli).

### 2. Configuration
Copy the example environment file and fill in your credentials:
```powershell
cp .env.example .env
```
Open `.env` and provide:
- **R2 Credentials**: `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_ACCOUNT_ID`, and `R2_BUCKET_NAME`.
- **Database**: `AIRFLOW_CONN_LANGURO_DB` (Postgres URL for the main Languro app).
- **Gemini SPI**: `GEMINI_API_KEY`.

### 3. Build and Run
Start the Airflow environment:
```powershell
astro dev start
```
This will:
1. Build a custom Docker image with all dependencies (`google-genai`, `boto3`, etc.).
2. Initialize a local Postgres metadata database.
3. Start the Webserver, Scheduler, and Triggerer.

### 4. Access the UI
Once the command completes, go to:
- **URL**: [http://localhost:8080](http://localhost:8080)
- **Login**: `admin` / `admin`

---

## 🏗️ Pipelines (DAGs)

### `verb_audio_generation`
This is the core pipeline for Languro's content:
1. **Fetch**: Queries the Languro database for conjugations missing audio.
2. **Generate**: Calls Gemini 2.5 (`gemini-2.5-pro-preview-tts`) with language-specific pronunciation rules.
3. **Format**: Generates high-efficiency `.opus` audio files.
4. **Store**: Uploads files to R2 using the convention: `conjugation/{iso_code}/{verb}/{tense}/{pronoun}_{form}.opus`.
5. **Update**: Writes the R2 key back to the database and marks `has_audio = true`.

## 🛠️ Maintenance

**Adding new dependencies**: Add them to `requirements.txt` and run `astro dev restart`.

**Stop the environment**:
```powershell
astro dev stop
```

**View logs**:
```powershell
astro dev logs
```
