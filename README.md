# Languro Airflow & Data Pipelines

This project manages the data orchestration for Languro, handling high-volume audio generation and processing using Airflow.

## 🚀 Quick Start

### 1. Prerequisites
- **Docker Desktop**: Must be running.
- **Astro CLI**: The easiest way to run Airflow locally. [Install it here](https://www.astronomer.io/docs/astro/cli/install-cli).

### 2. Configuration
Copy the example environment file and fill in your credentials:
```powershell
cp .env.example .env
```

Open `.env` and configure the following:

#### Storage & Database
- **R2 Credentials**: `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_ACCOUNT_ID`, `R2_BUCKET_NAME`.
- **Database**: `AIRFLOW_CONN_LANGURO_DB` (Postgres URL for the main Languro app).

#### AI Services
- **Gemini API**: `GEMINI_API_KEY` (for Verb Audio generation).
- **Gemini Model**: `GEMINI_MODEL_TTS_MODEL` (defaults to `gemini-2.5-pro-preview-tts`).
- **Google Cloud TTS**: `GOOGLE_APP_CREDS_JSON`.
  > **⚠️ IMPORTANT**: This variable must contain the **minified** JSON content of your Google Cloud Service Account key. Do not use newlines.

### 3. Build and Run
Start the Airflow environment:
```powershell
astro dev start
```
This will:
1. Build a custom Docker image with dependencies (`google-genai`, `boto3`, `google-cloud-texttospeech`, etc.).
2. Initialize a local Postgres metadata database.
3. Start the Webserver, Scheduler, and Triggerer.

### 4. Access the UI
- **URL**: [http://localhost:8080](http://localhost:8080)
- **Login**: `admin` / `admin`

---

## 🏗️ Pipelines (DAGs)

### 1. `verb_audio_generation_batch`
Generates audio for verb conjugations using the **Gemini Batch API** for high throughput and lower cost.
- **Source**: Queries `conjugations` table for items where `has_audio = false`.
- **Generation**: Uses `gemini-2.5-pro-preview-tts` with a minimal text prompt to reduce token usage.
- **Output**: Generates `.opus` files (64k bitrate).
- **Storage**: Uploads to R2 at `conjugation/{iso_code}/{verb}/{tense}/{pronoun}_{form}.opus`.
- **Schedule**: Hourly at minute 0 (`0 * * * *`).

### 2. `reading_audio_generation_batch`
Generates audio and word-level timestamps for reading lessons using **Google Cloud TTS**.
- **Source**: Queries `reading_lessons` table for items where `audioKey` is NULL.
- **Generation**: Uses Google Cloud TTS `v1beta1` to request SSML marks for word-level timestamps.
- **Output**: Generates `.opus` files.
- **Storage**: Uploads to R2 at `readings/{iso_code}/{reading_id}.opus`.
- **Metadata**: Updates the database with the R2 file key and the timestamp alignment JSON.
- **Schedule**: Hourly at minute 30 (`30 * * * *`).

## 🛠️ Maintenance

**Adding new dependencies**: 
Add them to `requirements.txt` and restart:
```powershell
astro dev restart
```

**Stop the environment**:
```powershell
astro dev stop
```

**View logs**:
```powershell
astro dev logs
```

**Clear DAG run history**:
To delete all run history for a specific DAG (useful for testing from scratch):
```powershell
astro dev bash
# Inside the container:
airflow dags delete verb_audio_generation_batch --yes
airflow dags delete reading_audio_generation_batch --yes
exit
```
