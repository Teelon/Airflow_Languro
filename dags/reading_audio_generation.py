import os
import io
import json
import logging
import time
import wave
import base64
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException, AirflowSkipException
from google import genai
from google.genai import types
import boto3
from pydub import AudioSegment
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Import helpers (reusing existing helpers where applicable)
try:
    from dags.utils.verb_audio_helpers import (
        get_language_code,
        convert_to_wav,
        validate_audio_segment
    )
except ImportError:
    from utils.verb_audio_helpers import (
        get_language_code,
        convert_to_wav,
        validate_audio_segment
    )

# Configuration
R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME", "test-audio")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL_TTS_MODEL", "gemini-2.5-pro-preview-tts")

DB_CONN_ID = "languro_db"

# Batch Config for Readings
# Readings are longer (approx 150 words), so fewer items per batch to stay under token limits.
# 150 words ~ 200 tokens. 
# 25,000 tokens / 200 = ~125 readings.
# Let's start with a small batch size of 10 for now.

BATCH_SIZE = 10
DAG_VERSION = "1.0.0"

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}

@dag(
    dag_id="reading_audio_generation_batch",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="30 * * * *",  # Run at minute 30 (offset from verb DAG)
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["languro", "reading", "audio", "gemini", "batch"],
)
def reading_audio_generation_batch_dag():

    @task
    def fetch_pending_readings():
        """
        Fetch reading lessons that need audio generation.
        Returns: List of reading records
        """
        pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Join with content_items and languages to get language code
        query = f"""
            SELECT 
                r.id as reading_id,
                r.title,
                r.content,
                l.name as language_name,
                l.iso_code
            FROM reading_lessons r
            JOIN content_items c ON r."contentItemId" = c.id
            JOIN languages l ON c."languageId" = l.id
            WHERE r."audioKey" IS NULL
            ORDER BY r.id
            LIMIT {BATCH_SIZE};
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        if not rows:
            logging.info("No pending readings found")
            raise AirflowSkipException("No work to do")

        # Convert to list of dicts
        readings = []
        for row in rows:
            r_id, title, content, lang_name, iso_code = row
            readings.append({
                'reading_id': r_id,
                'title': title,
                'content': content,
                'language_name': lang_name,
                'iso_code': iso_code
            })

        logging.info(f"Found {len(readings)} pending readings")
        return readings

    @task
    def build_batch_requests(readings: list):
        """
        Build Gemini API batch requests from reading data.
        """
        jsonl_lines = []
        request_metadata = {}
        
        for idx, reading in enumerate(readings):
            text_to_speak = reading['content']
            
            # Get language code (defaulting to en-US if missing, but should be there)
            lang_code = get_language_code(reading['language_name'])
            
            # Using 'Journey' voice for readings as it might be more narrative-friendly
            # or stick to 'Zephyr' for consistency. Let's start with Zephyr.
            
            jsonl_line = {
                "key": str(idx),
                "request": {
                    "contents": [{"parts": [{"text": text_to_speak}]}],
                    "generationConfig": {
                        "responseModalities": ["AUDIO"],
                        "speechConfig": {
                            "languageCode": lang_code,
                            "voiceConfig": {
                                "prebuiltVoiceConfig": {
                                    "voiceName": "Zephyr"
                                }
                            }
                        }
                    }
                }
            }
            
            jsonl_lines.append(jsonl_line)
            request_metadata[str(idx)] = reading
        
        logging.info(f"Built {len(jsonl_lines)} batch requests for readings")
        return {
            'jsonl_lines': jsonl_lines,
            'metadata': request_metadata
        }

    @task(
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
    )
    def submit_batch_job(batch_data: dict):
        """
        Submit batch job to Gemini API.
        """
        import tempfile
        
        if not GEMINI_API_KEY:
            raise AirflowFailException("GEMINI_API_KEY not found")

        client = genai.Client(api_key=GEMINI_API_KEY)
        jsonl_lines = batch_data['jsonl_lines']
        
        if not jsonl_lines:
            logging.info("No requests to submit")
            return None
            
        logging.info(f"Submitting batch job with {len(jsonl_lines)} reading requests")
        
        # Retry logic for 429s (same as verb dag)
        MAX_RETRIES = 5
        BASE_DELAY = 60
        
        jsonl_path = None
        
        try:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False, encoding='utf-8') as f:
                for line in jsonl_lines:
                    f.write(json.dumps(line, ensure_ascii=False) + '\n')
                jsonl_path = f.name
            
            uploaded_file = client.files.upload(
                file=jsonl_path,
                config=types.UploadFileConfig(
                    display_name=f'reading_audio_batch_{int(time.time())}',
                    mime_type='jsonl'
                )
            )
            
            last_error = None
            for attempt in range(MAX_RETRIES):
                try:
                    batch_job = client.batches.create(
                        model=GEMINI_MODEL,
                        src=uploaded_file.name,
                        config={'display_name': f'reading_audio_{int(time.time())}'}
                    )
                    
                    job_name = batch_job.name
                    logging.info(f"Batch job submitted: {job_name}")
                    
                    return {
                        'job_name': job_name,
                        'metadata': batch_data['metadata']
                    }
                except Exception as e:
                    error_str = str(e)
                    if '429' in error_str or 'RESOURCE_EXHAUSTED' in error_str:
                        wait_time = BASE_DELAY * (2 ** attempt)
                        logging.warning(f"Rate limited, waiting {wait_time}s...")
                        time.sleep(wait_time)
                        last_error = e
                    else:
                        raise

            raise AirflowFailException(f"Batch submission failed after retries: {last_error}")

        except Exception as e:
            raise AirflowFailException(f"Batch submission failed: {e}")
        finally:
            if jsonl_path and os.path.exists(jsonl_path):
                os.remove(jsonl_path)

    @task(
        retries=5,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=20), # Readings might take longer
    )
    def poll_batch_completion(batch_info: dict):
        """
        Poll batch job until completion.
        """
        if not batch_info:
            return None
            
        if not GEMINI_API_KEY:
            raise AirflowFailException("GEMINI_API_KEY not found")

        client = genai.Client(api_key=GEMINI_API_KEY)
        job_name = batch_info['job_name']
        
        @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=30))
        def get_status():
            return client.batches.get(name=job_name)

        max_wait_time = 1200 # 20 mins
        initial_delay = 120  # Readings might be faster to queue but processing takes time
        poll_interval = 30
        elapsed = 0
        
        logging.info(f"Waiting {initial_delay}s before first poll...")
        time.sleep(initial_delay)
        elapsed = initial_delay
        
        while elapsed < max_wait_time:
            job_status = get_status()
            state = job_status.state
            logging.info(f"Batch state: {state} ({elapsed}s)")
            
            if state == "JOB_STATE_SUCCEEDED":
                return batch_info
            elif state in ["JOB_STATE_FAILED", "JOB_STATE_CANCELLED", "JOB_STATE_EXPIRED"]:
                raise AirflowFailException(f"Batch job failed: {state}")
                
            time.sleep(poll_interval)
            elapsed += poll_interval
            
        raise AirflowFailException("Batch job timeout")

    @task(retries=2)
    def fetch_batch_results(batch_info: dict):
        if not batch_info: return None
        client = genai.Client(api_key=GEMINI_API_KEY)
        job_name = batch_info['job_name']
        
        batch_job = client.batches.get(name=job_name)
        if not batch_job.dest or not batch_job.dest.file_name:
            logging.warning("No output file found")
            return {'results': [], 'metadata': batch_info['metadata']}
            
        content = client.files.download(file=batch_job.dest.file_name).decode('utf-8')
        results = []
        for line in content.splitlines():
            if line.strip():
                try:
                    results.append(json.loads(line))
                except:
                    pass
        return {'results': results, 'metadata': batch_info['metadata']}

    @task(retries=2)
    def process_and_upload_results(batch_results: dict):
        """
        Process audio results and upload to R2.
        """
        if not batch_results: return None
        
        from botocore.config import Config
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        s3_client = boto3.client(
            service_name="s3",
            endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            region_name="auto",
            config=Config(signature_version='s3v4')
        )
        
        results = batch_results['results']
        metadata = batch_results['metadata']
        
        files_to_upload = []
        failed_items = []
        
        for result in results:
            key = result.get('key')
            if not key or key not in metadata: continue
            
            meta = metadata[key]
            r_id = meta['reading_id']
            
            if result.get('error'):
                failed_items.append({'reading_id': r_id, 'error': result.get('error')})
                continue
                
            try:
                # Extract Audio (Same logic as verb DAG)
                response = result.get('response')
                audio_chunks = []
                if response and response.get('candidates'):
                    if response['candidates'][0].get('content', {}).get('parts'):
                        for part in response['candidates'][0]['content']['parts']:
                            if part.get('inlineData'):
                                audio_chunks.append({
                                    'data': base64.b64decode(part['inlineData']['data']),
                                    'mime_type': part['inlineData']['mimeType']
                                })
                
                if not audio_chunks:
                    failed_items.append({'reading_id': r_id, 'error': 'No audio data'})
                    continue
                    
                raw_audio = b''.join([chunk['data'] for chunk in audio_chunks])
                mime_type = audio_chunks[0].get('mime_type') or "audio/L16;rate=24000"
                
                # Convert to WAV container if raw PCM
                is_riff = raw_audio.startswith(b'RIFF')
                if (("pcm" in mime_type.lower() or "l16" in mime_type.lower()) and not is_riff):
                    wav_buffer = io.BytesIO()
                    with wave.open(wav_buffer, 'wb') as wav_file:
                        wav_file.setnchannels(1)
                        wav_file.setsampwidth(2)
                        wav_file.setframerate(24000)
                        wav_file.writeframes(raw_audio)
                    wav_buffer.seek(0)
                    audio = AudioSegment.from_file(wav_buffer, format="wav")
                else:
                    audio = AudioSegment.from_file(io.BytesIO(raw_audio))
                
                # Validate Audio
                validate_audio_segment(audio, max_duration=300)

                # Convert to Opus
                opus_buffer = io.BytesIO()
                audio.export(opus_buffer, format="opus", codec="libopus", bitrate="64k")
                audio_data = opus_buffer.getvalue()
                
                # Generate File Key
                # readings/{iso_code}/{reading_id}.opus
                safe_lang = meta['iso_code'].lower()
                file_key = f"readings/{safe_lang}/{r_id}.opus"
                
                files_to_upload.append((r_id, file_key, audio_data))
                
            except Exception as e:
                failed_items.append({'reading_id': r_id, 'error': str(e)})
                
        # Upload
        processed_files = []
        if files_to_upload:
            def upload(info):
                r_id, f_key, data = info
                s3_client.put_object(
                    Bucket=R2_BUCKET_NAME,
                    Key=f_key,
                    Body=data,
                    ContentType="audio/opus"
                )
                return {'reading_id': r_id, 'file_key': f_key}
                
            with ThreadPoolExecutor(max_workers=10) as ex:
                futures = {ex.submit(upload, f): f for f in files_to_upload}
                for fut in as_completed(futures):
                    try:
                        processed_files.append(fut.result())
                    except Exception as e:
                        # Log error but don't stop everything
                        logging.error(f"Upload error: {e}")
                        
        return {
            'processed_files': processed_files,
            'failed_items': failed_items
        }

    @task(retries=3)
    def update_database(upload_results: dict):
        if not upload_results: return
        
        pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            for item in upload_results['processed_files']:
                # Update reading_lessons table
                # We do NOT set alignment here yet as we don't have it.
                sql = """
                    UPDATE reading_lessons
                    SET "audioKey" = %s
                    WHERE id = %s
                """
                cursor.execute(sql, (item['file_key'], item['reading_id']))
                
            conn.commit()
            logging.info(f"Updated {len(upload_results['processed_files'])} reading lessons")
            
        except Exception as e:
            conn.rollback()
            raise AirflowFailException(f"DB Update failed: {e}")
        finally:
            cursor.close()
            conn.close()

    # Flow
    readings = fetch_pending_readings()
    batch_data = build_batch_requests(readings)
    batch_info = submit_batch_job(batch_data)
    completed = poll_batch_completion(batch_info)
    results = fetch_batch_results(completed)
    uploaded = process_and_upload_results(results)
    update_database(uploaded)

reading_audio_generation_batch_dag()
