import os
import io
import json
import logging
import logging
import random
import time
import wave
import subprocess
import binascii
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException, AirflowSkipException
from google import genai
from google.genai import types
import boto3
from pydub import AudioSegment
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Import helpers
try:
    from dags.utils.verb_audio_helpers import (
        get_system_prompt,
        get_language_code,
        derive_subject,
        get_pronunciation_note,
        convert_to_wav,
        validate_audio_segment
    )
except ImportError:
    from utils.verb_audio_helpers import (
        get_system_prompt,
        get_language_code,
        derive_subject,
        get_pronunciation_note,
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

# Tier 1 Batch Config
# Gemini 2.5 TTS Tier 1: 25,000 batch enqueued tokens
# MINIMAL TEXT MODE: ~30 tokens per request (quality confirmed!)
# 300 requests × 30 tokens = ~9k tokens (safe margin under 25k)
BATCH_SIZE = 300
MAX_BATCH_TOKENS = 25000
DAG_VERSION = "2.0.1-minimal-text-production"

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}

@dag(
    dag_id="verb_audio_generation_batch",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="*/40 * * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["languro", "audio", "gemini", "batch"],
)
def verb_audio_generation_batch_dag():

    @task
    def fetch_pending_conjugations():
        """
        Fetch conjugations that need audio generation.
        Returns: List of conjugation records
        """
        pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        query = f"""
            SELECT 
                c.id as conjugation_id,
                v.word as verb,
                p.label as pronoun,
                t.tense_name as tense,
                c.display_form as conjugated_form,
                c.pronoun_id,
                c.tense_id,
                c.verb_translation_id,
                l.name as language_name,
                l.iso_code
            FROM conjugations c
            JOIN verb_translations v ON c.verb_translation_id = v.id
            JOIN pronouns p ON c.pronoun_id = p.id
            JOIN tenses t ON c.tense_id = t.id
            JOIN languages l ON v.language_id = l.id
            WHERE c.has_audio = false
            ORDER BY c.id
            LIMIT {BATCH_SIZE};
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        if not rows:
            logging.info("No pending conjugations found")
            raise AirflowSkipException("No work to do")

        # Convert to list of dicts for easier downstream processing
        conjugations = []
        for row in rows:
            c_id, verb, pronoun, tense, form, p_id, t_id, v_id, lang, iso_code = row
            conjugations.append({
                'conjugation_id': c_id,
                'verb': verb,
                'pronoun': pronoun,
                'tense': tense,
                'form': form,
                'pronoun_id': p_id,
                'tense_id': t_id,
                'verb_translation_id': v_id,
                'language_name': lang,
                'iso_code': iso_code
            })

        logging.info(f"Found {len(conjugations)} pending conjugations")
        return conjugations

    @task
    def build_batch_requests(conjugations: list):
        """
        Build Gemini API batch requests from conjugation data.
        Uses the JSONL file format like the working tts.py example.
        Each line: {"key": "...", "request": {...}}
        
        MINIMAL TEXT APPROACH: Only sends the text to speak.
        The languageCode in speechConfig handles pronunciation.
        This reduces tokens from ~200 to ~30 per request.
        
        Returns: dict with 'jsonl_lines' (list of dicts) and 'metadata' dict keyed by index
        """
        jsonl_lines = []
        request_metadata = {}  # Dict keyed by string index for easy lookup
        
        for idx, conj in enumerate(conjugations):
            subject = derive_subject(conj['pronoun'], conj['language_name'])
            
            # MINIMAL TEXT: Just the conjugation to speak
            # e.g., "yo como" instead of full instruction
            text_to_speak = f"{subject} {conj['form']}"
            
            # Get language code for Speech Config (this handles pronunciation)
            lang_code = get_language_code(conj['language_name'])
            
            # Build JSONL line - minimal text, rely on languageCode for pronunciation
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
            request_metadata[str(idx)] = {
                **conj,  # Include all conjugation data
                'subject': subject,
                'text_to_speak': text_to_speak
            }
        
        logging.info(f"Built {len(jsonl_lines)} batch requests (minimal text mode)")
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
        Submit batch job to Gemini API using JSONL file upload.
        This matches the working tts.py example approach.
        Includes retry logic for 429 rate limit errors.
        Returns: batch job name and metadata
        """
        import tempfile
        
        if not GEMINI_API_KEY:
            raise AirflowFailException("GEMINI_API_KEY not found")

        client = genai.Client(api_key=GEMINI_API_KEY)
        jsonl_lines = batch_data['jsonl_lines']
        
        logging.info(f"Submitting batch job with {len(jsonl_lines)} requests via JSONL file")
        
        # Retry settings for 429 errors
        MAX_RETRIES = 5
        BASE_DELAY = 60  # Start with 1 minute wait
        
        jsonl_path = None
        uploaded_file = None
        
        try:
            # 1. Create a temporary JSONL file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False, encoding='utf-8') as f:
                for line in jsonl_lines:
                    f.write(json.dumps(line, ensure_ascii=False) + '\n')
                jsonl_path = f.name
            
            logging.info(f"Created JSONL file: {jsonl_path}")
            
            # 2. Upload the JSONL file to the File API
            uploaded_file = client.files.upload(
                file=jsonl_path,
                config=types.UploadFileConfig(
                    display_name=f'verb_audio_batch_{int(time.time())}',
                    mime_type='jsonl'
                )
            )
            logging.info(f"Uploaded file: {uploaded_file.name}")
            
            # 3. Create batch job with retry for 429 errors
            last_error = None
            for attempt in range(MAX_RETRIES):
                try:
                    batch_job = client.batches.create(
                        model=GEMINI_MODEL,
                        src=uploaded_file.name,
                        config={'display_name': f'verb_audio_{int(time.time())}'}
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
                        wait_time = BASE_DELAY * (2 ** attempt)  # Exponential backoff
                        logging.warning(
                            f"Rate limited (429), attempt {attempt + 1}/{MAX_RETRIES}. "
                            f"Waiting {wait_time}s before retry..."
                        )
                        last_error = e
                        time.sleep(wait_time)
                    else:
                        raise  # Re-raise non-429 errors immediately
            
            # All retries exhausted
            raise AirflowFailException(f"Batch submission failed after {MAX_RETRIES} retries: {last_error}")
            
        except AirflowFailException:
            raise
        except Exception as e:
            logging.error(f"Failed to submit batch job: {e}")
            raise AirflowFailException(f"Batch submission failed: {e}")
        finally:
            # Clean up temp file
            if jsonl_path:
                try:
                    os.remove(jsonl_path)
                except:
                    pass

    @task(
        retries=5,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=15),
    )
    def poll_batch_completion(batch_info: dict):
        """
        Poll batch job until completion.
        Returns: batch info with job status
        """
        if not GEMINI_API_KEY:
            raise AirflowFailException("GEMINI_API_KEY not found")

        client = genai.Client(api_key=GEMINI_API_KEY)
        job_name = batch_info['job_name']
        
        @retry(
            stop=stop_after_attempt(5),
            wait=wait_exponential(multiplier=1, min=4, max=30),
            retry=retry_if_exception_type((Exception,)),
            reraise=True
        )
        def get_batch_job_status(job_name):
            """Get batch job status with retry on transient errors"""
            try:
                return client.batches.get(name=job_name)
            except Exception as e:
                logging.warning(f"Transient error fetching batch status: {e}")
                raise

        # Poll for completion
        max_wait_time = 600  # 10 minutes
        poll_interval = 15
        elapsed = 0
        
        while elapsed < max_wait_time:
            try:
                job_status = get_batch_job_status(job_name)
                state = job_status.state
                
                logging.info(f"Batch job state: {state} (elapsed: {elapsed}s)")
                
                if state == "JOB_STATE_SUCCEEDED":
                    logging.info("Batch job completed successfully")
                    return batch_info
                    
                elif state in ["JOB_STATE_FAILED", "JOB_STATE_CANCELLED"]:
                    raise AirflowFailException(f"Batch job failed with state: {state}")
                    
                elif state == "JOB_STATE_EXPIRED":
                    raise AirflowFailException("Batch job expired")
                    
                time.sleep(min(poll_interval, 60))
                elapsed += poll_interval
                poll_interval = min(poll_interval * 2, 60)
                
            except AirflowFailException:
                raise
            except Exception as e:
                if "not found" in str(e).lower():
                    raise AirflowFailException(f"Batch job {job_name} disappeared")
                raise

        raise AirflowFailException(f"Batch job timeout after {max_wait_time}s")

    @task(
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    def fetch_batch_results(batch_info: dict):
        """
        Fetch results from completed batch job.
        Downloads the output JSONL file from the File API.
        Returns: dict with results and metadata
        """
        if not GEMINI_API_KEY:
            raise AirflowFailException("GEMINI_API_KEY not found")

        client = genai.Client(api_key=GEMINI_API_KEY)
        job_name = batch_info['job_name']
        
        logging.info(f"Fetching batch results... (DAG Version: {DAG_VERSION})")
        
        try:
            # Fetch batch job details
            batch_job = client.batches.get(name=job_name)
            
            # For JSONL file-based batch jobs, results are in a file
            # The dest.file_name contains the output file reference
            if not batch_job.dest or not batch_job.dest.file_name:
                logging.warning("Batch job succeeded but no output file found.")
                return {
                    'results': [],
                    'metadata': batch_info['metadata']
                }
            
            # Download the output file content
            output_file_name = batch_job.dest.file_name
            logging.info(f"Downloading results from file: {output_file_name}")
            
            # Download the file content
            content = client.files.download(file=output_file_name).decode('utf-8')
            
            # Parse each line of the JSONL response
            results = []
            for line_idx, line in enumerate(content.splitlines()):
                if not line.strip():
                    continue
                try:
                    data = json.loads(line)
                    results.append(data)
                except json.JSONDecodeError as e:
                    logging.warning(f"Failed to parse result line {line_idx}: {e}")
                    continue
            
            logging.info(f"Retrieved {len(results)} results from output file")
            
            return {
                'results': results,
                'metadata': batch_info['metadata']
            }
            
        except Exception as e:
            raise AirflowFailException(f"Failed to fetch batch results: {e}")

    @task(
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    def process_and_upload_results(batch_results: dict):
        """
        Process audio results and upload to R2 in parallel.
        Returns: list of processed file info
        """
        from botocore.config import Config
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import base64

        # Number of concurrent uploads (adjust based on performance)
        MAX_UPLOAD_WORKERS = 20

        s3_client = boto3.client(
            service_name="s3",
            endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            region_name="auto",
            config=Config(signature_version='s3v4')
        )

        results = batch_results['results']
        metadata = batch_results['metadata']  # Dict keyed by string index
        
        # Stage 1: Process all audio files first (CPU-bound)
        files_to_upload = []  # List of (c_id, file_key, audio_data)
        failed_items = []
        
        logging.info(f"Processing {len(results)} audio results...")
        
        for result in results:
            # Extract the key from the JSONL result
            key = result.get('key')
            if key is None:
                logging.warning(f"Result missing 'key' field, skipping")
                continue
            
            meta = metadata.get(str(key))
            if not meta:
                logging.warning(f"No metadata found for key {key}, skipping")
                continue
            
            c_id = meta['conjugation_id']
            
            # Check for errors in the result
            if result.get('error'):
                logging.error(f"Batch error for conjugation {c_id}: {result.get('error')}")
                failed_items.append({'conjugation_id': c_id, 'error': str(result.get('error'))})
                continue
            
            try:
                # Extract audio chunks from the response
                response = result.get('response')
                audio_chunks = []
                
                if response and response.get('candidates'):
                    candidates = response['candidates']
                    if candidates and candidates[0].get('content'):
                         content = candidates[0]['content']
                         if content.get('parts'):
                             for part in content['parts']:
                                 inline_data = part.get('inlineData')
                                 if inline_data and inline_data.get('data'):
                                     data_bytes = base64.b64decode(inline_data['data'])
                                     audio_chunks.append({
                                         'data': data_bytes,
                                         'mime_type': inline_data.get('mimeType')
                                     })
                
                if not audio_chunks:
                    logging.error(f"No audio data for conjugation {c_id}")
                    failed_items.append({'conjugation_id': c_id, 'error': 'No audio data'})
                    continue
                
                # Combine audio chunks
                raw_audio = b''.join([chunk['data'] for chunk in audio_chunks])
                mime_type = audio_chunks[0].get('mime_type') or "audio/L16;rate=24000"
                
                # Check for RIFF header (WAV)
                is_riff = raw_audio.startswith(b'RIFF')

                if (("pcm" in mime_type.lower() or "l16" in mime_type.lower()) and not is_riff):
                    # Wrap raw PCM into WAV container
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
                validate_audio_segment(audio)

                # Convert to Opus
                opus_buffer = io.BytesIO()
                audio.export(opus_buffer, format="opus", codec="libopus", bitrate="64k")
                audio_data = opus_buffer.getvalue()

                # Generate file key
                safe_lang = meta['iso_code'].lower() if meta.get('iso_code') else meta.get('language_name', 'unknown').lower()
                safe_verb = meta['verb'].replace(" ", "_").lower()
                safe_tense = meta['tense'].replace(" ", "_").lower()
                safe_subject = meta['subject'].replace(" ", "_").lower()
                safe_form = meta['form'].replace(" ", "_").lower()
                
                file_key = f"conjugation/{safe_lang}/{safe_verb}/{safe_tense}/{safe_subject}_{safe_form}.opus"
                
                files_to_upload.append((c_id, file_key, audio_data))

            except Exception as e:
                logging.error(f"Failed to process conjugation {c_id}: {e}")
                failed_items.append({'conjugation_id': c_id, 'error': str(e)})

        logging.info(f"Processed {len(files_to_upload)} audio files, {len(failed_items)} failed")
        
        # Stage 2: Upload in parallel (I/O-bound)
        processed_files = []
        
        def upload_file(file_info):
            """Upload a single file to R2."""
            c_id, file_key, audio_data = file_info
            s3_client.put_object(
                Bucket=R2_BUCKET_NAME,
                Key=file_key,
                Body=audio_data,
                ContentType="audio/opus"
            )
            return {
                'conjugation_id': c_id,
                'file_key': file_key,
                'size_bytes': len(audio_data)
            }
        
        if files_to_upload:
            logging.info(f"Uploading {len(files_to_upload)} files to R2 with {MAX_UPLOAD_WORKERS} workers...")
            
            with ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS) as executor:
                future_to_file = {
                    executor.submit(upload_file, f): f for f in files_to_upload
                }
                
                for future in as_completed(future_to_file):
                    file_info = future_to_file[future]
                    c_id = file_info[0]
                    try:
                        result = future.result()
                        processed_files.append(result)
                    except Exception as e:
                        logging.error(f"Upload failed for conjugation {c_id}: {e}")
                        failed_items.append({'conjugation_id': c_id, 'error': str(e)})
            
            logging.info(f"Upload complete: {len(processed_files)} successful")

        logging.info(
            f"Processing complete: {len(processed_files)} successful, "
            f"{len(failed_items)} failed"
        )
        
        if not processed_files:
            raise AirflowFailException(f"All {len(failed_items)} items failed processing")
        
        return {
            'processed_files': processed_files,
            'failed_items': failed_items
        }

    @task(
        retries=3,
        retry_delay=timedelta(seconds=30),
    )
    def update_database(upload_results: dict):
        """
        Update database with audio file information.
        Includes rollback on failure.
        """
        from botocore.config import Config
        
        # S3 client for potential rollback
        s3_client = boto3.client(
            service_name="s3",
            endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            region_name="auto",
            config=Config(signature_version='s3v4')
        )
        
        pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        processed_files = upload_results['processed_files']
        failed_items = upload_results['failed_items']
        
        try:
            # Batch update all successful files
            for file_info in processed_files:
                update_query = """
                    UPDATE conjugations 
                    SET has_audio = true, audio_file_key = %s 
                    WHERE id = %s
                """
                cursor.execute(update_query, (file_info['file_key'], file_info['conjugation_id']))
            
            # Commit transaction
            conn.commit()
            logging.info(f"Successfully updated {len(processed_files)} database records")
            
        except Exception as commit_error:
            logging.error(f"Database commit failed: {commit_error}")
            conn.rollback()
            
            # Rollback S3 uploads
            logging.warning("Attempting to rollback S3 uploads...")
            rollback_failures = []
            for file_info in processed_files:
                try:
                    s3_client.delete_object(
                        Bucket=R2_BUCKET_NAME, 
                        Key=file_info['file_key']
                    )
                    logging.info(f"Rolled back: {file_info['file_key']}")
                except Exception as rb_error:
                    logging.error(f"Rollback failed for {file_info['file_key']}: {rb_error}")
                    rollback_failures.append(file_info['file_key'])
            
            cursor.close()
            conn.close()
            
            if rollback_failures:
                raise AirflowFailException(
                    f"DB commit failed. Rollback partially failed for: {rollback_failures}"
                )
            else:
                raise AirflowFailException(
                    f"DB commit failed. All S3 uploads rolled back successfully."
                )
        
        finally:
            cursor.close()
            conn.close()
        
        # Return summary
        return {
            'updated_count': len(processed_files),
            'failed_count': len(failed_items),
            'failed_items': failed_items
        }

    # Define task dependencies
    conjugations = fetch_pending_conjugations()
    batch_data = build_batch_requests(conjugations)
    batch_info = submit_batch_job(batch_data)
    completed_batch = poll_batch_completion(batch_info)
    batch_results = fetch_batch_results(completed_batch)
    upload_results = process_and_upload_results(batch_results)
    summary = update_database(upload_results)

# Instantiate the DAG
verb_audio_generation_batch_dag()