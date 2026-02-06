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
BATCH_SIZE = 10
MAX_BATCH_TOKENS = 20000
DAG_VERSION = "1.6.0-jsonl-upload"

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
    schedule="*/15 * * * *",
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
        Returns: dict with 'jsonl_lines' (list of dicts) and 'metadata' dict keyed by index
        """
        jsonl_lines = []
        request_metadata = {}  # Dict keyed by string index for easy lookup
        
        for idx, conj in enumerate(conjugations):
            subject = derive_subject(conj['pronoun'], conj['language_name'])
            prompt_text = f"{subject} {conj['form']}."
            pronounciation_note = get_pronunciation_note(
                conj['pronoun'], 
                conj['tense'], 
                conj['form'], 
                conj['language_name']
            )
            sys_instruction = get_system_prompt(conj['language_name'])
            user_content = f"{prompt_text} {pronounciation_note}"
            
            # Get language code for Speech Config
            lang_code = get_language_code(conj['language_name'])
            
            # Build JSONL line using the EXACT format from working tts.py example:
            # {"key": "<index>", "request": {...}}
            jsonl_line = {
                "key": str(idx),
                "request": {
                    "contents": [{"parts": [{"text": f"{sys_instruction}\n\n{user_content}"}]}],
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
                'user_content': user_content
            }
        
        logging.info(f"Built {len(jsonl_lines)} batch requests in JSONL format")
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
        Returns: batch job name and metadata
        """
        import tempfile
        
        if not GEMINI_API_KEY:
            raise AirflowFailException("GEMINI_API_KEY not found")

        client = genai.Client(api_key=GEMINI_API_KEY)
        jsonl_lines = batch_data['jsonl_lines']
        
        logging.info(f"Submitting batch job with {len(jsonl_lines)} requests via JSONL file")
        
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
            
            # 3. Create batch job using the uploaded file as source
            batch_job = client.batches.create(
                model=GEMINI_MODEL,
                src=uploaded_file.name,
                config={'display_name': f'verb_audio_{int(time.time())}'}
            )
            
            job_name = batch_job.name
            logging.info(f"Batch job submitted: {job_name}")
            
            # Clean up temp file
            try:
                os.remove(jsonl_path)
            except:
                pass
            
            return {
                'job_name': job_name,
                'metadata': batch_data['metadata']
            }
            
        except Exception as e:
            logging.error(f"Failed to submit batch job: {e}")
            raise AirflowFailException(f"Batch submission failed: {e}")

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
        Process audio results and upload to R2.
        Returns: list of processed file info
        """
        from botocore.config import Config
        
        import base64

        s3_client = boto3.client(
            service_name="s3",
            endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            region_name="auto",
            config=Config(signature_version='s3v4')
        )

        results = batch_results['results']
        metadata = batch_results['metadata']  # Now a dict keyed by string index
        
        processed_files = []
        failed_items = []
        
        for result in results:
            # Extract the key from the JSONL result (matches tts.py pattern)
            key = result.get('key')
            if key is None:
                logging.warning(f"Result missing 'key' field, skipping: {result}")
                continue
            
            # Look up metadata using the key
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
                # JSONL format: {"key": "...", "response": {"candidates": [...]}}
                response = result.get('response')
                audio_chunks = []
                
                # Navigate dict structure: response -> candidates -> content -> parts -> inlineData
                if response and response.get('candidates'):
                    candidates = response['candidates']
                    if candidates and candidates[0].get('content'):
                         content = candidates[0]['content']
                         if content.get('parts'):
                             for part in content['parts']:
                                 inline_data = part.get('inlineData')  # camelCase in JSONL
                                 if inline_data and inline_data.get('data'):
                                     # Decode base64 data
                                     data_encoded = inline_data['data']
                                     data_bytes = base64.b64decode(data_encoded)
                                     
                                     audio_chunks.append({
                                         'data': data_bytes,
                                         'mime_type': inline_data.get('mimeType')  # camelCase
                                     })
                
                if not audio_chunks:
                    logging.error(f"No audio data for conjugation {c_id}")
                    failed_items.append({'conjugation_id': c_id, 'error': 'No audio data'})
                    continue
                
                # Convert to Opus
                raw_audio = b''.join([chunk['data'] for chunk in audio_chunks])
                
                # Detect mime type from chunks
                mime_type = audio_chunks[0].get('mime_type')
                
                # Gemini TTS 2.5 Batch models return raw 16-bit PCM at 24kHz by default
                # and often don't return a specific mime type in the inline data.
                # If explicit mime type is missing or generic, enforce the known PCM format.
                if not mime_type or mime_type.lower() == "application/octet-stream":
                    mime_type = "audio/L16;rate=24000"
                    
                logging.info(f"Processing audio for conjugation {c_id} using mime_type: {mime_type}")
                
                # Debug: Log the first 64 bytes to diagnose format (Endianness, Header, etc.)
                if raw_audio:
                    header_hex = binascii.hexlify(raw_audio[:64]).decode('utf-8')
                    logging.info(f"Raw Audio Header (64 bytes): {header_hex}")

                # Check for RIFF header (WAV)
                is_riff = raw_audio.startswith(b'RIFF')

                if (("pcm" in mime_type.lower() or "l16" in mime_type.lower()) and not is_riff):
                    # Wrap raw PCM into WAV container using wave module
                    # Settings: 1 channel, 16-bit (2 bytes), 24kHz
                    # This is the same approach used in the working example script.
                    logging.info(f"Wrapping raw PCM into WAV container (1ch, 16-bit, 24kHz)")
                    
                    wav_buffer = io.BytesIO()
                    with wave.open(wav_buffer, 'wb') as wav_file:
                        wav_file.setnchannels(1)        # Mono
                        wav_file.setsampwidth(2)        # 16-bit (2 bytes per sample)
                        wav_file.setframerate(24000)    # 24kHz
                        wav_file.writeframes(raw_audio)
                    
                    wav_buffer.seek(0)
                    audio_data = wav_buffer.getvalue()
                    
                    # Load for validation only
                    audio = AudioSegment.from_file(io.BytesIO(audio_data), format="wav")

                else:
                    # Container format (MP3, WAV, OGG, etc.) OR Unexpected RIFF in PCM mime type
                    logging.info(f"Treating audio as container format (Is RIFF: {is_riff}, Mime: {mime_type})")
                    audio = AudioSegment.from_file(io.BytesIO(raw_audio))
                    
                    # Export to WAV for testing
                    wav_buffer = io.BytesIO()
                    audio.export(wav_buffer, format="wav")
                    audio_data = wav_buffer.getvalue()

                # Validate Audio (Shared)
                validate_audio_segment(audio)

                # Generate key based on metadata (using .wav for testing)
                safe_lang = meta['iso_code'].lower() if meta.get('iso_code') else meta.get('language_name', 'unknown').lower()
                safe_verb = meta['verb'].replace(" ", "_").lower()
                safe_tense = meta['tense'].replace(" ", "_").lower()
                safe_subject = meta['subject'].replace(" ", "_").lower()
                safe_form = meta['form'].replace(" ", "_").lower()
                
                file_key = f"conjugation/{safe_lang}/{safe_verb}/{safe_tense}/{safe_subject}_{safe_form}.wav"

                # Upload to R2 (WAV for testing)
                s3_client.put_object(
                    Bucket=R2_BUCKET_NAME,
                    Key=file_key,
                    Body=audio_data,
                    ContentType="audio/wav"
                )
                
                processed_files.append({
                    'conjugation_id': c_id,
                    'file_key': file_key,
                    'size_bytes': len(audio_data)
                })
                
                logging.info(f"Uploaded {file_key} ({len(audio_data)} bytes)")

            except Exception as e:
                logging.error(f"Failed to process conjugation {c_id}: {e}")
                failed_items.append({'conjugation_id': c_id, 'error': str(e)})

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