
import os
import io
import json
import logging
import mimetypes
import time
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from google import genai
from google.genai import types
import boto3
import struct
from pydub import AudioSegment

# Import helper functions from utils
# Make sure the folder 'dags/utils' has an __init__.py or is in pythonpath
try:
    from dags.utils.verb_audio_helpers import (
        get_system_prompt,
        derive_subject,
        get_pronunciation_note,
        convert_to_wav
    )
except ImportError:
    # Fallback if dags directory structure is different in execution env
    from utils.verb_audio_helpers import (
        get_system_prompt,
        derive_subject,
        get_pronunciation_note,
        convert_to_wav
    )

# Configuration from Environment Variables
R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME", "test-audio")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL_TTS_MODEL", "gemini-2.5-pro-preview-tts")

# Connection ID for the Languro Database
# derived from AIRFLOW_CONN_LANGURO_DB
DB_CONN_ID = "languro_db" 

default_args = {
    "owner": "airflow",
    "retries": 1,
}

@dag(
    dag_id="verb_audio_generation",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["languro", "audio", "gemini"],
)
def verb_audio_generation_dag():

    @task
    def process_pending_conjugations():
        """
        Fetches conjugations without audio, generates it using Gemini, 
        uploads to R2, and updates the database.
        """
        if not GEMINI_API_KEY:
            raise ValueError("GEMINI_API_KEY not found in environment")
        
        # Initialize Gemini Client
        client = genai.Client(api_key=GEMINI_API_KEY)
        
        from botocore.config import Config

        # Initialize S3 (R2) Client
        s3_client = boto3.client(
            service_name="s3",
            endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            region_name="auto",
            config=Config(signature_version='s3v4')
        )

        # Connect to Database
        pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()


        # Query for pending conjugations
        # Joining to get verb string, pronoun, and tense
        query = """
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
            LIMIT 5; 
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        logging.info(f"Found {len(rows)} conjugations pending audio.")

        
        failed_count = 0
        for row in rows:
            c_id, verb, pronoun, tense, form, p_id, t_id, v_id, lang, iso_code = row
            
            logging.info(f"Processing: [{lang}] {verb} - {pronoun} - {tense} ({form})")
            
            subject = derive_subject(pronoun, lang)
            prompt_text = f"{subject} {form}."
            pronounciation_note = get_pronunciation_note(pronoun, tense, form, lang)
            sys_instruction = get_system_prompt(lang)

            user_content = f"{prompt_text} {pronounciation_note}"
        
            logging.info(f"TTS User Content: {user_content}")

            # 2. Generate Audio using Streaming API
            max_retries = 5  # Increased from 3
            audio_chunks = []
            
            for attempt in range(max_retries):
                try:
                    audio_chunks = []  # Reset on each retry
                    
                    # Use streaming API (required for TTS)
                    for chunk in client.models.generate_content_stream(
                        model=GEMINI_MODEL,
                        contents=[
                            types.Content(
                                role="user",
                                parts=[
                                    types.Part.from_text(text=f"{sys_instruction}\n\n{user_content}")
                                ]
                            )
                        ],
                        config=types.GenerateContentConfig(
                            temperature=1,
                            response_modalities=["audio"],
                            speech_config=types.SpeechConfig(
                                voice_config=types.VoiceConfig(
                                    prebuilt_voice_config=types.PrebuiltVoiceConfig(voice_name="Zephyr")
                                )
                            )
                        )
                    ):
                        # Extract audio data from chunks
                        if (chunk.candidates and 
                            chunk.candidates[0].content and 
                            chunk.candidates[0].content.parts):
                            
                            for part in chunk.candidates[0].content.parts:
                                if part.inline_data and part.inline_data.data:
                                    audio_chunks.append({
                                        'data': part.inline_data.data,
                                        'mime_type': part.inline_data.mime_type
                                    })
                    
                    if audio_chunks:
                        break  # Success - exit retry loop
                        
                except Exception as e:
                    if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                        # Exponential backoff for rate limits
                        wait_time = (2 ** attempt) * 10  # 10s, 20s, 40s, 80s, 160s
                        logging.warning(
                            f"Rate limit hit for {verb} ({form}). "
                            f"Retrying in {wait_time}s... (Attempt {attempt + 1}/{max_retries})"
                        )
                        time.sleep(wait_time)
                    else:
                        logging.error(f"Error calling Gemini API: {e}")
                        break  # Non-retryable error
            
            if not audio_chunks:
                logging.warning(f"No audio data generated for {c_id}")
                failed_count += 1
                continue

            # 3. Combine audio chunks and convert to WAV
            raw_audio = b''.join([chunk['data'] for chunk in audio_chunks])
            mime_type = audio_chunks[0]['mime_type'] if audio_chunks else "audio/L16;rate=24000"
            
            logging.info(f"Received audio MIME type: {mime_type}, size: {len(raw_audio)} bytes")
            
            # Convert raw PCM to WAV
            wav_data = convert_to_wav(raw_audio, mime_type)
            
            # 4. Convert WAV to Opus for smaller file size
            try:
                audio = AudioSegment.from_wav(io.BytesIO(wav_data))
                opus_buffer = io.BytesIO()
                audio.export(
                    opus_buffer,
                    format="opus",
                    codec="libopus",
                    bitrate="64k"  # Adjust: 32k (smaller) to 128k (higher quality)
                )
                
                audio_data = opus_buffer.getvalue()
                content_type = "audio/opus"
                extension = "opus"
                
                logging.info(f"Converted to Opus. Size: {len(audio_data)} bytes (from {len(wav_data)} bytes WAV)")
                
            except Exception as e:
                logging.error(f"Opus conversion failed: {e}. Falling back to WAV.")
                audio_data = wav_data
                content_type = "audio/wav"
                extension = "wav"
            
            # Structure: {language}/{verb}/{tense}/{pronoun}_{conjugated-verb}.{extension}
            # Example: es/comer/presente/yo_como.wav
            
            # Sanitize components for filename
            safe_lang = iso_code.lower() if iso_code else lang.lower()
            safe_verb = verb.replace(" ", "_").lower()
            safe_tense = tense.replace(" ", "_").lower()
            safe_subject = subject.replace(" ", "_").lower()
            safe_form = form.replace(" ", "_").lower()
            
            file_key = f"conjugation/{safe_lang}/{safe_verb}/{safe_tense}/{safe_subject}_{safe_form}.{extension}"
            
            logging.info(f"Attempting upload to bucket: {R2_BUCKET_NAME} with key: {file_key} (Type: {content_type})")

            try:
                s3_client.put_object(
                    Bucket=R2_BUCKET_NAME,
                    Key=file_key,
                    Body=audio_data,
                    ContentType=content_type
                )
                logging.info(f"Uploaded to R2: {file_key}")

                # 4. Update Database
                update_query = """
                    UPDATE conjugations 
                    SET has_audio = true, audio_file_key = %s 
                    WHERE id = %s
                """
                cursor.execute(update_query, (file_key, c_id))
                conn.commit()

            except Exception as e:
                logging.error(f"Error uploading/updating for {c_id}: {e}")
                failed_count += 1


            
            # Rate limiting: Sleep between each item processed
            time.sleep(5) 
        
        cursor.close()
        conn.close()

        if failed_count > 0:
            raise Exception(f"Job failed: {failed_count} conjugations failed to process.")

    # Execute the task
    process_pending_conjugations()

# Instantiate the DAG
verb_audio_generation_dag()
