
import os
import io
import json
import logging
import base64
import re
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException, AirflowSkipException
from google.cloud import texttospeech

# Try to import v1beta1 which supports enable_time_pointing
TTS_BETA_AVAILABLE = False
try:
    from google.cloud import texttospeech_v1beta1 as tts
    TTS_BETA_AVAILABLE = True
    logging.info("Successfully imported texttospeech_v1beta1")
except ImportError as e1:
    logging.warning(f"Could not import texttospeech_v1beta1 from google.cloud: {e1}")
    try:
        import google.cloud.texttospeech_v1beta1 as tts
        TTS_BETA_AVAILABLE = True
        logging.info("Successfully imported google.cloud.texttospeech_v1beta1")
    except ImportError as e2:
        logging.warning(f"Could not import google.cloud.texttospeech_v1beta1: {e2}")
        # Fallback to standard v1 (won't support timestamps)
        tts = texttospeech
        logging.warning("USING STANDARD texttospeech (v1) - timestamps will NOT work!")
from google.oauth2 import service_account
import boto3
from pydub import AudioSegment
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME", "test-audio")

# Voice Mapping Configuration
# Can be overridden via Env Var: TTS_VOICE_MAPPING='{"en": "en-US-Neural2-F"}'
DEFAULT_VOICE_MAPPING = {
    "en": "en-US-Neural2-J",
    "es": "es-US-Neural2-B",
    "fr": "fr-FR-Neural2-B",
    "de": "de-DE-Neural2-B",
    "it": "it-IT-Neural2-C",
    "pt": "pt-BR-Neural2-B",
    "ru": "ru-RU-Wavenet-B", # Neural2 might not be available for all
    "ja": "ja-JP-Neural2-B",
    "ko": "ko-KR-Neural2-B",
    "zh": "cmn-CN-Wavenet-B",
}

def get_voice_for_language(iso_code):
    """
    Get the configured voice for a language.
    Falls back to environment variable mapping, then default mapping.
    """
    try:
        env_mapping_str = os.environ.get("TTS_VOICE_MAPPING", "{}")
        env_mapping = json.loads(env_mapping_str)
    except json.JSONDecodeError:
        logging.warning("Invalid JSON in TTS_VOICE_MAPPING env var, using defaults.")
        env_mapping = {}

    # Normalize iso_code (e.g. "en-US" -> "en")
    lang_prefix = iso_code.lower().split("-")[0]
    
    # Priority: Env -> Default -> Fallback
    voice = env_mapping.get(lang_prefix) or DEFAULT_VOICE_MAPPING.get(lang_prefix)
    
    if not voice:
         # Fallback logic if language not explicitly mapped
         # Try to reconstruct a reasonable default or fail safe
         logging.warning(f"No voice mapping found for {iso_code}, defaulting to en-US-Neural2-J")
         return "en-US-Neural2-J"
         
    return voice


DB_CONN_ID = "languro_db"
BATCH_SIZE = 10 # Process 10 readings at a time (Synchronous API calls)
DAG_VERSION = "2.4.0-request-object-pattern" # Version tracking

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
    schedule="30 * * * *", 
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["languro", "reading", "audio", "google-tts", "timestamps"],
)
def reading_audio_generation_batch_dag():
    
    # Log version at start of definition for visibility
    logging.info(f"Loading reading_audio_generation DAG Version: {DAG_VERSION}")


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
    def process_readings_with_google_tts(readings: list):
        """
        Process readings using Google Cloud TTS to get audio and timestamps.
        Uploads to R2 and returns results for DB update.
        """
        if not readings:
            return None

        # Initialize Google Cloud TTS Client
        try:
             creds = None
             # Check for JSON content in env var first (avoids file mounting issues)
             creds_json = os.environ.get("GOOGLE_APP_CREDS_JSON")
             if creds_json:
                 try:
                     creds_info = json.loads(creds_json)
                     creds = service_account.Credentials.from_service_account_info(creds_info)
                     logging.info("Loaded Google Credentials from GOOGLE_APP_CREDS_JSON")
                 except json.JSONDecodeError:
                     logging.warning("GOOGLE_APP_CREDS_JSON contains invalid JSON")

             if creds:
                 client = tts.TextToSpeechClient(credentials=creds)
             else:
                 # Fallback to ADC
                 logging.info("Attempting to load Google Credentials from ADC (Beta Client)...")
                 client = tts.TextToSpeechClient()
        except Exception as e:
             raise AirflowFailException(f"Failed to initialize Google TTS Client: {e}")

        s3_client = boto3.client(
            service_name="s3",
            endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            region_name="auto",
        )

        processed_results = []
        failed_items = []

        for reading in readings:
            r_id = reading['reading_id']
            iso_code = reading['iso_code']
            content = reading['content']

            try:
                # 1. Prepare SSML with Marks
                # Split by spaces to approximate words, but keep full punctuation for the "word" mapping if possible
                # A robust way is to find word boundaries. For simplicity, we'll split by spaces and wrap.
                
                # Cleaning extra whitespace
                text_clean = re.sub(r'\s+', ' ', content).strip()
                words = text_clean.split(' ')
                
                ssml_parts = []
                word_map = {} # Map index to word text for alignment JSON
                
                for i, word in enumerate(words):
                    # We wrap every "token" (word + potential punctuation attached)
                    # Use a safe mark name. "w{i}"
                    mark_name = f"w{i}"
                    ssml_parts.append(f'<mark name="{mark_name}"/>{word}')
                    word_map[mark_name] = word
                
                ssml_input = f"<speak>{' '.join(ssml_parts)}</speak>"
                
                # 2. Configure STS Request
                input_text = tts.SynthesisInput(ssml=ssml_input)
                
                voice_name = get_voice_for_language(iso_code)
                voice_lang_code = "-".join(voice_name.split("-")[:2]) # e.g. en-US-Neural2-J -> en-US
                
                voice_params = tts.VoiceSelectionParams(
                    language_code=voice_lang_code,
                    name=voice_name
                )
                
                audio_config = tts.AudioConfig(
                    audio_encoding=tts.AudioEncoding.MP3, # Get MP3 directly
                    speaking_rate=1.0
                )
                
                # 3. Call API using SynthesizeSpeechRequest object (required for enable_time_pointing)
                logging.info(f"Generating audio for reading {r_id} ({iso_code}) using {voice_name}")
                
                request = tts.SynthesizeSpeechRequest(
                    input=input_text,
                    voice=voice_params,
                    audio_config=audio_config,
                    enable_time_pointing=[tts.SynthesizeSpeechRequest.TimepointType.SSML_MARK],
                )
                
                response = client.synthesize_speech(request=request)
                
                # 4. Process Timestamps
                alignment = []
                for point in response.timepoints:
                    mark_name = point.mark_name
                    if mark_name in word_map:
                        alignment.append({
                            "word": word_map[mark_name],
                            "start": point.time_seconds
                        })
                
                # 5. Process Audio (MP3 -> Opus for standardization/size if desired, but User pattern asked for Opus)
                # Google returns MP3. We can store MP3 or convert to OPUS. 
                # Existing pattern uses Opus. Let's convert to Opus.
                
                audio_content = response.audio_content
                
                # Use pydub to convert MP3 bytes to Opus
                audio_segment = AudioSegment.from_file(io.BytesIO(audio_content), format="mp3")
                
                opus_buffer = io.BytesIO()
                audio_segment.export(opus_buffer, format="opus", codec="libopus", bitrate="64k")
                opus_data = opus_buffer.getvalue()
                
                # 6. Upload to R2
                safe_lang = iso_code.lower()
                file_key = f"readings/{safe_lang}/{r_id}.opus"
                
                s3_client.put_object(
                    Bucket=R2_BUCKET_NAME,
                    Key=file_key,
                    Body=opus_data,
                    ContentType="audio/opus"
                )
                
                processed_results.append({
                    "reading_id": r_id,
                    "file_key": file_key,
                    "alignment": json.dumps(alignment)
                })
                
            except Exception as e:
                logging.error(f"Failed to process reading {r_id}: {e}")
                failed_items.append({"reading_id": r_id, "error": str(e)})

        if failed_items:
            logging.warning(f"Failed items: {failed_items}")

        if not processed_results and failed_items:
             raise AirflowFailException("All items failed to process.")

        return processed_results

    @task
    def update_database(processed_results: list):
        """
        Update ReadingLesson table with audioKey and alignment.
        """
        if not processed_results:
            return

        pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            for item in processed_results:
                sql = """
                    UPDATE reading_lessons
                    SET "audioKey" = %s, alignment = %s::json
                    WHERE id = %s
                """
                cursor.execute(sql, (item['file_key'], item['alignment'], item['reading_id']))
                
            conn.commit()
            logging.info(f"Updated {len(processed_results)} reading lessons with audio and alignment.")
            
        except Exception as e:
            conn.rollback()
            raise AirflowFailException(f"DB Update failed: {e}")
        finally:
            cursor.close()
            conn.close()

    # Flow
    readings = fetch_pending_readings()
    results = process_readings_with_google_tts(readings)
    update_database(results)

reading_audio_generation_batch_dag()
