# Gemini TTS Batch API - Complete Developer Guide

> **Last Updated:** 2026-02-05  
> **Tested With:** `google-genai` Python SDK, Model: `gemini-2.5-flash-preview-tts`

This guide documents how to use the Gemini API's Batch mode for Text-to-Speech (TTS) audio generation. Since Google's official documentation is incomplete, this serves as a practical reference.

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Request Format (JSONL)](#request-format-jsonl)
4. [Submitting Batch Jobs](#submitting-batch-jobs)
5. [Polling for Completion](#polling-for-completion)
6. [Retrieving Results](#retrieving-results)
7. [Processing Audio Data](#processing-audio-data)
8. [Voice Configuration](#voice-configuration)
9. [Complete Working Example](#complete-working-example)
10. [Common Pitfalls](#common-pitfalls)

---

## Overview

The Gemini TTS Batch API allows you to submit multiple TTS requests in a single job. This is more efficient and cost-effective than making individual API calls.

**Key Insights:**
- **JSONL file upload is required** for TTS requests (inline requests don't support `generationConfig`)
- Audio is returned as **raw 16-bit PCM at 24kHz mono**
- Results are base64-encoded in the response file

---

## Prerequisites

```bash
pip install google-genai
```

```python
from google import genai
from google.genai import types

# Initialize client
client = genai.Client(api_key="YOUR_API_KEY")
```

---

## Request Format (JSONL)

### ⚠️ CRITICAL: Use JSONL File Upload, NOT Inline Requests

The Gemini Python SDK's `batches.create()` with inline requests does **NOT** support `generationConfig`. You MUST use the JSONL file upload approach.

### JSONL Line Format

Each line in the JSONL file must be a valid JSON object:

```json
{
  "key": "unique-identifier-0",
  "request": {
    "contents": [
      {
        "parts": [
          {
            "text": "Your text to synthesize here."
          }
        ]
      }
    ],
    "generationConfig": {
      "responseModalities": ["AUDIO"],
      "speechConfig": {
        "languageCode": "en-US",
        "voiceConfig": {
          "prebuiltVoiceConfig": {
            "voiceName": "Kore"
          }
        }
      }
    }
  }
}
```

### Field Reference

| Field | Required | Description |
|-------|----------|-------------|
| `key` | Yes | Unique identifier for matching results to requests |
| `request.contents` | Yes | Array containing the text content |
| `request.contents[].parts[].text` | Yes | The text to synthesize |
| `request.generationConfig.responseModalities` | Yes | Must be `["AUDIO"]` (uppercase) |
| `request.generationConfig.speechConfig.languageCode` | Recommended | BCP-47 language code (e.g., `en-US`, `fr-FR`) |
| `request.generationConfig.speechConfig.voiceConfig.prebuiltVoiceConfig.voiceName` | Recommended | Voice name (see [Voice Configuration](#voice-configuration)) |

### ⚠️ Important Notes on Field Names

- Use **camelCase** for all field names in the JSONL file
- `responseModalities` value must be `["AUDIO"]` (uppercase string in array)
- Do NOT include `role` in contents (it's optional and can cause issues)

---

## Submitting Batch Jobs

### Step 1: Create JSONL File

```python
import json
import tempfile

def create_jsonl_file(requests_data):
    """
    Create a JSONL file from request data.
    
    Args:
        requests_data: List of dicts, each with 'key', 'text', 'language_code', 'voice_name'
    
    Returns:
        Path to the created JSONL file
    """
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False, encoding='utf-8') as f:
        for item in requests_data:
            jsonl_line = {
                "key": str(item['key']),
                "request": {
                    "contents": [{"parts": [{"text": item['text']}]}],
                    "generationConfig": {
                        "responseModalities": ["AUDIO"],
                        "speechConfig": {
                            "languageCode": item.get('language_code', 'en-US'),
                            "voiceConfig": {
                                "prebuiltVoiceConfig": {
                                    "voiceName": item.get('voice_name', 'Kore')
                                }
                            }
                        }
                    }
                }
            }
            f.write(json.dumps(jsonl_line, ensure_ascii=False) + '\n')
        return f.name
```

### Step 2: Upload JSONL File

```python
from google.genai import types

# Upload the JSONL file to Gemini File API
uploaded_file = client.files.upload(
    file=jsonl_path,
    config=types.UploadFileConfig(
        display_name='my_tts_batch',
        mime_type='jsonl'  # Important: use 'jsonl' not 'application/jsonl'
    )
)
print(f"Uploaded file: {uploaded_file.name}")
```

### Step 3: Create Batch Job

```python
# Create batch job using the uploaded file
batch_job = client.batches.create(
    model="gemini-2.5-flash-preview-tts",  # TTS-specific model
    src=uploaded_file.name,  # Use the file name, not the file object
    config={'display_name': 'my_tts_job'}
)
print(f"Batch job created: {batch_job.name}")
```

---

## Polling for Completion

```python
import time

def wait_for_batch_completion(client, job_name, max_wait_seconds=600, poll_interval=15):
    """
    Poll batch job until completion.
    
    Args:
        client: Gemini client
        job_name: The batch job name
        max_wait_seconds: Maximum time to wait
        poll_interval: Time between polls
    
    Returns:
        Final batch job object
    
    Raises:
        Exception if job fails or times out
    """
    elapsed = 0
    
    while elapsed < max_wait_seconds:
        job = client.batches.get(name=job_name)
        state = job.state.name if hasattr(job.state, 'name') else str(job.state)
        
        print(f"Job state: {state} (elapsed: {elapsed}s)")
        
        if state == "JOB_STATE_SUCCEEDED":
            return job
        elif state in ["JOB_STATE_FAILED", "JOB_STATE_CANCELLED"]:
            raise Exception(f"Batch job failed: {state}")
        elif state == "JOB_STATE_EXPIRED":
            raise Exception("Batch job expired")
        
        time.sleep(poll_interval)
        elapsed += poll_interval
    
    raise Exception(f"Batch job timed out after {max_wait_seconds}s")
```

### Job States

| State | Description |
|-------|-------------|
| `JOB_STATE_PENDING` | Job is queued |
| `JOB_STATE_RUNNING` | Job is processing |
| `JOB_STATE_SUCCEEDED` | Job completed successfully |
| `JOB_STATE_FAILED` | Job failed |
| `JOB_STATE_CANCELLED` | Job was cancelled |
| `JOB_STATE_EXPIRED` | Job expired before completion |

---

## Retrieving Results

### Download Output File

```python
import json

def fetch_batch_results(client, job_name):
    """
    Fetch and parse results from a completed batch job.
    
    Args:
        client: Gemini client
        job_name: The batch job name
    
    Returns:
        List of result dicts, each with 'key' and 'response'
    """
    # Get job details
    job = client.batches.get(name=job_name)
    
    # Results are in the output file
    if not job.dest or not job.dest.file_name:
        raise Exception("No output file found")
    
    # Download the file
    content = client.files.download(file=job.dest.file_name).decode('utf-8')
    
    # Parse JSONL
    results = []
    for line in content.splitlines():
        if line.strip():
            results.append(json.loads(line))
    
    return results
```

### Result Format

Each result line in the output file:

```json
{
  "key": "unique-identifier-0",
  "response": {
    "candidates": [
      {
        "content": {
          "parts": [
            {
              "inlineData": {
                "mimeType": "audio/L16;codec=pcm;rate=24000",
                "data": "BASE64_ENCODED_PCM_DATA..."
              }
            }
          ]
        }
      }
    ]
  }
}
```

### ⚠️ Response Field Names (camelCase)

| Field Path | Description |
|------------|-------------|
| `key` | The unique identifier you provided |
| `response.candidates[0].content.parts[0].inlineData.data` | Base64-encoded audio |
| `response.candidates[0].content.parts[0].inlineData.mimeType` | Audio format info |

---

## Processing Audio Data

### Understanding the Audio Format

The TTS API returns:
- **Format:** Raw 16-bit PCM
- **Sample Rate:** 24000 Hz (24kHz)
- **Channels:** 1 (Mono)
- **Encoding:** Base64 in the `inlineData.data` field

### Converting to WAV

```python
import base64
import wave
import io

def pcm_to_wav(base64_audio, sample_rate=24000):
    """
    Convert base64-encoded raw PCM to WAV format.
    
    Args:
        base64_audio: Base64-encoded raw PCM data
        sample_rate: Sample rate (default 24000 for Gemini TTS)
    
    Returns:
        BytesIO containing WAV data
    """
    # Decode base64
    raw_audio = base64.b64decode(base64_audio)
    
    # Wrap in WAV container
    wav_buffer = io.BytesIO()
    with wave.open(wav_buffer, 'wb') as wav_file:
        wav_file.setnchannels(1)        # Mono
        wav_file.setsampwidth(2)        # 16-bit (2 bytes per sample)
        wav_file.setframerate(sample_rate)  # 24kHz
        wav_file.writeframes(raw_audio)
    
    wav_buffer.seek(0)
    return wav_buffer
```

### Converting to Other Formats (Opus, MP3, etc.)

Use pydub or ffmpeg after creating the WAV:

```python
from pydub import AudioSegment
import io

def wav_to_opus(wav_buffer):
    """Convert WAV to Opus format."""
    audio = AudioSegment.from_file(wav_buffer, format="wav")
    
    opus_buffer = io.BytesIO()
    audio.export(opus_buffer, format="opus", codec="libopus", bitrate="64k")
    opus_buffer.seek(0)
    
    return opus_buffer
```

### Complete Audio Extraction

```python
def extract_audio_from_result(result):
    """
    Extract audio from a batch result.
    
    Args:
        result: Dict from the JSONL output
    
    Returns:
        Tuple of (wav_bytes, mime_type) or (None, None) if no audio
    """
    response = result.get('response')
    if not response:
        return None, None
    
    candidates = response.get('candidates', [])
    if not candidates:
        return None, None
    
    content = candidates[0].get('content', {})
    parts = content.get('parts', [])
    
    for part in parts:
        inline_data = part.get('inlineData')  # camelCase!
        if inline_data and inline_data.get('data'):
            base64_audio = inline_data['data']
            mime_type = inline_data.get('mimeType', 'audio/L16;rate=24000')
            
            wav_buffer = pcm_to_wav(base64_audio)
            return wav_buffer.getvalue(), mime_type
    
    return None, None
```

---

## Voice Configuration

### Available Voices

| Voice Name | Description |
|------------|-------------|
| `Kore` | Default voice |
| `Zephyr` | Alternative voice |
| `Aoede` | Alternative voice |
| `Charon` | Alternative voice |
| `Fenrir` | Alternative voice |
| `Puck` | Alternative voice |

> Note: Available voices may vary. Check the latest Gemini documentation.

### Language Codes

Common BCP-47 language codes:

| Language | Code |
|----------|------|
| English (US) | `en-US` |
| English (UK) | `en-GB` |
| French (France) | `fr-FR` |
| Spanish (Spain) | `es-ES` |
| Spanish (Mexico) | `es-MX` |
| German | `de-DE` |
| Italian | `it-IT` |
| Portuguese (Brazil) | `pt-BR` |
| Japanese | `ja-JP` |
| Korean | `ko-KR` |
| Chinese (Simplified) | `zh-CN` |

---

## Complete Working Example

```python
import json
import time
import base64
import wave
import io
import tempfile
import os

from google import genai
from google.genai import types

def generate_tts_batch(api_key, texts, language_code="en-US", voice_name="Kore"):
    """
    Generate TTS audio for multiple texts using Gemini Batch API.
    
    Args:
        api_key: Your Gemini API key
        texts: List of strings to synthesize
        language_code: BCP-47 language code
        voice_name: Voice to use
    
    Returns:
        Dict mapping text index to WAV bytes
    """
    client = genai.Client(api_key=api_key)
    
    # 1. Create JSONL file
    jsonl_path = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False, encoding='utf-8') as f:
            for idx, text in enumerate(texts):
                line = {
                    "key": str(idx),
                    "request": {
                        "contents": [{"parts": [{"text": text}]}],
                        "generationConfig": {
                            "responseModalities": ["AUDIO"],
                            "speechConfig": {
                                "languageCode": language_code,
                                "voiceConfig": {
                                    "prebuiltVoiceConfig": {
                                        "voiceName": voice_name
                                    }
                                }
                            }
                        }
                    }
                }
                f.write(json.dumps(line, ensure_ascii=False) + '\n')
            jsonl_path = f.name
        
        # 2. Upload JSONL file
        uploaded_file = client.files.upload(
            file=jsonl_path,
            config=types.UploadFileConfig(
                display_name=f'tts_batch_{int(time.time())}',
                mime_type='jsonl'
            )
        )
        print(f"Uploaded: {uploaded_file.name}")
        
        # 3. Create batch job
        batch_job = client.batches.create(
            model="gemini-2.5-flash-preview-tts",
            src=uploaded_file.name,
            config={'display_name': f'tts_{int(time.time())}'}
        )
        print(f"Job created: {batch_job.name}")
        
        # 4. Poll for completion
        while True:
            job = client.batches.get(name=batch_job.name)
            state = job.state.name if hasattr(job.state, 'name') else str(job.state)
            
            if state == "JOB_STATE_SUCCEEDED":
                print("Job completed!")
                break
            elif state in ["JOB_STATE_FAILED", "JOB_STATE_CANCELLED", "JOB_STATE_EXPIRED"]:
                raise Exception(f"Job failed: {state}")
            
            print(f"Status: {state}...")
            time.sleep(15)
        
        # 5. Download results
        content = client.files.download(file=job.dest.file_name).decode('utf-8')
        
        # 6. Parse and convert audio
        audio_results = {}
        for line in content.splitlines():
            if not line.strip():
                continue
            
            data = json.loads(line)
            key = data.get('key')
            
            # Extract audio
            response = data.get('response', {})
            candidates = response.get('candidates', [])
            if candidates:
                parts = candidates[0].get('content', {}).get('parts', [])
                for part in parts:
                    inline_data = part.get('inlineData')
                    if inline_data and inline_data.get('data'):
                        # Convert PCM to WAV
                        raw_audio = base64.b64decode(inline_data['data'])
                        
                        wav_buffer = io.BytesIO()
                        with wave.open(wav_buffer, 'wb') as wav_file:
                            wav_file.setnchannels(1)
                            wav_file.setsampwidth(2)
                            wav_file.setframerate(24000)
                            wav_file.writeframes(raw_audio)
                        
                        audio_results[key] = wav_buffer.getvalue()
        
        return audio_results
    
    finally:
        # Clean up temp file
        if jsonl_path and os.path.exists(jsonl_path):
            os.remove(jsonl_path)


# Usage
if __name__ == "__main__":
    API_KEY = "your-api-key"
    
    texts = [
        "Hello, how are you?",
        "This is a test of the Gemini TTS API.",
        "Batch processing is efficient!"
    ]
    
    results = generate_tts_batch(API_KEY, texts)
    
    # Save audio files
    for key, wav_bytes in results.items():
        with open(f"output_{key}.wav", "wb") as f:
            f.write(wav_bytes)
        print(f"Saved output_{key}.wav")
```

---

## Common Pitfalls

### ❌ Using Inline Requests

**Wrong:**
```python
# This does NOT work for TTS with generationConfig!
batch_job = client.batches.create(
    model="gemini-2.5-flash-preview-tts",
    src=[{"contents": [...], "generationConfig": {...}}]  # ❌ ValidationError
)
```

**Right:**
```python
# Upload JSONL file first, then use file name
uploaded_file = client.files.upload(file=jsonl_path, ...)
batch_job = client.batches.create(
    model="...",
    src=uploaded_file.name  # ✅ Use file name
)
```

### ❌ Using snake_case in JSONL

**Wrong:**
```json
{
  "generation_config": {...},
  "response_modalities": ["AUDIO"],
  "speech_config": {...},
  "inline_data": {...}
}
```

**Right:**
```json
{
  "generationConfig": {...},
  "responseModalities": ["AUDIO"],
  "speechConfig": {...},
  "inlineData": {...}
}
```

### ❌ Lowercase "audio"

**Wrong:**
```json
"responseModalities": ["audio"]
```

**Right:**
```json
"responseModalities": ["AUDIO"]
```

### ❌ Treating Audio as Container Format

**Wrong:**
```python
# Raw PCM is not a container format!
audio = AudioSegment.from_file(raw_bytes)  # ❌ Will fail or produce garbage
```

**Right:**
```python
# Wrap in WAV container first
wav_buffer = io.BytesIO()
with wave.open(wav_buffer, 'wb') as wav:
    wav.setnchannels(1)
    wav.setsampwidth(2)
    wav.setframerate(24000)
    wav.writeframes(raw_bytes)
wav_buffer.seek(0)
audio = AudioSegment.from_file(wav_buffer, format="wav")  # ✅
```

### ❌ Wrong Sample Rate/Channels

The default Gemini TTS output is:
- **24000 Hz** (not 16000, not 44100)
- **Mono** (1 channel, not stereo)
- **16-bit** (2 bytes per sample)

---

## Troubleshooting

### "Extra inputs are not permitted" Error

This means you're using inline requests with `generationConfig`. Use the JSONL file upload approach instead.

### Static/Noise in Audio

1. Ensure you're using the correct sample rate (24000)
2. Ensure you're treating data as 16-bit PCM
3. Wrap raw PCM in WAV container before further processing

### "No output file found"

The job may have failed silently. Check `job.state` and `job.error` for details.

### Empty Results

Check that your text content is valid and not empty. Very short texts may produce empty or very short audio.

---

## Rate Limits

Batch jobs have their own rate limits separate from real-time API calls:

- Tier 1: Up to 1,000 requests per batch
- Higher tiers available with increased quotas

Check the [Gemini API quotas](https://ai.google.dev/pricing) for current limits.

---

## See Also

- [Gemini API Documentation](https://ai.google.dev/docs)
- [Batch API Reference](https://ai.google.dev/api/batch-mode)
- [google-genai Python SDK](https://pypi.org/project/google-genai/)
