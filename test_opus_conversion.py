import os
from google import genai
from google.genai import types
from pydub import AudioSegment
import io
import sys

# Add the current directory to sys.path to ensure imports work
sys.path.append(os.getcwd())

try:
    from dags.utils.verb_audio_helpers import convert_to_wav, parse_audio_mime_type
except ImportError:
    # Try local import if running from dags folder
    sys.path.append(os.path.join(os.getcwd(), 'dags'))
    from utils.verb_audio_helpers import convert_to_wav, parse_audio_mime_type

client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))

print("Generating audio...")
# Test with a simple phrase
audio_chunks = []
for chunk in client.models.generate_content_stream(
    model="gemini-2.5-pro-preview-tts",
    contents=[types.Content(role="user", parts=[types.Part.from_text(text="Yo como.")])],
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
    if chunk.candidates and chunk.candidates[0].content and chunk.candidates[0].content.parts:
        for part in chunk.candidates[0].content.parts:
            if part.inline_data and part.inline_data.data:
                audio_chunks.append({'data': part.inline_data.data, 'mime_type': part.inline_data.mime_type})

if not audio_chunks:
    print("Error: No audio generated.")
    sys.exit(1)

# Convert to WAV then Opus
print(f"Received {len(audio_chunks)} chunks.")
raw_audio = b''.join([chunk['data'] for chunk in audio_chunks])
mime_type = audio_chunks[0]['mime_type'] if audio_chunks else "audio/L16;rate=24000"

print(f"MIME Type: {mime_type}")
wav_data = convert_to_wav(raw_audio, mime_type)

print("Converting to Opus...")
audio = AudioSegment.from_wav(io.BytesIO(wav_data))
audio.export("test_output.opus", format="opus", codec="libopus", bitrate="64k")

print(f"✓ Test file created: test_output.opus")
print(f"✓ Size: {os.path.getsize('test_output.opus')} bytes")
