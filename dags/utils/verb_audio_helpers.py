
import struct

def get_system_prompt(language):
    """Returns the system instruction for the specific language."""
    if language.lower() == "french":
        return """Role: You are a professional French voice talent and language instructor.
Voice Profile:
Accent: Standard Metropolitan French (Parisian standard), clear and neutral.
Tone: Encouraging, educational, and articulate.
Pacing: Moderate. Ensure clear separation between the subject pronoun and the conjugated verb, but maintain natural flow.
Linguistic Rules:
Strict Liaisons: Always execute mandatory liaisons (e.g., vous avez should sound like "voo-z-avay").
Enunciation: Clearly pronounce terminal consonants that are not silent, and ensure the distinction between nasal vowels (e.g., an, en, in, on) is sharp.
Intonation: Use a slightly rising intonation at the end of clauses to indicate the sentence is continuing, with a definitive drop at the end of the full sentence.
E-muet: Minimize the "schwa" (the silent 'e') in casual speech patterns to sound modern, but keep it audible if it aids in learner comprehension of the verb stem.
Context: This audio is for a conjugation app. The focus is on the precision of the verb endings (e.g., the difference between -ais and -ait should be subtle but correct)."""
    
    elif language.lower() == "spanish":
         return """Role: You are a professional Spanish voice talent and language instructor.
Voice Profile:
Accent: Neutral Standard Spanish (Castilian/International), clear and articulate.
Tone: Encouraging, educational, and professional.
Pacing: Moderate. Ensure clear separation between the subject pronoun and the conjugated verb.
Linguistic Rules:
Pronunciation: Ensure clear distinction between vowels (a, e, i, o, u). Consonants should be crisp.
Intonation: Natural, with a standard declarative curve.
Context: This audio is for a conjugation app. Focus on precise pronunciation of verb endings."""

    elif language.lower() == "english":
        return """Role: You are a professional English voice talent and language instructor.
Voice Profile:
Accent: General American or Received Pronunciation (neutral), clear and articulate.
Tone: Encouraging, educational, and professional.
Pacing: Moderate.
Linguistic Rules:
Pronunciation: Clear enunciation of all syllables, particularly verb endings (s, ed, ing).
Context: This audio is for a conjugation app."""
    
    return "You are a professional voice talent. Pronounce the following sentence clearly."

def derive_subject(pronoun_label, language):
    """Maps database pronoun labels to subjects based on language."""
    label = pronoun_label.lower()
    lang = language.lower()

    if lang == "french":
        if "1" in label and "sing" in label: return "Je"
        if "2" in label and "sing" in label: return "Tu"
        if "3" in label and "sing" in label and "fem" in label: return "Elle"
        if "3" in label and "sing" in label: return "Il"
        
        if "1" in label and "plur" in label: return "Nous"
        if "2" in label and "plur" in label: return "Vous"
        if "3" in label and "plur" in label and "fem" in label: return "Elles"
        if "3" in label and "plur" in label: return "Ils"
    
        # Fallbacks for literal pronouns
        if label in ["je", "tu", "il", "elle", "on", "nous", "vous", "ils", "elles"]: return label.capitalize()

    elif lang == "spanish":
        if "1" in label and "sing" in label: return "Yo"
        if "2" in label and "sing" in label: return "Tú"
        if "3" in label and "sing" in label and "fem" in label: return "Ella"
        if "3" in label and "sing" in label: return "Él"
        
        if "1" in label and "plur" in label and "fem" in label: return "Nosotras"
        if "1" in label and "plur" in label: return "Nosotros"
        
        if "2" in label and "plur" in label and "fem" in label: return "Vosotras"
        if "2" in label and "plur" in label: return "Vosotros"
        
        if "3" in label and "plur" in label and "fem" in label: return "Ellas"
        if "3" in label and "plur" in label: return "Ellos"

        if label in ["yo", "tú", "él", "ella", "usted", "nosotros", "nosotras", "vosotros", "vosotras", "ellos", "ellas", "ustedes"]: return label.capitalize()

    elif lang == "english":
        if "1" in label and "sing" in label: return "I"
        if "2" in label and "sing" in label: return "You"
        if "3" in label and "sing" in label and "fem" in label: return "She"
        if "3" in label and "sing" in label: return "He" # or It
        
        if "1" in label and "plur" in label: return "We"
        if "2" in label and "plur" in label: return "You"
        if "3" in label and "plur" in label: return "They"
        
        if label in ["i", "you", "he", "she", "it", "we", "they"]: return label.capitalize()

    # Default fallback to the label itself if unknown
    return pronoun_label 

def get_pronunciation_note(pronoun_label, tense, form, language):
    """Returns specific pronunciation notes based on form and language."""
    label = pronoun_label.lower()
    lang = language.lower()

    if lang == "french":
        if "1" in label and "sing" in label:
            return 'Final "e" is silent (unless it is "ai" in future/imperfect, then pronounce carefully).'
        if "2" in label and "sing" in label:
            return 'Final "es" is silent.'
        if "3" in label and "sing" in label:
            return 'Final "e" is silent (matches 1st person usually).'
        if "1" in label and "plur" in label:
            return 'The "s" is silent; "ons" is nasal.'
        if "2" in label and "plur" in label:
            return 'The "ez" sounds like "ay".'
        if "3" in label and "plur" in label:
            return 'Crucial: The "ent" is silent (unless future tense "ont").'
    
    return ""

def convert_to_wav(audio_data: bytes, mime_type: str) -> bytes:
    """
    Generates a WAV file with proper header for the given raw PCM audio data.
    
    Args:
        audio_data: The raw PCM audio data as bytes
        mime_type: MIME type from Gemini API (e.g., "audio/L16;rate=24000")
    
    Returns:
        Complete WAV file as bytes with header
    """
    parameters = parse_audio_mime_type(mime_type)
    bits_per_sample = parameters["bits_per_sample"]
    sample_rate = parameters["rate"]
    num_channels = 1
    data_size = len(audio_data)
    bytes_per_sample = bits_per_sample // 8
    block_align = num_channels * bytes_per_sample
    byte_rate = sample_rate * block_align
    chunk_size = 36 + data_size  # 36 bytes for header fields before data chunk size

    # WAV file header structure: http://soundfile.sapp.org/doc/WaveFormat/
    header = struct.pack(
        "<4sI4s4sIHHIIHH4sI",
        b"RIFF",          # ChunkID
        chunk_size,       # ChunkSize (total file size - 8 bytes)
        b"WAVE",          # Format
        b"fmt ",          # Subchunk1ID
        16,               # Subchunk1Size (16 for PCM)
        1,                # AudioFormat (1 for PCM)
        num_channels,     # NumChannels
        sample_rate,      # SampleRate
        byte_rate,        # ByteRate
        block_align,      # BlockAlign
        bits_per_sample,  # BitsPerSample
        b"data",          # Subchunk2ID
        data_size         # Subchunk2Size (size of audio data)
    )
    return header + audio_data


def parse_audio_mime_type(mime_type: str) -> dict:
    """
    Parses bits per sample and sample rate from an audio MIME type string.
    
    Args:
        mime_type: The audio MIME type string (e.g., "audio/L16;rate=24000")
    
    Returns:
        Dictionary with "bits_per_sample" and "rate" keys as integers
    """
    bits_per_sample = 16  # Default
    rate = 24000  # Default

    # Extract rate from parameters
    parts = mime_type.split(";")
    for param in parts:
        param = param.strip()
        if param.lower().startswith("rate="):
            try:
                rate_str = param.split("=", 1)[1]
                rate = int(rate_str)
            except (ValueError, IndexError):
                pass  # Keep default
        elif param.startswith("audio/L"):
            try:
                bits_per_sample = int(param.split("L", 1)[1])
            except (ValueError, IndexError):
                pass  # Keep default

    return {"bits_per_sample": bits_per_sample, "rate": rate}
