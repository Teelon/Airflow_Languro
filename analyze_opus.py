
import os
from pydub import AudioSegment

file_path = r"d:\Gemini Hackathon\conjugation_en_to_come_conditional_perfect_he_she_it_would_have_come.opus"

try:
    print(f"Analyzing: {file_path}")
    if not os.path.exists(file_path):
        print("File not found!")
    else:
        # Load the file
        audio = AudioSegment.from_file(file_path)
        
        print(f"--- Analysis Results ---")
        print(f"Duration: {audio.duration_seconds:.4f} seconds")
        print(f"dBFS (Loudness): {audio.dBFS:.2f} dB")
        print(f"Peak Amplitude: {audio.max_possible_amplitude}")
        print(f"Frame Rate: {audio.frame_rate}")
        print(f"Channels: {audio.channels}")
        
        # Check against our validation logic
        valid = True
        if audio.duration_seconds < 0.1:
            print("[FAIL] Too short (< 0.1s)")
            valid = False
        if audio.duration_seconds > 30:
            print("[FAIL] Too long (> 30s)")
            valid = False
        if audio.dBFS == float('-inf'):
            print("[FAIL] Absolute silence")
            valid = False
        elif audio.dBFS < -60:
            print("[WARN] Extremely quiet (< -60 dB)")
            # We don't fail on this in strict mode currently, just warn/pass
            
        if valid:
            print("[PASS] Current validation would ACCEPT this file.")
        else:
            print("[REJECT] Current validation would BLOCK this file.")
            
except Exception as e:
    print(f"Error analyzing file: {e}")
