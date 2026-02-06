
import os
from google import genai

try:
    client = genai.Client(api_key="TEST_KEY")
    print("Methods of client.batches:")
    print(dir(client.batches))
except Exception as e:
    print(f"Error: {e}")
