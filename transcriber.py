# transcriber.py
import asyncio
import json
import os
import subprocess
import tempfile
import wave
import struct
import sys

from openai import AsyncOpenAI

# Initialize the async client, required for parallel API calls
# Expects the OPENAI_API_KEY environment variable to be set
try:
    async_client = AsyncOpenAI()
except Exception as e:
    print("Failed to initialize OpenAI client. Ensure OPENAI_API_KEY is set.")
    async_client = None

async def split_channels_ffmpeg(source_path: str) -> (str, str):
    """
    Splits a stereo audio file into two mono temporary files using ffmpeg.
    Returns the file paths of the left and right channels.
    """
    print(f"[FFMPEG] Splitting channels for {source_path}")
    
    # Create temporary files for the mono channels
    left_channel_path = tempfile.NamedTemporaryFile(delete=False, suffix="_left.wav").name
    right_channel_path = tempfile.NamedTemporaryFile(delete=False, suffix="_right.wav").name

    # Construct the ffmpeg command
    # -i: input file
    # -filter_complex "[0:a]channelsplit=channel_layout=stereo[left][right]": The core logic to split channels
    # -map "[left]" / -map "[right]": Map the created streams to output files
    # -y: Overwrite output files without asking
    command = [
        "ffmpeg",
        "-i", source_path,
        "-filter_complex", "[0:a]channelsplit=channel_layout=stereo[left][right]",
        "-map", "[left]", left_channel_path,
        "-map", "[right]", right_channel_path,
        "-y"
    ]

    # Run the command asynchronously
    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    stdout, stderr = await process.communicate()

    if process.returncode != 0:
        # Cleanup failed temporary files
        os.remove(left_channel_path)
        os.remove(right_channel_path)
        raise RuntimeError(f"FFMPEG Error: {stderr.decode()}")
        
    print(f"[FFMPEG] Split successful. Left: {left_channel_path}, Right: {right_channel_path}")
    return left_channel_path, right_channel_path


async def transcribe_channel(file_path: str, channel_id: int) -> list:
    """
    Transcribes a single mono audio file and returns a list of utterance segments.
    """
    if not async_client:
        raise ConnectionError("OpenAI client not available.")
        
    print(f"[Transcriber] Starting transcription for channel {channel_id} ({file_path})")
    try:
        with open(file_path, "rb") as f:
            transcript = await async_client.audio.transcriptions.create(
                # Using "whisper-1" is standard. gpt-4o-mini-transcribe is not a valid model name.
                model="whisper-1",
                file=f,
                response_format="verbose_json"
            )

        utterances = [
            {
                "channel": channel_id,
                "timestamp": seg.start,
                "utterance": seg.text.strip()
            }
            for seg in transcript.segments
        ]
        print(f"[Transcriber] Finished channel {channel_id}.")
        return utterances
    except Exception as e:
        print(f"[Transcriber] FAILED for channel {channel_id}: {e}")
        # Return an empty list on failure so one failed channel doesn't block the other
        return []

async def run_transcription(file_path: str) -> dict:
    """
    Orchestrates the stereo transcription process.
    1. Splits stereo file into two mono files.
    2. Transcribes both mono files in parallel.
    3. Collates and sorts the results.
    4. Cleans up temporary files.
    """
    if not os.path.exists(file_path):
        return {"error": "File not found"}

    temp_files = []
    try:
        # 1. Split channels using ffmpeg
        left_path, right_path = await split_channels_ffmpeg(file_path)
        temp_files.extend([left_path, right_path])

        # 2. Create and run transcription tasks in parallel
        tasks = [
            transcribe_channel(left_path, 0),  # Channel 0 (left)
            transcribe_channel(right_path, 1) # Channel 1 (right)
        ]
        results = await asyncio.gather(*tasks)

        # 3. Collate and sort all utterances by timestamp
        all_utterances = [utterance for channel_result in results for utterance in channel_result]
        all_utterances.sort(key=lambda x: x["timestamp"])

        print("[Main] Transcription complete and collated.")
        return {"utterances": all_utterances}

    except Exception as e:
        print(f"[Main] An error occurred: {e}")
        return {"error": str(e), "utterances": []}
    finally:
        # 4. Clean up all temporary files
        print("[Main] Cleaning up temporary files...")
        for path in temp_files:
            if os.path.exists(path):
                os.remove(path)
        print("[Main] Cleanup complete.")


# --- Self-Contained Test ---
def create_dummy_stereo_wav(filename="dummy_stereo.wav"):
    """Generates a simple stereo WAV file for testing purposes."""
    sample_rate = 44100
    duration = 2  # seconds
    n_samples = int(duration * sample_rate)
    
    with wave.open(filename, 'w') as wf:
        wf.setnchannels(2)      # Stereo
        wf.setsampwidth(2)      # 16-bit
        wf.setframerate(sample_rate)
        
        for i in range(n_samples):
            # Left channel: A low-frequency tone
            left_val = int(32767.0 * 0.5 * (i / n_samples))
            # Right channel: A high-frequency tone
            right_val = int(32767.0 * 0.5 * (1 - (i / n_samples)))
            
            # Pack as little-endian signed short
            wf.writeframes(struct.pack('<h', left_val))
            wf.writeframes(struct.pack('<h', right_val))
    print(f"Created dummy stereo file: {filename}")
    return filename

async def main(recording_path):
    """Main function to run the test."""
    print("--- Running Transcription Test ---")
    if not async_client:
        print("Exiting test because OpenAI client is not conigured.")
        return
        
    #dummy_file = create_dummy_stereo_wav()
    result = await run_transcription(recording_path)
    
    print("\n--- FINAL TRANSCRIPT ---")
    print(json.dumps(result, indent=2))
    
    # Clean up the dummy file
    #os.remove(dummy_file)

if __name__ == "__main__":
    # To run this test:
    # 1. Ensure you have ffmpeg installed and in your system's PATH.
    # 2. Run `pip install openai`
    # 3. Set your OpenAI API key: `export OPENAI_API_KEY='sk-...'`
    # 4. Run the script: `python transcriber.py`
    asyncio.run(main(sys.argv[1]))
