from flask import Flask, Response, send_file, abort, request, render_template_string, redirect, url_for
import subprocess
import threading
import time
import sqlite3
from pathlib import Path
import os
from collections import Counter, deque
from queue import Queue, Empty
import shutil
import logging
import json
import requests
import re
from datetime import datetime

# --- Configuration ---
PORT = 9090
FFMPEG = "ffmpeg"
FFMPEG_PROBE = "ffprobe"
DB_PATH = r"C:\\YourPath\\vod.db" #Path to your DB
CHANNELS_DVR_URL = "http://123.456.789.012:8089/dvr" #Path to your Channels DVR server

QUALITY_PRIORITY = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20"]
SOURCE_PRIORITY = ["Source1", "Source2"] #List your sources here
SOURCE_LIMITS = {"Source1": 5, "Source2": 4} #List how many concurent streams for each source here

# Transcoding configuration - always transcode for consistency
TARGET_VIDEO_BITRATE = "5M"
TARGET_AUDIO_BITRATE = "128k"

# Buffering configuration in seconds
BUFFER_CHECK_INTERVAL = 5
BUFFER_THRESHOLD = 3
STREAM_START_TIMEOUT = 15  # Reduced since we serve playlist faster
INACTIVITY_TIMEOUT = 30
FFPROBE_TIMEOUT = 10  # Timeout for FFprobe validation in seconds

VERBOSE = True

app = Flask(__name__)

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

log_buffer = deque(maxlen=500)

class UILogHandler(logging.Handler):
    def emit(self, record):
        msg = self.format(record)
        log_buffer.append(msg)

ui_log_handler = UILogHandler()
logger.addHandler(ui_log_handler)

# --- Global State for live streams ---
streams = {}
source_usage = Counter()
stream_attempt_cache = {}
stream_start_lock = threading.Lock()
last_request_time = {}

channel_locks = {}
cleanup_lock = threading.Lock()
last_dvr_check_time = 0

all_streams_from_db = {}

def is_ffmpeg_available():
    return shutil.which(FFMPEG) is not None

def is_ffprobe_available():
    return shutil.which(FFMPEG_PROBE) is not None

def validate_stream_with_ffprobe(stream_url, stream_name, source):
    """
    Uses FFprobe to validate if a stream is accessible and contains valid video/audio streams.
    Returns True if the stream is valid, False otherwise.
    """
    logger.info(f"Validating stream with FFprobe: {stream_name} [{source}]")
    
    probe_cmd = [
        FFMPEG_PROBE,
        "-v", "error",
        "-show_entries", "stream=codec_type",
        "-of", "json",
        "-probesize", "5M",
        "-analyzeduration", "5M",
        stream_url
    ]
    
    try:
        result = subprocess.run(
            probe_cmd,
            capture_output=True,
            text=True,
            timeout=FFPROBE_TIMEOUT
        )
        
        if result.returncode != 0:
            logger.warning(f"FFprobe failed for {stream_name} [{source}]: {result.stderr[:200]}")
            return False
        
        # Parse the JSON output
        try:
            probe_data = json.loads(result.stdout)
            streams = probe_data.get("streams", [])
            
            if not streams:
                logger.warning(f"No streams found in {stream_name} [{source}]")
                return False
            
            # Check if we have at least video or audio
            has_video = any(s.get("codec_type") == "video" for s in streams)
            has_audio = any(s.get("codec_type") == "audio" for s in streams)
            
            if has_video or has_audio:
                stream_types = []
                if has_video:
                    stream_types.append("video")
                if has_audio:
                    stream_types.append("audio")
                logger.info(f"Stream validated successfully: {stream_name} [{source}] - Contains: {', '.join(stream_types)}")
                return True
            else:
                logger.warning(f"Stream has no valid video or audio: {stream_name} [{source}]")
                return False
                
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse FFprobe JSON output for {stream_name} [{source}]: {e}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.warning(f"FFprobe timeout ({FFPROBE_TIMEOUT}s) for {stream_name} [{source}]")
        return False
    except Exception as e:
        logger.error(f"Error running FFprobe for {stream_name} [{source}]: {e}")
        return False

def load_streams_from_db():
    """
    Loads all stream configurations from the SQLite database into a global cache.
    This function is now called on-demand to refresh the cache.
    """
    global all_streams_from_db
    all_streams_from_db = {}
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        
        rows = conn.execute("SELECT * FROM LiveStreams WHERE M3U IS NOT NULL AND M3U != ''").fetchall()
        conn.close()
        
        valid_streams_count = 0
        for row in rows:
            cid_raw = row["Channel-id"]
            if cid_raw is None:
                logger.warning(f"Skipping a record with a null Channel-id. Row data: {dict(row)}")
                continue

            try:
                cid = int(cid_raw)
                all_streams_from_db.setdefault(cid, []).append(dict(row))
                valid_streams_count += 1
            except (ValueError, TypeError) as e:
                logger.error(f"Could not convert Channel-id '{cid_raw}' to integer. Skipping record. Error: {e}. Row data: {dict(row)}")

        logger.info(f"Loaded {valid_streams_count} valid stream configs across {len(all_streams_from_db)} unique channels from '{DB_PATH}'.")
    except Exception as e:
        logger.error(f"Error loading streams from database: {e}")
        all_streams_from_db = {}

def get_unique_m3u_names():
    """Retrieves all unique M3U base names (without the .m3u extension) from the database."""
    unique_m3u_files = set()
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute("SELECT DISTINCT M3U FROM LiveStreams WHERE M3U IS NOT NULL AND M3U != ''").fetchall()
        conn.close()
        for row in rows:
            m3u_file_full = row[0]
            if m3u_file_full and m3u_file_full.lower().endswith(".m3u"):
                base_name = m3u_file_full[:-4] 
                unique_m3u_files.add(base_name)
    except Exception as e:
        logger.error(f"Error retrieving unique M3U names from database: {e}")
    
    return sorted(list(unique_m3u_files))

def update_stream_status(stream_url, status, is_success=None):
    """
    Updates the Stream-status, Last-checked-time, and success/failure counters
    for a given stream URL.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        current_time_str = datetime.now().isoformat()
        
        if is_success is not None:
            column_to_increment = "Uptime" if is_success else "Downtime"
            cursor.execute(
                f"""
                UPDATE LiveStreams
                SET "Stream-status" = ?, "Last-checked-time" = ?, "{column_to_increment}" = "{column_to_increment}" + 1
                WHERE "stream-url" = ?
                """,
                (status, current_time_str, stream_url)
            )
        else:
            cursor.execute(
                """
                UPDATE LiveStreams
                SET "Stream-status" = ?, "Last-checked-time" = ?
                WHERE "stream-url" = ?
                """,
                (status, current_time_str, stream_url)
            )

        conn.commit()
        conn.close()
        logger.debug(f"DB updated for {stream_url}: status='{status}', time='{current_time_str}'")
    except Exception as e:
        logger.error(f"Error updating database for stream {stream_url}: {e}")
        
def pick_stream_candidates(all_candidates_for_cid, exclude_url=None):
    candidates = []
    for s in all_candidates_for_cid:
        if s.get("stream-url") == exclude_url:
            if VERBOSE:
                logger.debug(f"Skipping excluded candidate {s.get('Stream-name')} from {s.get('Source')}.")
            continue

        source = s["Source"]
        if source in SOURCE_LIMITS and source_usage.get(source, 0) >= SOURCE_LIMITS.get(source, float('inf')):
            if VERBOSE:
                logger.debug(f"Skipping candidate {s.get('Stream-name')} from {source} due to limit.")
            continue
        candidates.append(s)
    
    candidates.sort(key=lambda s: (
        SOURCE_PRIORITY.index(s["Source"]) if s["Source"] in SOURCE_PRIORITY else 999,
        QUALITY_PRIORITY.index(s["Stream-quality"]) if s["Stream-quality"] in QUALITY_PRIORITY else 999
    ))
    return candidates

def clean_output_dir(out_dir):
    max_attempts = 5
    delay_between_attempts = 0.5

    for attempt in range(max_attempts):
        if not out_dir.exists():
            break
        
        try:
            if not out_dir.exists():
                break

            for f in out_dir.iterdir(): 
                try:
                    if f.is_file():
                        f.unlink()
                    elif f.is_dir(): 
                        shutil.rmtree(f)
                except OSError as e:
                    if VERBOSE:
                        logger.debug(f"Attempt {attempt+1}: Error unlinking/removing {f}: {e}")
            
            out_dir.rmdir()
            if VERBOSE:
                logger.info(f"Successfully cleaned directory {out_dir} on attempt {attempt+1}.")
            break
        except OSError as e:
            if VERBOSE:
                logger.debug(f"Attempt {attempt+1}: Error removing directory {out_dir}: {e}")
            time.sleep(delay_between_attempts)
    else:
        logger.warning(f"Failed to clean directory {out_dir} after {max_attempts} attempts.")
    
    out_dir.mkdir(parents=True, exist_ok=True)

def enqueue_output(out, queue, cid):
    prefix = f"[ffmpeg:{cid}]"
    for line in iter(out.readline, ''):
        queue.put(line)
        if VERBOSE:
            logger.debug(f"{prefix} {line.strip()}")
    out.close()

def monitor_ffmpeg_output(proc, output_queue):
    """Monitors FFmpeg output for buffering/stalling messages and sets a flag."""
    buffering_count = 0
    last_log_time = time.time()
    while proc.poll() is None:
        try:
            line = output_queue.get(timeout=BUFFER_CHECK_INTERVAL)
            if "stalling" in line.lower() or "slow" in line.lower():
                buffering_count += 1
                logger.warning(f"Channel {proc.channel_id} detected as buffering. Count: {buffering_count}")
                if buffering_count >= BUFFER_THRESHOLD:
                    logger.error(f"Channel {proc.channel_id} has been buffering for too long. Triggering stream restart.")
                    proc.is_buffering = True
            else:
                buffering_count = 0
            
            last_log_time = time.time()
        except Empty:
            if time.time() - last_log_time > BUFFER_CHECK_INTERVAL * 2:
                logger.warning(f"FFmpeg process for channel {proc.channel_id} seems to have stalled. No log activity for {BUFFER_CHECK_INTERVAL * 2}s.")
                buffering_count += 1
                if buffering_count >= BUFFER_THRESHOLD:
                    logger.error(f"Channel {proc.channel_id} log stream stalled. Triggering stream restart.")
                    proc.is_buffering = True

def ensure_stream_running(cid, exclude_url=None):
    if cid not in channel_locks:
        channel_locks[cid] = threading.Lock()
    
    with channel_locks[cid]:
        out_dir = Path(f"./output/channel_{cid}")
        out_file = out_dir / "stream.m3u8"
        
        # Check if a stream is already running for this channel.
        if cid in streams and streams[cid].poll() is None:
            logger.debug(f"Stream for channel {cid} is already running.")
            return

        # Cleanup any dead processes or old files.
        if cid in streams and streams[cid].poll() is not None:
            logger.info(f"Cleaning up dead stream for channel {cid}")
            proc_to_clean = streams.pop(cid, None)
            if proc_to_clean:
                source = getattr(proc_to_clean, "source_tag", None)
                if source and source_usage[source] > 0:
                    source_usage[source] -= 1
                try:
                    proc_to_clean.kill()
                    proc_to_clean.wait(timeout=2)
                except Exception as e:
                    if VERBOSE:
                        logger.debug(f"Error killing dead process for {cid}: {e}")
            clean_output_dir(out_dir)
        else:
            clean_output_dir(out_dir)

        logger.info(f"No active stream for {cid}. Searching for a live source and starting immediately...")

        # Find and start a live stream.
        if cid not in stream_attempt_cache or not stream_attempt_cache[cid]:
            all_channel_streams = all_streams_from_db.get(cid, [])
            logger.info(f"Preparing candidates for channel {cid} ({len(all_channel_streams)} total configs)...")
            stream_attempt_cache[cid] = pick_stream_candidates(all_channel_streams, exclude_url)

        current_candidates = list(stream_attempt_cache[cid])
        proc_live = None
        
        while current_candidates:
            c = current_candidates.pop(0)
            stream_url = c["stream-url"]
            source = c["Source"] or "Unknown"
            stream_name = c["Stream-name"] or f"Channel {cid}"
            iso3_value = c.get("ISO3", "UK")
            uniform_name = c.get("Uniform-name", stream_name)

            if source in SOURCE_LIMITS and source_usage.get(source, 0) >= SOURCE_LIMITS.get(source, float('inf')):
                logger.info(f"Skipping {stream_name} [{source}] for {cid}: Source limit reached for this attempt.")
                if c in stream_attempt_cache[cid]:
                    stream_attempt_cache[cid].remove(c)
                continue

            logger.info(f"Trying: {iso3_value}: {stream_name} [{source}]")
            
            # Validate stream with FFprobe first
            if not validate_stream_with_ffprobe(stream_url, stream_name, source):
                logger.warning(f"Stream validation failed for {stream_name} [{source}]. Trying next candidate...")
                update_stream_status(stream_url, "Invalid", is_success=False)
                if c in stream_attempt_cache[cid]:
                    stream_attempt_cache[cid].remove(c)
                continue
            
            is_mpd_stream = stream_url.lower().endswith('.mpd')

            base_ffmpeg_args = [
                FFMPEG, "-y", "-v", "info",
                "-probesize", "10M", "-analyzeduration", "20M",
            ]
            
            if not is_mpd_stream:
                base_ffmpeg_args.extend(["-max_interleave_delta", "0"])

            ffmpeg_args = base_ffmpeg_args + [
                "-i", stream_url,
                "-c", "copy",
                "-f", "hls",
                "-hls_time", "6",
                "-hls_list_size", "3",
                "-hls_flags", "delete_segments",
                "-hls_start_number_source", "epoch",
                "-hls_segment_type", "mpegts",
                str(out_file)
            ]
            logger.info(f"Starting direct stream copy (remux) to HLS for {stream_name} [{source}]")

            cmd = ffmpeg_args

            try:
                proc_live = subprocess.Popen(cmd, stderr=subprocess.PIPE, universal_newlines=True)
                proc_live.source_tag = source
                proc_live.channel_id = cid
                proc_live.stream_data = c
                proc_live.is_buffering = False
                proc_live.source_slot_index = source_usage.get(source, 0)
                proc_live.start_time = time.time()
                proc_live.terminated_by_proxy = False

                q = Queue()
                t = threading.Thread(target=enqueue_output, args=(proc_live.stderr, q, cid))
                t.daemon = True
                t.start()
                
                monitor_thread = threading.Thread(target=monitor_ffmpeg_output, args=(proc_live, q))
                monitor_thread.daemon = True
                monitor_thread.start()

                # Monitor FFmpeg process for startup success or failure
                stream_started = False
                start_time = time.time()
                max_wait_time = STREAM_START_TIMEOUT
                
                while time.time() - start_time < max_wait_time:
                    if proc_live.poll() is not None:
                        logger.error(f"FFmpeg process for channel {cid} exited prematurely with code {proc_live.poll()}.")
                        break
                    
                    # Better startup detection by checking playlist content
                    if out_file.exists():
                        try:
                            with open(out_file, 'r') as f:
                                playlist_content = f.read()
                                if '.ts' in playlist_content:
                                    stream_started = True
                                    break
                        except Exception as file_e:
                            logger.debug(f"Error reading playlist file {out_file}: {file_e}")
                            
                    time.sleep(0.1)
                
                if stream_started:
                    streams[cid] = proc_live
                    source_usage[source] = source_usage.get(source, 0) + 1
                    logger.info(f"Started stream for channel {cid} using '{stream_name}' [{source}]")
                    logger.info(f"Active streams: {len(streams)} | Usage: {dict(source_usage)}")
                    
                    update_stream_status(stream_url, "Working", is_success=True)

                    stream_attempt_cache.pop(cid, None)
                    return
                else:
                    logger.warning(f"Failed to start stream for {cid} after {max_wait_time}s. FFmpeg process status: {proc_live.poll()}. Playlist segments not found. Trying next candidate...")
                    
                    update_stream_status(stream_url, "Failed", is_success=False)

                    if proc_live and proc_live.poll() is None:
                        logger.info(f"Killing failed FFmpeg process for {stream_name} [{source}]")
                        proc_live.terminated_by_proxy = True
                        proc_live.kill()
                        proc_live.wait(timeout=2)
                    clean_output_dir(out_dir)
                    if c in stream_attempt_cache[cid]:
                        stream_attempt_cache[cid].remove(c)
                    time.sleep(1)

            except Exception as e:
                logger.error(f"Error starting stream for {cid}: {e}")
                
                update_stream_status(stream_url, "Failed", is_success=False)

                if proc_live and proc_live.poll() is None:
                    logger.info(f"Killing FFmpeg process due to exception for {cid}")
                    proc_live.terminated_by_proxy = True
                    proc_live.kill()
                    proc_live.wait(timeout=2)
                clean_output_dir(out_dir)
                if c in stream_attempt_cache[cid]:
                    stream_attempt_cache[cid].remove(c)

def check_and_cleanup_streams():
    """
    Checks the Channels DVR API and terminates any inactive streams.
    This function is now called from both a background thread and on new requests.
    """
    with cleanup_lock:
        try:
            logger.info("Channels DVR API check initiated...")
            response = requests.get(CHANNELS_DVR_URL, timeout=10)
            response.raise_for_status()
            dvr_status = response.json()
            
            dvr_active_channels_now = set()
            activity = dvr_status.get("activity", {})
            
            if activity:
                ch_pattern = re.compile(r"(?:Watching|Recording) ch(\d+)", re.IGNORECASE)
                for activity_message in activity.values():
                    match = ch_pattern.search(activity_message)
                    if match:
                        dvr_active_channels_now.add(int(match.group(1)))
                logger.info(f"Channels DVR reports these channels are active: {list(dvr_active_channels_now)}")
            else:
                logger.info("Channels DVR reports no active streams.")

            proxy_active_channels = set(streams.keys())
            logger.info(f"Proxy is currently managing streams for: {list(proxy_active_channels)}")

            streams_to_kill = set()
            for cid in proxy_active_channels:
                is_dvr_active = cid in dvr_active_channels_now
                last_request_delta = time.time() - last_request_time.get(cid, 0) if cid in last_request_time else float('inf')
                
                if streams[cid].poll() is not None:
                    logger.warning(f"FFmpeg process for channel {cid} has terminated with exit code {streams[cid].poll()}. Cleaning up.")
                    streams_to_kill.add(cid)
                    continue

                if not is_dvr_active and last_request_delta > INACTIVITY_TIMEOUT: 
                    streams_to_kill.add(cid)
                    logger.info(f"Stream for channel {cid} will be terminated. DVR reports it as inactive and no requests in the last {last_request_delta:.2f}s.")
                elif not is_dvr_active and last_request_delta <= INACTIVITY_TIMEOUT:
                    logger.warning(f"Stream for channel {cid} is not reported by DVR, but has recent activity (last request {last_request_delta:.2f}s ago). Skipping termination.")
            
            if streams_to_kill:
                for cid in list(streams_to_kill):
                    if cid not in channel_locks:
                        channel_locks[cid] = threading.Lock()
                    
                    with channel_locks[cid]:
                        proc = streams.pop(cid, None)
                        if proc:
                            if hasattr(proc, "source_tag") and source_usage[proc.source_tag] > 0:
                                source_usage[proc.source_tag] -= 1
                            if proc.poll() is None:
                                try:
                                    proc.terminated_by_proxy = True
                                    proc.kill()
                                    proc.wait(timeout=5)
                                    logger.warning(f"Terminating stream for channel {cid} due to inactivity and no Channels DVR report.")
                                except Exception as e:
                                    if VERBOSE:
                                        logger.debug(f"Error killing/waiting for timed-out process {cid}: {e}")
                                    pass
                            clean_output_dir(Path(f"./output/channel_{cid}"))
                        stream_attempt_cache.pop(cid, None)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error polling Channels DVR at {CHANNELS_DVR_URL}: {e}")
        

def cleanup_timer_thread():
    DVR_POLL_INTERVAL = 30
    while True:
        check_and_cleanup_streams()
        time.sleep(DVR_POLL_INTERVAL)

def get_m3u_channels(m3u_filename):
    selected_channels = {}
    logger.debug(f"get_m3u_channels called for filename: '{m3u_filename}'")
    logger.debug(f"all_streams_from_db contains {len(all_streams_from_db)} unique channel IDs.")

    for cid, candidates_for_cid in all_streams_from_db.items():
        m3u_specific_candidates = []
        for s in candidates_for_cid:
            if s["M3U"] == f"{m3u_filename}.m3u":
                m3u_specific_candidates.append(s)

        if not m3u_specific_candidates:
            if VERBOSE:
                logger.debug(f"No candidates for CID {cid} found for M3U '{m3u_filename}'.")
            continue

        m3u_specific_candidates.sort(key=lambda s: (
            SOURCE_PRIORITY.index(s.get("Source", "UKN")) if s.get("Source", "UKN") in SOURCE_PRIORITY else 999,
            QUALITY_PRIORITY.index(s.get("Stream-quality", "UKN")) if s.get("Stream-quality", "UKN") in QUALITY_PRIORITY else 999
        ))

        best_candidate = m3u_specific_candidates[0]
        selected_channels[cid] = best_candidate
        if VERBOSE:
            logger.debug(f"Selected best candidate for CID {cid}: {best_candidate.get('Stream-name')} from {best_candidate.get('Source')}")

    sorted_channels_for_m3u = sorted(selected_channels.values(), key=lambda x: x["Channel-id"])
    logger.debug(f"get_m3u_channels returning {len(sorted_channels_for_m3u)} channels for M3U '{m3u_filename}'.")
    return sorted_channels_for_m3u

@app.route("/channel/<int:cid>/<path:filename>")
def serve_channel_segment(cid, filename):
    
    out_dir = Path(f"./output/channel_{cid}")
    out_path = out_dir / filename

    # Update the last request time for this channel to prevent premature cleanup
    last_request_time[cid] = time.time()
    
    # Check if the stream is actively buffering and needs a restart
    if cid in streams and hasattr(streams[cid], "is_buffering") and streams[cid].is_buffering:
        logger.warning(f"Buffering detected for channel {cid}. Restarting stream.")
        streams[cid].is_buffering = False
        threading.Thread(target=ensure_stream_running, args=(cid,)).start()
        return abort(503, "Stream buffering, attempting to restart...")

    # Use a lock to prevent race conditions on playlist requests
    if cid not in channel_locks:
        channel_locks[cid] = threading.Lock()

    if filename.endswith(".m3u8"):
        logger.debug(f"Handling playlist request for channel {cid}...")
        
        # Start stream in background if needed, but don't wait long
        if cid not in streams or streams[cid].poll() is not None:
            logger.info(f"Starting background stream launch for channel {cid}")
            threading.Thread(target=ensure_stream_running, args=(cid,)).start()
        
        # Wait briefly for playlist file to exist (max 3 seconds)
        start_time = time.time()
        while time.time() - start_time < 3:
            if out_path.exists():
                break
            time.sleep(0.1)
        
        # Serve the playlist even if it's empty/incomplete
        try:
            if out_path.exists():
                logger.info(f"Serving playlist for channel {cid}.")
                def generate_playlist():
                    with open(out_path, "r") as f:
                        yield f.read()
                return Response(generate_playlist(), mimetype='application/x-mpegurl')
            else:
                # Return minimal valid playlist to keep client connected
                logger.info(f"Serving minimal playlist for channel {cid} while stream starts.")
                minimal_playlist = "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:6\n"
                return Response(minimal_playlist, mimetype='application/x-mpegurl')
        except Exception as e:
            logger.error(f"Error serving playlist: {e}")
            minimal_playlist = "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:6\n"
            return Response(minimal_playlist, mimetype='application/x-mpegurl')
                
    # If the request is for a TS segment
    elif filename.endswith(".ts"):
        out_path = out_dir / filename
        logger.debug(f"Handling TS file request for channel {cid}: {filename}")

        if not out_path.exists():
            logger.warning(f"TS file not found: {out_path}")
            return abort(404)

    try:
        def generate():
            with open(out_path, "rb") as f:
                while True:
                    chunk = f.read(4096)
                    if not chunk:
                        break
                    yield chunk
        
        mimetype = "application/x-mpegURL" if filename.endswith(".m3u8") else "video/mp2t"
        return Response(generate(), mimetype=mimetype)
    except ConnectionAbortedError:
        logger.info(f"Client aborted request: /channel/{cid}/{filename}")
        return ("", 200)
    except Exception as e:
        logger.error(f"Error serving /channel/{cid}/{filename}: {e}")
        return abort(500)

@app.route("/m3u/<m3u_filename>.m3u")
def generate_m3u(m3u_filename):
    """
    Generates and serves an M3U playlist file based on the specified filename
    from the 'M3U' column in the database. The cache is refreshed on each request.
    """
    load_streams_from_db()
    
    m3u_content = ["#EXTM3U"]
    
    channels_for_m3u = get_m3u_channels(m3u_filename)

    is_dvr_format = m3u_filename.lower().startswith("dvr")

    for channel in channels_for_m3u:
        cid = channel["Channel-id"]
        tv_logo = channel.get("TV-Logo", "")
        group_title = channel.get("Group-title", "Uncategorized")
        uniform_name = channel.get("Uniform-name", channel.get("Stream-name", f"Channel {cid}"))
        
        local_stream_url = f"http://{request.host}/channel/{cid}/stream.m3u8"

        if is_dvr_format:
            tvg_id = channel.get("Channel-id", "")
            tvg_chno = channel.get("Channel-id", "")
            tvc_guide_stationid = channel.get("TV-guide-id", "")
            tvg_name = channel.get("TV-guide-name", uniform_name)

            extinf_line = (
                f'#EXTINF:-1 channel-id="{cid}" tvg-id="{tvg_id}" tvg-chno="{tvg_chno}" '
                f'tvg-logo="{tv_logo}" tvc-guide-stationid="{tvc_guide_stationid}" '
                f'tvg-name="{tvg_name}" group-title="{group_title}",{uniform_name}'
            )
        else:
            tvg_id = channel.get("TV-guide-name", "")
            tvg_chno = channel.get("Channel-id", "")
            tvc_guide_stationid = channel.get("TV-guide-name", "")
            tvg_name = channel.get("TV-guide-name", uniform_name)

            extinf_line = (
                f'#EXTINF:-1 channel-id="{cid}" tvg-id="{tvg_id}" tvg-chno="{tvg_chno}" '
                f'tvg-logo="{tv_logo}" tvc-guide-stationid="{tvc_guide_stationid}" '
                f'tvg-name="{tvg_name}" group-title="{group_title}",{uniform_name}'
            )
        
        m3u_content.append(extinf_line)
        m3u_content.append(local_stream_url)

    return Response("\n".join(m3u_content), mimetype='application/x-mpegurl')


@app.route("/force_switch/<int:cid>")
def force_switch(cid):
    """
    Forces a stream to stop and attempts to restart it with the next available candidate.
    """
    if cid not in channel_locks:
        channel_locks[cid] = threading.Lock()

    with channel_locks[cid]:
        logger.warning(f"Force switch requested for channel {cid}.")
        
        proc_to_kill = streams.pop(cid, None)
        terminated_stream_url = None
        if proc_to_kill and proc_to_kill.poll() is None:
            source = getattr(proc_to_kill, "source_tag", None)
            if source and source_usage[source] > 0:
                source_usage[source] -= 1
            
            proc_to_kill.terminated_by_proxy = True
            proc_to_kill.kill()
            proc_to_kill.wait(timeout=2)
            terminated_stream_url = proc_to_kill.stream_data.get('stream-url')
            logger.info(f"Terminated current stream for channel {cid}.")
        else:
            logger.info(f"No active stream found to terminate for channel {cid}.")

        # Clear the stream's attempt cache and start the new stream with the excluded URL
        stream_attempt_cache.pop(cid, None)
        threading.Thread(target=ensure_stream_running, args=(cid, terminated_stream_url)).start()

    # Redirect to the status page so the user can see the change
    return redirect(url_for('status_page'))

# --- New Route for DB Refresh ---
@app.route("/refresh_db_cache")
def refresh_db_cache():
    """Forces a refresh of the database cache."""
    logger.info("Manual database cache refresh requested.")
    load_streams_from_db()
    return redirect(url_for('status_page'))

# --- Web UI Routes ---

STATUS_PAGE_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Stream Proxy Status</title>
    <meta http-equiv="refresh" content="5">
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background-color: #f4f4f4; color: #333; }
        .container { max-width: 900px; margin: auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        h1, h2, h3 { color: #0056b3; }
        table { width: 100%; border-collapse: collapse; margin-top: 20px; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #e2e6ea; }
        .active { background-color: #d4edda; }
        .free { background-color: #e9ecef; }
        .section-separator { background-color: #f8f9fa; text-align: center; font-weight: bold; }
        .info-box { background-color: #e0f7fa; border-left: 5px solid #00acc1; padding: 10px; margin-top: 20px; }
        .nav-links { margin-bottom: 20px; display: flex; align-items: center;}
        .nav-links a { margin-right: 15px; text-decoration: none; color: #007bff; font-weight: bold; }
        .nav-links a:hover { text-decoration: underline; }
        .force-button { background-color: #ff6347; color: white; border: none; padding: 5px 10px; text-align: center; text-decoration: none; display: inline-block; font-size: 14px; border-radius: 4px; cursor: pointer; }
        .force-button:hover { background-color: #e5533d; }
        .refresh-button { background-color: #007bff; color: white; border: none; padding: 8px 16px; text-align: center; text-decoration: none; display: inline-block; font-size: 14px; border-radius: 4px; cursor: pointer; margin-left: auto; }
        .refresh-button:hover { background-color: #0056b3; }
        .m3u-list { list-style: none; padding: 0; }
        .m3u-list li { margin-bottom: 5px; }
        .m3u-list a { color: #28a745; text-decoration: none; font-weight: bold; }
        .m3u-list a:hover { text-decoration: underline; }
    </style>
</head><body>
    <div class="container">
        <h1>Stream Proxy Status</h1>
        <div class="nav-links">
            <a href="/status">Stream Status</a>
            <a href="/logs">View Logs</a>
            <a href="{{ url_for('refresh_db_cache') }}"><button class="refresh-button">Refresh DB Cache</button></a>
        </div>

        <h2>Available M3U Playlists</h2>
        <ul class="m3u-list">
            {% for m3u in m3u_files %}
                <li><a href="{{ url_for('generate_m3u', m3u_filename=m3u) }}">{{ m3u }}.m3u</a></li>
            {% else %}
                <li>No M3U files found in the database.</li>
            {% endfor %}
        </ul>
        <hr>

        <h2>Active Streams & Source Usage</h2>
        <table>
            <thead>
                <tr>
                    <th>Source Slot</th>
                    <th>Channel ID</th>
                    <th>Channel Name</th>
                    <th>Status</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>
                {% for source_type in source_priority %}
                    {% set limit = source_limits.get(source_type, 0) %}
                    {% for i in range(limit) %}
                        {% set slot_name = source_type ~ (i+1) %}
                        {% set stream_info = active_slots.get(source_type, {}).get(i) %}
                        {% if stream_info %}
                            <tr class="active">
                                <td>{{ slot_name }}</td>
                                <td>{{ stream_info.channel_id }}</td>
                                <td>{{ stream_info.uniform_name or stream_info.channel_name }}</td>
                                <td>Active</td>
                                <td><a href="{{ url_for('force_switch', cid=stream_info.channel_id) }}"><button class="force-button">Force Switch</button></a></td>
                            </tr>
                        {% else %}
                            <tr class="free">
                                <td>{{ slot_name }}</td>
                            <td colspan="3">Free</td>
                            <td></td>
                            </tr>
                        {% endif %}
                    {% endfor %}
                    {% if limit > 0 and not loop.last %}
                        <tr><td colspan="5" class="section-separator"></td></tr>
                    {% endif %}
                {% endfor %}
            </tbody>
        </table>

        <div class="info-box">
            <h3>Summary:</h3>
            <ul>
                {% for source, usage in source_usage.items() %}
                    <li>{{ source }}: {{ usage }} / {{ source_limits.get(source, 'N/A') }} used</li>
                {% endfor %}
            </ul>
        </div>
    </div>
</body>
</html>
"""

LOG_PAGE_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Stream Proxy Logs</title>
    <meta http-equiv="refresh" content="5"> <style>
        body { font-family: 'Courier New', monospace; margin: 20px; background-color: #2b2b2b; color: #f0f0f0; }
        .container { max-width: 1000px; margin: auto; background: #3c3c3c; padding: 15px; border-radius: 5px; overflow-x: auto; white-space: pre-wrap; word-wrap: break-word; font-size: 0.9em; max-height: 80vh; }
        h1 { color: #66ccff; }
        pre { background-color: #1e1e1e; padding: 15px; border-radius: 5px; overflow-x: auto; white-space: pre-wrap; word-wrap: break-word; font-size: 0.9em; max-height: 80vh; }
        .nav-links { margin-bottom: 20px; }
        .nav-links a { margin-right: 15px; text-decoration: none; color: #ffcc66; font-weight: bold; }
        .nav-links a:hover { text-decoration: underline; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Stream Proxy Logs</h1>
        <div class="nav-links">
            <a href="/status">Stream Status</a>
            <a href="/logs">View Logs</a>
        </div>
        <pre>{{ logs|join('\\n') }}</pre>
    </div>
</body>
</html>
"""

@app.route("/status")
def status_page():
    # Ensure the DB cache is fresh to list all M3U files accurately
    load_streams_from_db()
    m3u_files = get_unique_m3u_names()

    current_time = time.time()
    
    # New dictionary to hold streams by their source and slot
    active_slots = {source_type: {} for source_type in SOURCE_PRIORITY}

    for cid, proc in streams.items():
        if proc.poll() is None:
            source = getattr(proc, "source_tag", "Unknown")
            stream_data = getattr(proc, 'stream_data', {})
            channel_name = stream_data.get("Stream-name", f"Channel {cid}")
            uniform_name = stream_data.get("Uniform-name", channel_name)
            
            stream_info = {
                "source": source,
                "channel_id": cid,
                "channel_name": channel_name,
                "uniform_name": uniform_name,
                "source_slot_index": getattr(proc, "source_slot_index", 0)
            }
            
            # Place the stream in the active_slots dictionary
            if source in active_slots:
                source_slot_index = getattr(proc, "source_slot_index", 0)
                active_slots[source][source_slot_index] = stream_info

    return render_template_string(STATUS_PAGE_TEMPLATE, 
                                  active_slots=active_slots,
                                  source_usage=source_usage,
                                  source_limits=SOURCE_LIMITS,
                                  source_priority=SOURCE_PRIORITY,
                                  current_time=current_time,
                                  m3u_files=m3u_files)

@app.route("/logs")
def logs_page():
    return render_template_string(LOG_PAGE_TEMPLATE, logs=list(log_buffer))

if __name__ == "__main__":
    if not is_ffmpeg_available():
        logger.error(f"ERROR: FFmpeg not found. Please ensure '{FFMPEG}' is in your system's PATH.")
        exit(1)
    if not is_ffprobe_available():
        logger.error(f"ERROR: FFprobe not found. Please ensure '{FFMPEG_PROBE}' is in your system's PATH.")
        exit(1)

    logger.info("Starting Flask proxy server...")
    Path("./output").mkdir(exist_ok=True)
    
    load_streams_from_db()

    threading.Thread(target=cleanup_timer_thread, daemon=True).start()
    
    app.run(host="0.0.0.0", port=PORT, threaded=True, debug=False, use_reloader=False)