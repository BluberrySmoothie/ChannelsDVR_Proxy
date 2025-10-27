# Stream Proxy Server

A Flask-based HLS stream proxy that manages live video streams from multiple sources, validates stream integrity, and serves them through dynamic M3U playlists. Designed to work with Channels DVR for intelligent stream selection and automatic failover management.

## Features

- **Multi-source Stream Management**: Handles streams from multiple sources with configurable priority and concurrency limits
- **Stream Validation**: Uses FFprobe to validate stream accessibility and codec integrity before starting
- **HLS Output**: Converts input streams to HLS (HTTP Live Streaming) format with segment-based delivery
- **Smart Stream Selection**: Prioritizes streams by source and quality, with automatic failover to alternative sources
- **Channels DVR Integration**: Monitors Channels DVR activity and automatically terminates inactive streams
- **Inactivity Detection**: Terminates streams that are no longer being watched or requested
- **Web Dashboard**: Real-time UI showing active streams, source usage, and stream management controls
- **Buffering Detection**: Monitors FFmpeg output for buffering issues and triggers stream restarts
- **Database-Driven**: SQLite database stores stream configurations with dynamic M3U playlist generation
- **Comprehensive Logging**: Detailed logging with both console output and in-browser log viewer

## Requirements

- Python 3.8+
- FFmpeg (with FFprobe)
- SQLite3
- Channels DVR (for DVR integration features)

See `requirements.txt` for Python dependencies.

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/yourusername/stream-proxy-server.git
cd stream-proxy-server
```

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Install FFmpeg

**Windows:**
Download from [ffmpeg.org](https://ffmpeg.org/download.html) or use a package manager:
```bash
choco install ffmpeg
```

**macOS:**
```bash
brew install ffmpeg
```

**Linux (Ubuntu/Debian):**
```bash
sudo apt-get install ffmpeg
```

Ensure FFmpeg and FFprobe are in your system PATH.

### 4. Configure the application

Edit `proxy_server.py` and update these configuration values:

```python
PORT = 9090                              # HTTP port for the proxy
DB_PATH = r"C:\\YourPath\\vod.db"        # Path to SQLite database
CHANNELS_DVR_URL = "http://123.456.789.012:8089/dvr" # Channels DVR API URL

QUALITY_PRIORITY = ["1", "2", ...]      # Stream quality ranking (lower = higher priority)
SOURCE_PRIORITY = ["Source1", "Source2"]           # Source ranking
SOURCE_LIMITS = {"Source1": 5, "Source2": 4}           # Max concurrent streams per source

TARGET_VIDEO_BITRATE = "5M"              # Output video bitrate
TARGET_AUDIO_BITRATE = "128k"            # Output audio bitrate
```

### 5. Set up the SQLite database

The database should have a `LiveStreams` table with the following columns:
- `Channel-id` (INTEGER)
- `Stream-name` (TEXT)
- `stream-url` (TEXT)
- `Source` (TEXT)
- `Stream-quality` (TEXT)
- `M3U` (TEXT) - M3U playlist filename
- `TV-Logo` (TEXT)
- `Group-title` (TEXT)
- `Uniform-name` (TEXT)
- `ISO3` (TEXT)
- `TV-guide-id` (TEXT)
- `TV-guide-name` (TEXT)
- `Stream-status` (TEXT)
- `Last-checked-time` (TEXT)
- `Uptime` (INTEGER)
- `Downtime` (INTEGER)

## Usage

### Starting the server

```bash
python proxy_server.py
```

The server will start on `http://localhost:9090` (or configured PORT).

### Web Dashboard

Access the status page at:
- **Status**: `http://localhost:9090/status` - View active streams and manage them
- **Logs**: `http://localhost:9090/logs` - View real-time application logs

### M3U Playlists

Generate M3U playlists dynamically:
```
http://localhost:9090/m3u/playlist_name.m3u
```

The server will serve streams from the database based on the `M3U` column value.

### Stream Serving

Individual streams are served at:
```
http://localhost:9090/channel/<channel_id>/stream.m3u8
http://localhost:9090/channel/<channel_id>/<segment_file>.ts
```

### Management API

**Force stream switch** (terminate current stream and try next candidate):
```
http://localhost:9090/force_switch/<channel_id>
```

**Refresh database cache**:
```
http://localhost:9090/refresh_db_cache
```

## How It Works

1. **Stream Selection**: When a channel is requested, the proxy selects the best available stream based on source and quality priority.

2. **Validation**: FFprobe validates that the stream is accessible and contains valid video/audio codecs before starting.

3. **HLS Conversion**: FFmpeg remuxes the stream to HLS format with 6-second segments and a rolling playlist of up to 3 segments.

4. **Buffering Detection**: Monitors FFmpeg output for stalling/buffering messages and automatically restarts the stream if issues are detected.

5. **Activity Monitoring**: Periodically checks Channels DVR for active channels. If a stream isn't being watched and hasn't been requested recently, it's automatically terminated to free resources.

6. **Automatic Failover**: If a stream fails validation, fails to start, or encounters buffering issues, the proxy automatically tries the next candidate from the available sources.

## Configuration Details

### Buffering and Timeout Thresholds

```python
BUFFER_CHECK_INTERVAL = 5      # Check interval in seconds
BUFFER_THRESHOLD = 3           # Number of buffering events before automatic restart
STREAM_START_TIMEOUT = 15      # Max time to wait for stream to start (seconds)
INACTIVITY_TIMEOUT = 30        # Seconds before terminating inactive streams
FFPROBE_TIMEOUT = 10           # Timeout for FFprobe validation (seconds)
```

### Source Management

Control concurrent streams per source to prevent overwhelming upstream providers:

```python
SOURCE_PRIORITY = ["XCodes"]   # Priority order for sources
SOURCE_LIMITS = {"XCodes": 5}  # Max 5 simultaneous streams from XCodes
```

When a source reaches its limit, the proxy will try other sources before queuing the request.

### Quality Priority

Set the quality ranking (lower index = higher priority):

```python
QUALITY_PRIORITY = ["1", "2", "3", "4", "5", ...]
```

The proxy will prefer lower-indexed qualities when multiple options are available.

## Troubleshooting

**FFmpeg not found**
- Ensure FFmpeg is installed and in your system PATH
- Test with: `ffmpeg -version`

**Database connection errors**
- Verify `DB_PATH` points to the correct SQLite database
- Ensure the database has the required schema

**Channels DVR connection issues**
- Confirm `CHANNELS_DVR_URL` is correct and the DVR is running
- Check network connectivity to the DVR
- Look for error messages in the logs page

**Streams keep restarting**
- Check FFmpeg output in logs for codec issues
- Verify stream URLs are accessible and working
- Increase `BUFFER_THRESHOLD` if streams are temporarily unstable
- Check if source limits are being reached

**High CPU usage**
- Consider reducing `QUALITY_PRIORITY` to lower resolution streams
- Increase segment duration (`-hls_time` parameter in ffmpeg_args)
- Reduce maximum concurrent streams via `SOURCE_LIMITS`

**404 errors when requesting streams**
- Ensure the stream's M3U playlist filename is set correctly in the database
- Verify channel IDs in requests match database values
- Check logs for stream startup errors

## Logging

Logs are displayed in real-time and available via the web UI at `/logs`. The application maintains a rolling buffer of the last 500 log messages. Logs include:
- Stream startup/shutdown events
- FFprobe validation results
- Buffering detection and recovery
- Source usage tracking
- Database operations
- Channels DVR polling results

## Architecture

- **Flask**: Web framework and HTTP routing
- **FFmpeg**: Stream remuxing to HLS format
- **FFprobe**: Stream validation and format detection
- **SQLite**: Stream configuration and metadata storage
- **Threading**: Concurrent stream management and monitoring
- **Requests**: HTTP communication with Channels DVR API

## Performance Tips

1. **Monitor Source Limits**: Adjust `SOURCE_LIMITS` based on upstream provider capabilities
2. **Tune Quality Priority**: Start with lower qualities if CPU is constrained
3. **Adjust Segment Duration**: Longer segments reduce fragmentation but increase latency
4. **Buffer Thresholds**: Lower `BUFFER_THRESHOLD` for faster failover, higher values for stability with unstable streams
5. **DVR Poll Interval**: The cleanup thread checks DVR status every 30 seconds—adjust as needed

## License

[Specify your license here]

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For issues, questions, or suggestions, please open an issue on GitHub.
