# Automated YouTube Scraping Workflow

A comprehensive package for scraping and analyzing YouTube data including videos, channels, playlists, and comments. This tool provides both API-based and web scraping methods with automated scheduling capabilities.

## Features

- **Multiple Scraping Methods**: Uses YouTube Data API with fallback to web scraping techniques
- **Comprehensive Data Collection**: Scrape videos, channels, playlists, comments, and search results
- **Flexible Output Formats**: Save data in JSON or CSV formats
- **Automated Scheduling**: Schedule scraping tasks at regular intervals or specific times
- **Robust Error Handling**: Graceful fallbacks and comprehensive logging
- **Rate Limit Management**: Built-in pauses to respect YouTube's rate limits
- **Customizable Configuration**: Easily configure all aspects of the scraping process

## Prerequisites

- Python 3.8 or higher
- YouTube Data API key (optional but recommended)
- Internet connection

## Installation

1. **Download and extract the package**:
   ```bash
   unzip youtube_scraper_package.zip
   cd youtube_scraper_package
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up YouTube API key** (optional but recommended):
   
   Create a `.env` file in the root directory with your YouTube API key:
   ```
   YOUTUBE_API_KEY=your_api_key_here
   ```
   
   To obtain a YouTube API key:
   1. Go to the [Google Cloud Console](https://console.cloud.google.com/)
   2. Create a new project
   3. Enable the YouTube Data API v3
   4. Create credentials (API key)
   5. For more details, see [YouTube API Documentation](https://developers.google.com/youtube/v3/getting-started)

## Configuration

Edit the `config.yaml` file to customize the scraping parameters:

```yaml
# API Settings
api_key_env_var: "YOUTUBE_API_KEY"  # Environment variable name for YouTube API key

# Output Settings
output_directory: "data"  # Directory to store scraped data
output_format: "json"     # Output format (json or csv)

# Scraping Settings
max_results: 50           # Maximum number of videos to retrieve per channel/playlist
comment_count: 100        # Maximum number of comments to retrieve per video
rate_limit_pause: 1       # Pause between API requests in seconds
```

## Usage

### Basic Usage

#### Scrape a YouTube Channel

```bash
python src/scraper.py --channel CHANNEL_ID --comments
```

#### Scrape a YouTube Video

```bash
python src/scraper.py --video VIDEO_ID --comments
```

#### Scrape a YouTube Playlist

```bash
python src/scraper.py --playlist PLAYLIST_ID --comments
```

#### Search YouTube and Scrape Results

```bash
python src/scraper.py --search "your search query" --comments
```

### Scheduled Scraping

#### Start the Scheduler

```bash
python src/scheduler.py --config config.yaml
```

#### Schedule a Channel Scrape (Daily)

```bash
python src/scheduler.py --channel CHANNEL_ID --schedule-type interval --interval-unit days --interval-value 1
```

#### Schedule a Video Scrape (Every 6 Hours)

```bash
python src/scheduler.py --video VIDEO_ID --comments --schedule-type interval --interval-unit hours --interval-value 6
```

#### Schedule with Cron Expression (Every Sunday at 1 AM)

```bash
python src/scheduler.py --playlist PLAYLIST_ID --schedule-type cron --cron-day-of-week 0 --cron-hour 1 --cron-minute 0
```

#### List All Scheduled Jobs

```bash
python src/scheduler.py --action list
```

#### Run a Scheduled Job Immediately

```bash
python src/scheduler.py --action run --job-id JOB_ID
```

#### Remove a Scheduled Job

```bash
python src/scheduler.py --action remove --job-id JOB_ID
```

#### Pause a Scheduled Job

```bash
python src/scheduler.py --action pause --job-id JOB_ID
```

#### Resume a Paused Job

```bash
python src/scheduler.py --action resume --job-id JOB_ID
```

## Data Output

Scraped data is saved in the specified output directory (default: `data/`) with the following structure:

```
data/
├── videos/
│   └── video_VIDEO_ID_info_TIMESTAMP.json
├── channels/
│   └── channel_CHANNEL_ID_videos_TIMESTAMP.json
├── playlists/
│   └── playlist_PLAYLIST_ID_videos_TIMESTAMP.json
├── comments/
│   └── video_VIDEO_ID_comments_TIMESTAMP.json
├── search/
│   └── search_QUERY_videos_TIMESTAMP.json
└── logs/
    └── youtube_scraper_TIMESTAMP.log
```

## Advanced Usage

### Using as a Python Module

You can import and use the scraper in your own Python scripts:

```python
from src.scraper import YouTubeScraper

# Initialize the scraper
scraper = YouTubeScraper("config.yaml")

# Scrape a channel
channel_data = scraper.get_channel_videos("CHANNEL_ID")

# Scrape video comments
comments = scraper.get_video_comments("VIDEO_ID")

# Save the data
scraper.save_data(channel_data, "channel_data", "channels")
```

### Batch Processing

You can define multiple channels, videos, playlists, or search queries in the `config.yaml` file and process them in batch:

```python
from src.scraper import YouTubeScraper
from src.utils import load_config

# Load configuration
config = load_config("config.yaml")
scraper = YouTubeScraper("config.yaml")

# Process all channels defined in config
for channel in config.get('channels', []):
    channel_id = channel['id']
    include_comments = channel.get('include_comments', False)
    scraper.run_channel_scrape(channel_id, include_comments)
```

## Troubleshooting

### Common Issues

1. **API Quota Exceeded**:
   - The YouTube Data API has daily quotas. If you exceed them, the scraper will fall back to web scraping methods.
   - Consider reducing the frequency of scheduled jobs or the amount of data requested.

2. **Web Scraping Failures**:
   - YouTube's web interface changes frequently, which may break web scraping methods.
   - Update to the latest version of this package or use the API method.

3. **Rate Limiting**:
   - If you're getting rate limit errors, increase the `rate_limit_pause` value in the config.

### Logs

Check the log files in the `logs/` directory for detailed information about any errors or issues.

## Legal Considerations

- Ensure you comply with YouTube's [Terms of Service](https://www.youtube.com/t/terms) when using this tool.
- Respect rate limits and don't overload YouTube's servers.
- This tool is for educational and research purposes only.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [pytube](https://github.com/pytube/pytube) for YouTube video downloading capabilities
- [google-api-python-client](https://github.com/googleapis/google-api-python-client) for YouTube API integration
- [APScheduler](https://github.com/agronholm/apscheduler) for scheduling functionality
