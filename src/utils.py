#!/usr/bin/env python3
"""
Utility functions for YouTube Scraper

This module provides helper functions for:
- Data saving and loading
- Authentication with YouTube API
- Data processing and transformation
- Error handling and logging
"""

import os
import json
import csv
import logging
import time
from typing import Dict, List, Any, Union, Optional
import pandas as pd
import yaml
from datetime import datetime
import googleapiclient.discovery
import googleapiclient.errors
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("youtube_scraper_utils.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("youtube_scraper_utils")

def load_config(config_path: str) -> Dict:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Dict containing configuration parameters
    """
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        logger.info(f"Configuration loaded from {config_path}")
        return config
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        logger.info("Using default configuration")
        return {
            'api_key_env_var': 'YOUTUBE_API_KEY',
            'output_directory': 'data',
            'output_format': 'json',
            'max_results': 50,
            'comment_count': 100,
            'rate_limit_pause': 1
        }

def setup_youtube_api(api_key: Optional[str] = None) -> Any:
    """
    Set up the YouTube API client.
    
    Args:
        api_key: YouTube API key (if None, will try to get from environment)
        
    Returns:
        YouTube API client or None if setup fails
    """
    # Load environment variables from .env file if it exists
    load_dotenv()
    
    # Use provided API key or get from environment
    if not api_key:
        api_key = os.environ.get('YOUTUBE_API_KEY')
    
    if not api_key:
        logger.error("No YouTube API key found")
        return None
    
    try:
        api_service_name = "youtube"
        api_version = "v3"
        youtube = googleapiclient.discovery.build(
            api_service_name, api_version, developerKey=api_key)
        logger.info("YouTube API client initialized successfully")
        return youtube
    except Exception as e:
        logger.error(f"Failed to initialize YouTube API client: {e}")
        return None

def save_to_json(data: Union[List[Dict], Dict], file_path: str) -> bool:
    """
    Save data to a JSON file.
    
    Args:
        data: Data to save (list of dictionaries or single dictionary)
        file_path: Path to save the JSON file
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Data saved to JSON file: {file_path}")
        return True
    except Exception as e:
        logger.error(f"Error saving to JSON file: {e}")
        return False

def save_to_csv(data: Union[List[Dict], Dict], file_path: str) -> bool:
    """
    Save data to a CSV file.
    
    Args:
        data: Data to save (list of dictionaries or single dictionary)
        file_path: Path to save the CSV file
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Convert to DataFrame
        if isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            df = pd.DataFrame([data])
        
        # Save to CSV
        df.to_csv(file_path, index=False, encoding='utf-8')
        
        logger.info(f"Data saved to CSV file: {file_path}")
        return True
    except Exception as e:
        logger.error(f"Error saving to CSV file: {e}")
        return False

def load_from_json(file_path: str) -> Union[List[Dict], Dict, None]:
    """
    Load data from a JSON file.
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        Loaded data or None if loading fails
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        logger.info(f"Data loaded from JSON file: {file_path}")
        return data
    except Exception as e:
        logger.error(f"Error loading from JSON file: {e}")
        return None

def load_from_csv(file_path: str) -> Union[pd.DataFrame, None]:
    """
    Load data from a CSV file.
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        Pandas DataFrame or None if loading fails
    """
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
        
        logger.info(f"Data loaded from CSV file: {file_path}")
        return df
    except Exception as e:
        logger.error(f"Error loading from CSV file: {e}")
        return None

def merge_data_files(file_paths: List[str], output_path: str, file_format: str = 'json') -> bool:
    """
    Merge multiple data files into a single file.
    
    Args:
        file_paths: List of paths to the files to merge
        output_path: Path to save the merged file
        file_format: Format of the files ('json' or 'csv')
        
    Returns:
        True if successful, False otherwise
    """
    try:
        merged_data = []
        
        for file_path in file_paths:
            if file_format.lower() == 'json':
                data = load_from_json(file_path)
                if data:
                    if isinstance(data, list):
                        merged_data.extend(data)
                    else:
                        merged_data.append(data)
            elif file_format.lower() == 'csv':
                df = load_from_csv(file_path)
                if df is not None:
                    if not merged_data:
                        merged_data = df
                    else:
                        merged_data = pd.concat([merged_data, df], ignore_index=True)
            else:
                logger.error(f"Unsupported file format: {file_format}")
                return False
        
        # Save merged data
        if file_format.lower() == 'json':
            return save_to_json(merged_data, output_path)
        elif file_format.lower() == 'csv':
            return merged_data.to_csv(output_path, index=False, encoding='utf-8')
        
        return True
    except Exception as e:
        logger.error(f"Error merging data files: {e}")
        return False

def extract_video_id_from_url(url: str) -> Optional[str]:
    """
    Extract the video ID from a YouTube URL.
    
    Args:
        url: YouTube URL
        
    Returns:
        Video ID or None if extraction fails
    """
    import re
    
    # Patterns for YouTube URLs
    patterns = [
        r'(?:v=|\/)([0-9A-Za-z_-]{11}).*',  # Standard YouTube URL
        r'(?:embed\/)([0-9A-Za-z_-]{11})',  # Embedded YouTube URL
        r'(?:youtu\.be\/)([0-9A-Za-z_-]{11})'  # Shortened YouTube URL
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    
    logger.error(f"Could not extract video ID from URL: {url}")
    return None

def extract_channel_id_from_url(url: str) -> Optional[str]:
    """
    Extract the channel ID from a YouTube URL.
    
    Args:
        url: YouTube channel URL
        
    Returns:
        Channel ID or None if extraction fails
    """
    import re
    
    # Patterns for YouTube channel URLs
    patterns = [
        r'(?:channel\/)([0-9A-Za-z_-]+)',  # Standard channel URL
        r'(?:c\/)([0-9A-Za-z_-]+)',  # Custom channel URL
        r'(?:user\/)([0-9A-Za-z_-]+)'  # User URL
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    
    logger.error(f"Could not extract channel ID from URL: {url}")
    return None

def extract_playlist_id_from_url(url: str) -> Optional[str]:
    """
    Extract the playlist ID from a YouTube URL.
    
    Args:
        url: YouTube playlist URL
        
    Returns:
        Playlist ID or None if extraction fails
    """
    import re
    
    # Pattern for YouTube playlist URL
    pattern = r'(?:list=)([0-9A-Za-z_-]+)'
    
    match = re.search(pattern, url)
    if match:
        return match.group(1)
    
    logger.error(f"Could not extract playlist ID from URL: {url}")
    return None

def format_duration(duration_str: str) -> int:
    """
    Convert YouTube API duration format (ISO 8601) to seconds.
    
    Args:
        duration_str: Duration string in ISO 8601 format (e.g., 'PT1H2M3S')
        
    Returns:
        Duration in seconds
    """
    import re
    import isodate
    
    try:
        duration = isodate.parse_duration(duration_str)
        return int(duration.total_seconds())
    except Exception as e:
        logger.error(f"Error parsing duration: {e}")
        
        # Fallback to manual parsing
        hours = 0
        minutes = 0
        seconds = 0
        
        # Extract hours, minutes, seconds
        hour_match = re.search(r'(\d+)H', duration_str)
        if hour_match:
            hours = int(hour_match.group(1))
        
        minute_match = re.search(r'(\d+)M', duration_str)
        if minute_match:
            minutes = int(minute_match.group(1))
        
        second_match = re.search(r'(\d+)S', duration_str)
        if second_match:
            seconds = int(second_match.group(1))
        
        return hours * 3600 + minutes * 60 + seconds

def handle_rate_limit(func):
    """
    Decorator to handle YouTube API rate limiting.
    
    Args:
        func: Function to decorate
        
    Returns:
        Wrapped function with rate limit handling
    """
    def wrapper(*args, **kwargs):
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                return func(*args, **kwargs)
            except googleapiclient.errors.HttpError as e:
                if e.resp.status in [403, 429]:  # Rate limit exceeded
                    retry_count += 1
                    wait_time = 2 ** retry_count  # Exponential backoff
                    logger.warning(f"Rate limit exceeded. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"HTTP error: {e}")
                    raise
            except Exception as e:
                logger.error(f"Error in {func.__name__}: {e}")
                raise
        
        logger.error(f"Max retries exceeded for {func.__name__}")
        raise Exception(f"Max retries exceeded for {func.__name__}")
    
    return wrapper

def create_directory_structure(base_dir: str) -> bool:
    """
    Create the directory structure for the YouTube scraper.
    
    Args:
        base_dir: Base directory path
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Create main directories
        directories = [
            base_dir,
            os.path.join(base_dir, 'videos'),
            os.path.join(base_dir, 'channels'),
            os.path.join(base_dir, 'playlists'),
            os.path.join(base_dir, 'comments'),
            os.path.join(base_dir, 'search'),
            os.path.join(base_dir, 'logs')
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
            logger.info(f"Created directory: {directory}")
        
        return True
    except Exception as e:
        logger.error(f"Error creating directory structure: {e}")
        return False

def setup_logging(log_dir: str, log_level: str = 'INFO') -> None:
    """
    Set up logging for the YouTube scraper.
    
    Args:
        log_dir: Directory to store log files
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    # Create log directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)
    
    # Set up logging level
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")
    
    # Configure logging
    log_file = os.path.join(log_dir, f"youtube_scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    logger.info(f"Logging set up with level {log_level} to {log_file}")

def generate_report(data_files: List[str], output_file: str, report_format: str = 'markdown') -> bool:
    """
    Generate a report from scraped data.
    
    Args:
        data_files: List of paths to data files
        output_file: Path to save the report
        report_format: Format of the report ('markdown', 'html', 'txt')
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Load data from files
        data = []
        for file_path in data_files:
            if file_path.endswith('.json'):
                file_data = load_from_json(file_path)
                if file_data:
                    if isinstance(file_data, list):
                        data.extend(file_data)
                    else:
                        data.append(file_data)
            elif file_path.endswith('.csv'):
                df = load_from_csv(file_path)
                if df is not None:
                    data.extend(df.to_dict('records'))
        
        if not data:
            logger.error("No data found in the provided files")
            return False
        
        # Generate report
        report = []
        
        # Report header
        if report_format == 'markdown':
            report.append("# YouTube Scraper Report")
            report.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            report.append("")
            report.append(f"## Summary")
            report.append(f"- Total items: {len(data)}")
            report.append("")
            
            # Add data summary based on type
            if 'video_id' in data[0]:
                report.append("## Video Data")
                
                # Count unique videos
                video_ids = set(item.get('video_id') for item in data if 'video_id' in item)
                report.append(f"- Unique videos: {len(video_ids)}")
                
                # List top videos by view count if available
                if any('view_count' in item for item in data):
                    videos_with_views = [item for item in data if 'view_count' in item]
                    videos_with_views.sort(key=lambda x: int(x.get('view_count', 0)), reverse=True)
                    
                    report.append("\n### Top Videos by View Count")
                    report.append("| Video ID | Title | View Count |")
                    report.append("|----------|-------|------------|")
                    
                    for video in videos_with_views[:10]:  # Top 10 videos
                        report.append(f"| {video.get('video_id', 'N/A')} | {video.get('title', 'N/A')} | {video.get('view_count', 'N/A')} |")
            
            # Add comment data if available
            if 'comment_id' in data[0]:
                report.append("\n## Comment Data")
                
                # Count unique comments
                comment_ids = set(item.get('comment_id') for item in data if 'comment_id' in item)
                report.append(f"- Total comments: {len(comment_ids)}")
                
                # Count comments per video
                videos_with_comments = {}
                for item in data:
                    if 'video_id' in item:
                        video_id = item['video_id']
                        videos_with_comments[video_id] = videos_with_comments.get(video_id, 0) + 1
                
                report.append("\n### Comments per Video")
                report.append("| Video ID | Comment Count |")
                report.append("|----------|---------------|")
                
                for video_id, count in sorted(videos_with_comments.items(), key=lambda x: x[1], reverse=True)[:10]:
                    report.append(f"| {video_id} | {count} |")
        
        elif report_format == 'html':
            report.append("<html>")
            report.append("<head><title>YouTube Scraper Report</title></head>")
            report.append("<body>")
            report.append("<h1>YouTube Scraper Report</h1>")
            report.append(f"<p>Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>")
            report.append("<h2>Summary</h2>")
            report.append(f"<p>Total items: {len(data)}</p>")
            
            # Add data summary based on type
            if 'video_id' in data[0]:
                report.append("<h2>Video Data</h2>")
                
                # Count unique videos
                video_ids = set(item.get('video_id') for item in data if 'video_id' in item)
                report.append(f"<p>Unique videos: {len(video_ids)}</p>")
                
                # List top videos by view count if available
                if any('view_count' in item for item in data):
                    videos_with_views = [item for item in data if 'view_count' in item]
                    videos_with_views.sort(key=lambda x: int(x.get('view_count', 0)), reverse=True)
                    
                    report.append("<h3>Top Videos by View Count</h3>")
                    report.append("<table border='1'>")
                    report.append("<tr><th>Video ID</th><th>Title</th><th>View Count</th></tr>")
                    
                    for video in videos_with_views[:10]:  # Top 10 videos
                        report.append(f"<tr><td>{video.get('video_id', 'N/A')}</td><td>{video.get('title', 'N/A')}</td><td>{video.get('view_count', 'N/A')}</td></tr>")
                    
                    report.append("</table>")
            
            report.append("</body>")
            report.append("</html>")
        
        elif report_format == 'txt':
            report.append("YouTube Scraper Report")
            report.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            report.append("")
            report.append("Summary")
            report.append(f"Total items: {len(data)}")
            report.append("")
            
            # Add data summary based on type
            if 'video_id' in data[0]:
                report.append("Video Data")
                
                # Count unique videos
                video_ids = set(item.get('video_id') for item in data if 'video_id' in item)
                report.append(f"Unique videos: {len(video_ids)}")
                
                # List top videos by view count if available
                if any('view_count' in item for item in data):
                    videos_with_views = [item for item in data if 'view_count' in item]
                    videos_with_views.sort(key=lambda x: int(x.get('view_count', 0)), reverse=True)
                    
                    report.append("\nTop Videos by View Count")
                    report.append("-----------------------")
                    
                    for video in videos_with_views[:10]:  # Top 10 videos
                        report.append(f"Video ID: {video.get('video_id', 'N/A')}")
                        report.append(f"Title: {video.get('title', 'N/A')}")
                        report.append(f"View Count: {video.get('view_count', 'N/A')}")
                        report.append("")
        
        # Save report to file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report))
        
        logger.info(f"Report generated and saved to {output_file}")
        return True
    except Exception as e:
        logger.error(f"Error generating report: {e}")
        return False

if __name__ == "__main__":
    # Example usage
    config = load_config("../config.yaml")
    print(f"Loaded configuration: {config}")
    
    # Set up YouTube API
    youtube = setup_youtube_api()
    if youtube:
        print("YouTube API client set up successfully")
    else:
        print("Failed to set up YouTube API client")
    
    # Create directory structure
    create_directory_structure("data")
