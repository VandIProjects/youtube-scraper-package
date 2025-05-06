#!/usr/bin/env python3
"""
YouTube Scraper Module

This module provides functionality to scrape data from YouTube including:
- Video information (title, views, likes, description)
- Channel information
- Comments
- Playlist data

It uses a combination of YouTube Data API and web scraping techniques.
"""

import os
import json
import logging
import time
from datetime import datetime
import re
import requests
from typing import Dict, List, Optional, Union, Any
import googleapiclient.discovery
import googleapiclient.errors
from pytube import YouTube, Channel, Playlist
import pandas as pd
from bs4 import BeautifulSoup
import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("youtube_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("youtube_scraper")

class YouTubeScraper:
    """Main YouTube scraper class that handles all scraping operations."""
    
    def __init__(self, config_path: str = "../config.yaml"):
        """
        Initialize the YouTube scraper with configuration.
        
        Args:
            config_path: Path to the configuration YAML file
        """
        self.config = self._load_config(config_path)
        self.api_key = os.environ.get(self.config['api_key_env_var'])
        self.output_dir = self.config.get('output_directory', 'data')
        self.output_format = self.config.get('output_format', 'json')
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Initialize YouTube API client if API key is available
        self.youtube = None
        if self.api_key:
            try:
                api_service_name = "youtube"
                api_version = "v3"
                self.youtube = googleapiclient.discovery.build(
                    api_service_name, api_version, developerKey=self.api_key)
                logger.info("YouTube API client initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize YouTube API client: {e}")
                logger.info("Falling back to web scraping methods")
        else:
            logger.warning("No YouTube API key found. Using web scraping methods only.")
    
    def _load_config(self, config_path: str) -> Dict:
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
    
    def get_video_info(self, video_id: str) -> Dict:
        """
        Get comprehensive information about a YouTube video.
        
        Args:
            video_id: YouTube video ID
            
        Returns:
            Dictionary containing video information
        """
        video_data = {}
        
        # Try API method first if available
        if self.youtube:
            try:
                request = self.youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=video_id
                )
                response = request.execute()
                
                if response['items']:
                    item = response['items'][0]
                    video_data = {
                        'video_id': video_id,
                        'title': item['snippet']['title'],
                        'description': item['snippet']['description'],
                        'published_at': item['snippet']['publishedAt'],
                        'channel_id': item['snippet']['channelId'],
                        'channel_title': item['snippet']['channelTitle'],
                        'tags': item['snippet'].get('tags', []),
                        'category_id': item['snippet'].get('categoryId', ''),
                        'duration': item['contentDetails']['duration'],
                        'view_count': item['statistics'].get('viewCount', 0),
                        'like_count': item['statistics'].get('likeCount', 0),
                        'comment_count': item['statistics'].get('commentCount', 0),
                        'thumbnail_url': item['snippet']['thumbnails']['high']['url'],
                        'scraped_at': datetime.now().isoformat(),
                        'source': 'api'
                    }
                    logger.info(f"Retrieved video info for {video_id} via API")
                    return video_data
            except Exception as e:
                logger.error(f"API error when getting video info for {video_id}: {e}")
                logger.info("Falling back to web scraping")
        
        # Fallback to pytube
        try:
            yt = YouTube(f"https://www.youtube.com/watch?v={video_id}")
            video_data = {
                'video_id': video_id,
                'title': yt.title,
                'description': yt.description,
                'length': yt.length,
                'views': yt.views,
                'author': yt.author,
                'channel_id': yt.channel_id,
                'channel_url': yt.channel_url,
                'thumbnail_url': yt.thumbnail_url,
                'publish_date': yt.publish_date.isoformat() if yt.publish_date else None,
                'keywords': yt.keywords,
                'scraped_at': datetime.now().isoformat(),
                'source': 'pytube'
            }
            logger.info(f"Retrieved video info for {video_id} via pytube")
        except Exception as e:
            logger.error(f"Error getting video info for {video_id}: {e}")
            video_data = {'video_id': video_id, 'error': str(e), 'scraped_at': datetime.now().isoformat()}
        
        return video_data
    
    def get_channel_videos(self, channel_id: str, max_results: Optional[int] = None) -> List[Dict]:
        """
        Get videos from a YouTube channel.
        
        Args:
            channel_id: YouTube channel ID
            max_results: Maximum number of videos to retrieve (None for all)
            
        Returns:
            List of dictionaries containing video information
        """
        if max_results is None:
            max_results = self.config.get('max_results', 50)
        
        videos = []
        
        # Try API method first if available
        if self.youtube:
            try:
                # First get upload playlist ID for the channel
                request = self.youtube.channels().list(
                    part="contentDetails",
                    id=channel_id
                )
                response = request.execute()
                
                if response['items']:
                    uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
                    
                    # Now get videos from the uploads playlist
                    next_page_token = None
                    video_count = 0
                    
                    while True:
                        playlist_request = self.youtube.playlistItems().list(
                            part="snippet,contentDetails",
                            playlistId=uploads_playlist_id,
                            maxResults=50,
                            pageToken=next_page_token
                        )
                        playlist_response = playlist_request.execute()
                        
                        for item in playlist_response['items']:
                            video_id = item['contentDetails']['videoId']
                            video_info = {
                                'video_id': video_id,
                                'title': item['snippet']['title'],
                                'description': item['snippet']['description'],
                                'published_at': item['snippet']['publishedAt'],
                                'channel_id': channel_id,
                                'channel_title': item['snippet']['channelTitle'],
                                'position': item['snippet']['position'],
                                'thumbnail_url': item['snippet']['thumbnails']['high']['url'] if 'high' in item['snippet']['thumbnails'] else '',
                                'scraped_at': datetime.now().isoformat(),
                                'source': 'api'
                            }
                            videos.append(video_info)
                            video_count += 1
                            
                            if max_results and video_count >= max_results:
                                break
                        
                        next_page_token = playlist_response.get('nextPageToken')
                        
                        if not next_page_token or (max_results and video_count >= max_results):
                            break
                        
                        # Respect YouTube API rate limits
                        time.sleep(self.config.get('rate_limit_pause', 1))
                    
                    logger.info(f"Retrieved {len(videos)} videos for channel {channel_id} via API")
                    return videos
            except Exception as e:
                logger.error(f"API error when getting channel videos for {channel_id}: {e}")
                logger.info("Falling back to web scraping")
        
        # Fallback to pytube
        try:
            channel = Channel(f"https://www.youtube.com/channel/{channel_id}")
            video_count = 0
            
            for url in channel.video_urls:
                if max_results and video_count >= max_results:
                    break
                
                video_id = url.split('v=')[1]
                try:
                    yt = YouTube(url)
                    video_info = {
                        'video_id': video_id,
                        'title': yt.title,
                        'description': yt.description,
                        'length': yt.length,
                        'views': yt.views,
                        'author': yt.author,
                        'channel_id': channel_id,
                        'channel_url': yt.channel_url,
                        'thumbnail_url': yt.thumbnail_url,
                        'publish_date': yt.publish_date.isoformat() if yt.publish_date else None,
                        'keywords': yt.keywords,
                        'scraped_at': datetime.now().isoformat(),
                        'source': 'pytube'
                    }
                    videos.append(video_info)
                    video_count += 1
                    
                    # Respect rate limits
                    time.sleep(self.config.get('rate_limit_pause', 1))
                except Exception as e:
                    logger.error(f"Error getting info for video {video_id}: {e}")
            
            logger.info(f"Retrieved {len(videos)} videos for channel {channel_id} via pytube")
        except Exception as e:
            logger.error(f"Error getting channel videos for {channel_id}: {e}")
        
        return videos
    
    def get_video_comments(self, video_id: str, max_comments: Optional[int] = None) -> List[Dict]:
        """
        Get comments for a YouTube video.
        
        Args:
            video_id: YouTube video ID
            max_comments: Maximum number of comments to retrieve
            
        Returns:
            List of dictionaries containing comment information
        """
        if max_comments is None:
            max_comments = self.config.get('comment_count', 100)
        
        comments = []
        
        # Try API method first if available
        if self.youtube:
            try:
                request = self.youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=min(max_comments, 100)  # API limit is 100 per request
                )
                response = request.execute()
                
                for item in response['items']:
                    comment = item['snippet']['topLevelComment']['snippet']
                    comment_info = {
                        'comment_id': item['id'],
                        'video_id': video_id,
                        'text': comment['textDisplay'],
                        'author': comment['authorDisplayName'],
                        'author_channel_id': comment.get('authorChannelId', {}).get('value', ''),
                        'like_count': comment['likeCount'],
                        'published_at': comment['publishedAt'],
                        'updated_at': comment['updatedAt'],
                        'scraped_at': datetime.now().isoformat(),
                        'source': 'api'
                    }
                    comments.append(comment_info)
                
                logger.info(f"Retrieved {len(comments)} comments for video {video_id} via API")
                return comments
            except Exception as e:
                logger.error(f"API error when getting comments for {video_id}: {e}")
                logger.info("Falling back to web scraping")
        
        # Fallback to web scraping with requests and BeautifulSoup
        try:
            # This is a simplified version - in practice, YouTube comments require more complex scraping
            # due to dynamic loading and anti-scraping measures
            url = f"https://www.youtube.com/watch?v={video_id}"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                comment_elements = soup.find_all('ytd-comment-thread-renderer')
                
                for element in comment_elements[:max_comments]:
                    try:
                        author_element = element.find('a', id='author-text')
                        text_element = element.find('yt-formatted-string', id='content-text')
                        
                        if author_element and text_element:
                            comment_info = {
                                'video_id': video_id,
                                'text': text_element.text.strip(),
                                'author': author_element.text.strip(),
                                'scraped_at': datetime.now().isoformat(),
                                'source': 'web_scraping'
                            }
                            comments.append(comment_info)
                    except Exception as e:
                        logger.error(f"Error parsing comment element: {e}")
                
                logger.info(f"Retrieved {len(comments)} comments for video {video_id} via web scraping")
            else:
                logger.error(f"Failed to retrieve page for video {video_id}: {response.status_code}")
        except Exception as e:
            logger.error(f"Error getting comments for video {video_id}: {e}")
        
        return comments
    
    def get_playlist_videos(self, playlist_id: str, max_results: Optional[int] = None) -> List[Dict]:
        """
        Get videos from a YouTube playlist.
        
        Args:
            playlist_id: YouTube playlist ID
            max_results: Maximum number of videos to retrieve
            
        Returns:
            List of dictionaries containing video information
        """
        if max_results is None:
            max_results = self.config.get('max_results', 50)
        
        videos = []
        
        # Try API method first if available
        if self.youtube:
            try:
                next_page_token = None
                video_count = 0
                
                while True:
                    request = self.youtube.playlistItems().list(
                        part="snippet,contentDetails",
                        playlistId=playlist_id,
                        maxResults=50,
                        pageToken=next_page_token
                    )
                    response = request.execute()
                    
                    for item in response['items']:
                        video_id = item['contentDetails']['videoId']
                        video_info = {
                            'video_id': video_id,
                            'title': item['snippet']['title'],
                            'description': item['snippet']['description'],
                            'published_at': item['snippet']['publishedAt'],
                            'channel_id': item['snippet']['channelId'],
                            'channel_title': item['snippet']['channelTitle'],
                            'position': item['snippet']['position'],
                            'playlist_id': playlist_id,
                            'thumbnail_url': item['snippet']['thumbnails']['high']['url'] if 'high' in item['snippet']['thumbnails'] else '',
                            'scraped_at': datetime.now().isoformat(),
                            'source': 'api'
                        }
                        videos.append(video_info)
                        video_count += 1
                        
                        if max_results and video_count >= max_results:
                            break
                    
                    next_page_token = response.get('nextPageToken')
                    
                    if not next_page_token or (max_results and video_count >= max_results):
                        break
                    
                    # Respect YouTube API rate limits
                    time.sleep(self.config.get('rate_limit_pause', 1))
                
                logger.info(f"Retrieved {len(videos)} videos for playlist {playlist_id} via API")
                return videos
            except Exception as e:
                logger.error(f"API error when getting playlist videos for {playlist_id}: {e}")
                logger.info("Falling back to web scraping")
        
        # Fallback to pytube
        try:
            playlist = Playlist(f"https://www.youtube.com/playlist?list={playlist_id}")
            video_count = 0
            
            for url in playlist.video_urls:
                if max_results and video_count >= max_results:
                    break
                
                video_id = url.split('v=')[1]
                try:
                    yt = YouTube(url)
                    video_info = {
                        'video_id': video_id,
                        'title': yt.title,
                        'description': yt.description,
                        'length': yt.length,
                        'views': yt.views,
                        'author': yt.author,
                        'channel_id': yt.channel_id,
                        'playlist_id': playlist_id,
                        'channel_url': yt.channel_url,
                        'thumbnail_url': yt.thumbnail_url,
                        'publish_date': yt.publish_date.isoformat() if yt.publish_date else None,
                        'keywords': yt.keywords,
                        'scraped_at': datetime.now().isoformat(),
                        'source': 'pytube'
                    }
                    videos.append(video_info)
                    video_count += 1
                    
                    # Respect rate limits
                    time.sleep(self.config.get('rate_limit_pause', 1))
                except Exception as e:
                    logger.error(f"Error getting info for video {video_id}: {e}")
            
            logger.info(f"Retrieved {len(videos)} videos for playlist {playlist_id} via pytube")
        except Exception as e:
            logger.error(f"Error getting playlist videos for {playlist_id}: {e}")
        
        return videos
    
    def search_videos(self, query: str, max_results: Optional[int] = None) -> List[Dict]:
        """
        Search for YouTube videos based on a query.
        
        Args:
            query: Search query
            max_results: Maximum number of videos to retrieve
            
        Returns:
            List of dictionaries containing video information
        """
        if max_results is None:
            max_results = self.config.get('max_results', 50)
        
        videos = []
        
        # This functionality requires the YouTube API
        if self.youtube:
            try:
                next_page_token = None
                video_count = 0
                
                while True:
                    request = self.youtube.search().list(
                        part="snippet",
                        q=query,
                        type="video",
                        maxResults=50,
                        pageToken=next_page_token
                    )
                    response = request.execute()
                    
                    for item in response['items']:
                        video_id = item['id']['videoId']
                        video_info = {
                            'video_id': video_id,
                            'title': item['snippet']['title'],
                            'description': item['snippet']['description'],
                            'published_at': item['snippet']['publishedAt'],
                            'channel_id': item['snippet']['channelId'],
                            'channel_title': item['snippet']['channelTitle'],
                            'thumbnail_url': item['snippet']['thumbnails']['high']['url'] if 'high' in item['snippet']['thumbnails'] else '',
                            'search_query': query,
                            'scraped_at': datetime.now().isoformat(),
                            'source': 'api'
                        }
                        videos.append(video_info)
                        video_count += 1
                        
                        if max_results and video_count >= max_results:
                            break
                    
                    next_page_token = response.get('nextPageToken')
                    
                    if not next_page_token or (max_results and video_count >= max_results):
                        break
                    
                    # Respect YouTube API rate limits
                    time.sleep(self.config.get('rate_limit_pause', 1))
                
                logger.info(f"Retrieved {len(videos)} videos for search query '{query}' via API")
            except Exception as e:
                logger.error(f"API error when searching for '{query}': {e}")
        else:
            logger.error("Search functionality requires YouTube API key")
        
        return videos
    
    def save_data(self, data: Union[List[Dict], Dict], filename: str, data_type: str) -> str:
        """
        Save scraped data to a file.
        
        Args:
            data: Data to save (list of dictionaries or single dictionary)
            filename: Base filename without extension
            data_type: Type of data (videos, comments, etc.)
            
        Returns:
            Path to the saved file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{filename}_{timestamp}"
        
        # Create subdirectory for data type if it doesn't exist
        data_dir = os.path.join(self.output_dir, data_type)
        os.makedirs(data_dir, exist_ok=True)
        
        if self.output_format.lower() == 'json':
            file_path = os.path.join(data_dir, f"{filename}.json")
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        elif self.output_format.lower() == 'csv':
            file_path = os.path.join(data_dir, f"{filename}.csv")
            if isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                df = pd.DataFrame([data])
            df.to_csv(file_path, index=False, encoding='utf-8')
        else:
            logger.error(f"Unsupported output format: {self.output_format}")
            return ""
        
        logger.info(f"Data saved to {file_path}")
        return file_path
    
    def run_channel_scrape(self, channel_id: str, include_comments: bool = False) -> Dict[str, Any]:
        """
        Run a complete scrape of a YouTube channel.
        
        Args:
            channel_id: YouTube channel ID
            include_comments: Whether to scrape comments for each video
            
        Returns:
            Dictionary with paths to saved files
        """
        logger.info(f"Starting channel scrape for {channel_id}")
        
        # Get channel videos
        videos = self.get_channel_videos(channel_id)
        videos_file = self.save_data(videos, f"channel_{channel_id}_videos", "videos")
        
        result = {
            "channel_id": channel_id,
            "videos_count": len(videos),
            "videos_file": videos_file,
            "comments_files": []
        }
        
        # Get comments for each video if requested
        if include_comments and videos:
            for video in videos:
                video_id = video['video_id']
                comments = self.get_video_comments(video_id)
                if comments:
                    comments_file = self.save_data(comments, f"video_{video_id}_comments", "comments")
                    result["comments_files"].append(comments_file)
                
                # Respect rate limits
                time.sleep(self.config.get('rate_limit_pause', 1))
        
        logger.info(f"Completed channel scrape for {channel_id}")
        return result
    
    def run_video_scrape(self, video_id: str, include_comments: bool = True) -> Dict[str, Any]:
        """
        Run a complete scrape of a YouTube video.
        
        Args:
            video_id: YouTube video ID
            include_comments: Whether to scrape comments
            
        Returns:
            Dictionary with paths to saved files
        """
        logger.info(f"Starting video scrape for {video_id}")
        
        # Get video info
        video_info = self.get_video_info(video_id)
        video_file = self.save_data(video_info, f"video_{video_id}_info", "videos")
        
        result = {
            "video_id": video_id,
            "video_file": video_file
        }
        
        # Get comments if requested
        if include_comments:
            comments = self.get_video_comments(video_id)
            if comments:
                comments_file = self.save_data(comments, f"video_{video_id}_comments", "comments")
                result["comments_file"] = comments_file
                result["comments_count"] = len(comments)
        
        logger.info(f"Completed video scrape for {video_id}")
        return result
    
    def run_playlist_scrape(self, playlist_id: str, include_comments: bool = False) -> Dict[str, Any]:
        """
        Run a complete scrape of a YouTube playlist.
        
        Args:
            playlist_id: YouTube playlist ID
            include_comments: Whether to scrape comments for each video
            
        Returns:
            Dictionary with paths to saved files
        """
        logger.info(f"Starting playlist scrape for {playlist_id}")
        
        # Get playlist videos
        videos = self.get_playlist_videos(playlist_id)
        videos_file = self.save_data(videos, f"playlist_{playlist_id}_videos", "playlists")
        
        result = {
            "playlist_id": playlist_id,
            "videos_count": len(videos),
            "videos_file": videos_file,
            "comments_files": []
        }
        
        # Get comments for each video if requested
        if include_comments and videos:
            for video in videos:
                video_id = video['video_id']
                comments = self.get_video_comments(video_id)
                if comments:
                    comments_file = self.save_data(comments, f"video_{video_id}_comments", "comments")
                    result["comments_files"].append(comments_file)
                
                # Respect rate limits
                time.sleep(self.config.get('rate_limit_pause', 1))
        
        logger.info(f"Completed playlist scrape for {playlist_id}")
        return result
    
    def run_search_scrape(self, query: str, include_comments: bool = False) -> Dict[str, Any]:
        """
        Run a complete scrape based on a YouTube search query.
        
        Args:
            query: Search query
            include_comments: Whether to scrape comments for each video
            
        Returns:
            Dictionary with paths to saved files
        """
        logger.info(f"Starting search scrape for '{query}'")
        
        # Get search results
        videos = self.search_videos(query)
        videos_file = self.save_data(videos, f"search_{query.replace(' ', '_')}_videos", "search")
        
        result = {
            "search_query": query,
            "videos_count": len(videos),
            "videos_file": videos_file,
            "comments_files": []
        }
        
        # Get comments for each video if requested
        if include_comments and videos:
            for video in videos:
                video_id = video['video_id']
                comments = self.get_video_comments(video_id)
                if comments:
                    comments_file = self.save_data(comments, f"video_{video_id}_comments", "comments")
                    result["comments_files"].append(comments_file)
                
                # Respect rate limits
                time.sleep(self.config.get('rate_limit_pause', 1))
        
        logger.info(f"Completed search scrape for '{query}'")
        return result


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="YouTube Scraper")
    parser.add_argument("--config", type=str, default="../config.yaml", help="Path to configuration file")
    parser.add_argument("--channel", type=str, help="YouTube channel ID to scrape")
    parser.add_argument("--video", type=str, help="YouTube video ID to scrape")
    parser.add_argument("--playlist", type=str, help="YouTube playlist ID to scrape")
    parser.add_argument("--search", type=str, help="YouTube search query to scrape")
    parser.add_argument("--comments", action="store_true", help="Include comments in scraping")
    
    args = parser.parse_args()
    
    scraper = YouTubeScraper(args.config)
    
    if args.channel:
        result = scraper.run_channel_scrape(args.channel, args.comments)
        print(f"Channel scrape completed: {result}")
    elif args.video:
        result = scraper.run_video_scrape(args.video, args.comments)
        print(f"Video scrape completed: {result}")
    elif args.playlist:
        result = scraper.run_playlist_scrape(args.playlist, args.comments)
        print(f"Playlist scrape completed: {result}")
    elif args.search:
        result = scraper.run_search_scrape(args.search, args.comments)
        print(f"Search scrape completed: {result}")
    else:
        print("No scraping action specified. Use --channel, --video, --playlist, or --search.")
