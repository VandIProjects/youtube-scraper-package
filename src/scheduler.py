#!/usr/bin/env python3
"""
YouTube Scraper Scheduler

This module provides functionality to schedule and automate YouTube scraping tasks.
It uses APScheduler to run scraping jobs at specified intervals.
"""

import os
import sys
import logging
import argparse
import time
from datetime import datetime
import yaml
from typing import Dict, List, Any, Optional
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor

# Add parent directory to path to import scraper and utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.scraper import YouTubeScraper
from src.utils import load_config, setup_logging, create_directory_structure

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("youtube_scheduler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("youtube_scheduler")

class YouTubeScraperScheduler:
    """Scheduler for YouTube scraping tasks."""
    
    def __init__(self, config_path: str = "../config.yaml"):
        """
        Initialize the scheduler with configuration.
        
        Args:
            config_path: Path to the configuration YAML file
        """
        self.config = load_config(config_path)
        self.config_path = config_path
        
        # Set up logging
        log_dir = self.config.get('log_directory', 'logs')
        log_level = self.config.get('log_level', 'INFO')
        setup_logging(log_dir, log_level)
        
        # Create directory structure
        output_dir = self.config.get('output_directory', 'data')
        create_directory_structure(output_dir)
        
        # Initialize scheduler
        self._setup_scheduler()
        
        # Initialize scraper
        self.scraper = YouTubeScraper(config_path)
    
    def _setup_scheduler(self) -> None:
        """Set up the APScheduler with job stores and executors."""
        jobstores = {
            'default': SQLAlchemyJobStore(url='sqlite:///jobs.sqlite')
        }
        
        executors = {
            'default': ThreadPoolExecutor(self.config.get('max_threads', 20)),
            'processpool': ProcessPoolExecutor(self.config.get('max_processes', 5))
        }
        
        job_defaults = {
            'coalesce': False,
            'max_instances': 3,
            'misfire_grace_time': 3600  # 1 hour
        }
        
        self.scheduler = BackgroundScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone=self.config.get('timezone', 'UTC')
        )
    
    def start(self) -> None:
        """Start the scheduler."""
        self.scheduler.start()
        logger.info("Scheduler started")
        
        # Add scheduled jobs from config
        self._add_scheduled_jobs_from_config()
    
    def shutdown(self) -> None:
        """Shutdown the scheduler."""
        self.scheduler.shutdown()
        logger.info("Scheduler shutdown")
    
    def _add_scheduled_jobs_from_config(self) -> None:
        """Add scheduled jobs from the configuration file."""
        scheduled_jobs = self.config.get('scheduled_jobs', [])
        
        for job in scheduled_jobs:
            job_type = job.get('type')
            job_id = job.get('id', f"{job_type}_{datetime.now().strftime('%Y%m%d%H%M%S')}")
            
            # Schedule based on job type
            if job_type == 'channel':
                self.schedule_channel_scrape(
                    channel_id=job.get('channel_id'),
                    include_comments=job.get('include_comments', False),
                    schedule_type=job.get('schedule_type', 'interval'),
                    interval=job.get('interval'),
                    cron=job.get('cron'),
                    job_id=job_id
                )
            elif job_type == 'video':
                self.schedule_video_scrape(
                    video_id=job.get('video_id'),
                    include_comments=job.get('include_comments', True),
                    schedule_type=job.get('schedule_type', 'interval'),
                    interval=job.get('interval'),
                    cron=job.get('cron'),
                    job_id=job_id
                )
            elif job_type == 'playlist':
                self.schedule_playlist_scrape(
                    playlist_id=job.get('playlist_id'),
                    include_comments=job.get('include_comments', False),
                    schedule_type=job.get('schedule_type', 'interval'),
                    interval=job.get('interval'),
                    cron=job.get('cron'),
                    job_id=job_id
                )
            elif job_type == 'search':
                self.schedule_search_scrape(
                    query=job.get('query'),
                    include_comments=job.get('include_comments', False),
                    schedule_type=job.get('schedule_type', 'interval'),
                    interval=job.get('interval'),
                    cron=job.get('cron'),
                    job_id=job_id
                )
            else:
                logger.error(f"Unknown job type: {job_type}")
    
    def _parse_schedule(self, schedule_type: str, interval: Optional[Dict] = None, cron: Optional[Dict] = None):
        """
        Parse schedule configuration and return the appropriate trigger.
        
        Args:
            schedule_type: Type of schedule ('interval' or 'cron')
            interval: Interval configuration (e.g., {'hours': 1})
            cron: Cron configuration (e.g., {'hour': '*/2'})
            
        Returns:
            APScheduler trigger
        """
        if schedule_type == 'interval':
            if not interval:
                # Default to daily
                interval = {'days': 1}
            return IntervalTrigger(**interval)
        elif schedule_type == 'cron':
            if not cron:
                # Default to midnight every day
                cron = {'hour': '0', 'minute': '0'}
            return CronTrigger(**cron)
        else:
            logger.error(f"Unknown schedule type: {schedule_type}")
            # Default to daily
            return IntervalTrigger(days=1)
    
    def schedule_channel_scrape(self, channel_id: str, include_comments: bool = False,
                               schedule_type: str = 'interval', interval: Optional[Dict] = None,
                               cron: Optional[Dict] = None, job_id: Optional[str] = None) -> str:
        """
        Schedule a channel scraping job.
        
        Args:
            channel_id: YouTube channel ID
            include_comments: Whether to include comments in the scrape
            schedule_type: Type of schedule ('interval' or 'cron')
            interval: Interval configuration (e.g., {'hours': 1})
            cron: Cron configuration (e.g., {'hour': '*/2'})
            job_id: Optional job ID
            
        Returns:
            Job ID
        """
        if not job_id:
            job_id = f"channel_{channel_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        trigger = self._parse_schedule(schedule_type, interval, cron)
        
        self.scheduler.add_job(
            self._run_channel_scrape,
            trigger=trigger,
            args=[channel_id, include_comments],
            id=job_id,
            name=f"Channel Scrape: {channel_id}"
        )
        
        logger.info(f"Scheduled channel scrape for {channel_id} with job ID {job_id}")
        return job_id
    
    def schedule_video_scrape(self, video_id: str, include_comments: bool = True,
                             schedule_type: str = 'interval', interval: Optional[Dict] = None,
                             cron: Optional[Dict] = None, job_id: Optional[str] = None) -> str:
        """
        Schedule a video scraping job.
        
        Args:
            video_id: YouTube video ID
            include_comments: Whether to include comments in the scrape
            schedule_type: Type of schedule ('interval' or 'cron')
            interval: Interval configuration (e.g., {'hours': 1})
            cron: Cron configuration (e.g., {'hour': '*/2'})
            job_id: Optional job ID
            
        Returns:
            Job ID
        """
        if not job_id:
            job_id = f"video_{video_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        trigger = self._parse_schedule(schedule_type, interval, cron)
        
        self.scheduler.add_job(
            self._run_video_scrape,
            trigger=trigger,
            args=[video_id, include_comments],
            id=job_id,
            name=f"Video Scrape: {video_id}"
        )
        
        logger.info(f"Scheduled video scrape for {video_id} with job ID {job_id}")
        return job_id
    
    def schedule_playlist_scrape(self, playlist_id: str, include_comments: bool = False,
                                schedule_type: str = 'interval', interval: Optional[Dict] = None,
                                cron: Optional[Dict] = None, job_id: Optional[str] = None) -> str:
        """
        Schedule a playlist scraping job.
        
        Args:
            playlist_id: YouTube playlist ID
            include_comments: Whether to include comments in the scrape
            schedule_type: Type of schedule ('interval' or 'cron')
            interval: Interval configuration (e.g., {'hours': 1})
            cron: Cron configuration (e.g., {'hour': '*/2'})
            job_id: Optional job ID
            
        Returns:
            Job ID
        """
        if not job_id:
            job_id = f"playlist_{playlist_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        trigger = self._parse_schedule(schedule_type, interval, cron)
        
        self.scheduler.add_job(
            self._run_playlist_scrape,
            trigger=trigger,
            args=[playlist_id, include_comments],
            id=job_id,
            name=f"Playlist Scrape: {playlist_id}"
        )
        
        logger.info(f"Scheduled playlist scrape for {playlist_id} with job ID {job_id}")
        return job_id
    
    def schedule_search_scrape(self, query: str, include_comments: bool = False,
                              schedule_type: str = 'interval', interval: Optional[Dict] = None,
                              cron: Optional[Dict] = None, job_id: Optional[str] = None) -> str:
        """
        Schedule a search scraping job.
        
        Args:
            query: Search query
            include_comments: Whether to include comments in the scrape
            schedule_type: Type of schedule ('interval' or 'cron')
            interval: Interval configuration (e.g., {'hours': 1})
            cron: Cron configuration (e.g., {'hour': '*/2'})
            job_id: Optional job ID
            
        Returns:
            Job ID
        """
        if not job_id:
            job_id = f"search_{query.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        trigger = self._parse_schedule(schedule_type, interval, cron)
        
        self.scheduler.add_job(
            self._run_search_scrape,
            trigger=trigger,
            args=[query, include_comments],
            id=job_id,
            name=f"Search Scrape: {query}"
        )
        
        logger.info(f"Scheduled search scrape for '{query}' with job ID {job_id}")
        return job_id
    
    def _run_channel_scrape(self, channel_id: str, include_comments: bool) -> None:
        """
        Run a channel scrape job.
        
        Args:
            channel_id: YouTube channel ID
            include_comments: Whether to include comments in the scrape
        """
        logger.info(f"Running channel scrape for {channel_id}")
        try:
            result = self.scraper.run_channel_scrape(channel_id, include_comments)
            logger.info(f"Channel scrape completed: {result}")
        except Exception as e:
            logger.error(f"Error in channel scrape for {channel_id}: {e}")
    
    def _run_video_scrape(self, video_id: str, include_comments: bool) -> None:
        """
        Run a video scrape job.
        
        Args:
            video_id: YouTube video ID
            include_comments: Whether to include comments in the scrape
        """
        logger.info(f"Running video scrape for {video_id}")
        try:
            result = self.scraper.run_video_scrape(video_id, include_comments)
            logger.info(f"Video scrape completed: {result}")
        except Exception as e:
            logger.error(f"Error in video scrape for {video_id}: {e}")
    
    def _run_playlist_scrape(self, playlist_id: str, include_comments: bool) -> None:
        """
        Run a playlist scrape job.
        
        Args:
            playlist_id: YouTube playlist ID
            include_comments: Whether to include comments in the scrape
        """
        logger.info(f"Running playlist scrape for {playlist_id}")
        try:
            result = self.scraper.run_playlist_scrape(playlist_id, include_comments)
            logger.info(f"Playlist scrape completed: {result}")
        except Exception as e:
            logger.error(f"Error in playlist scrape for {playlist_id}: {e}")
    
    def _run_search_scrape(self, query: str, include_comments: bool) -> None:
        """
        Run a search scrape job.
        
        Args:
            query: Search query
            include_comments: Whether to include comments in the scrape
        """
        logger.info(f"Running search scrape for '{query}'")
        try:
            result = self.scraper.run_search_scrape(query, include_comments)
            logger.info(f"Search scrape completed: {result}")
        except Exception as e:
            logger.error(f"Error in search scrape for '{query}': {e}")
    
    def list_jobs(self) -> List[Dict]:
        """
        List all scheduled jobs.
        
        Returns:
            List of job information dictionaries
        """
        jobs = []
        for job in self.scheduler.get_jobs():
            job_info = {
                'id': job.id,
                'name': job.name,
                'next_run_time': job.next_run_time.strftime('%Y-%m-%d %H:%M:%S') if job.next_run_time else None,
                'trigger': str(job.trigger)
            }
            jobs.append(job_info)
        
        return jobs
    
    def remove_job(self, job_id: str) -> bool:
        """
        Remove a scheduled job.
        
        Args:
            job_id: Job ID to remove
            
        Returns:
            True if job was removed, False otherwise
        """
        try:
            self.scheduler.remove_job(job_id)
            logger.info(f"Removed job {job_id}")
            return True
        except Exception as e:
            logger.error(f"Error removing job {job_id}: {e}")
            return False
    
    def pause_job(self, job_id: str) -> bool:
        """
        Pause a scheduled job.
        
        Args:
            job_id: Job ID to pause
            
        Returns:
            True if job was paused, False otherwise
        """
        try:
            self.scheduler.pause_job(job_id)
            logger.info(f"Paused job {job_id}")
            return True
        except Exception as e:
            logger.error(f"Error pausing job {job_id}: {e}")
            return False
    
    def resume_job(self, job_id: str) -> bool:
        """
        Resume a paused job.
        
        Args:
            job_id: Job ID to resume
            
        Returns:
            True if job was resumed, False otherwise
        """
        try:
            self.scheduler.resume_job(job_id)
            logger.info(f"Resumed job {job_id}")
            return True
        except Exception as e:
            logger.error(f"Error resuming job {job_id}: {e}")
            return False
    
    def run_job_now(self, job_id: str) -> bool:
        """
        Run a job immediately.
        
        Args:
            job_id: Job ID to run
            
        Returns:
            True if job was triggered, False otherwise
        """
        try:
            job = self.scheduler.get_job(job_id)
            if job:
                job_func = job.func
                job_args = job.args
                job_kwargs = job.kwargs
                
                # Run the job function with its arguments
                job_func(*job_args, **job_kwargs)
                
                logger.info(f"Manually triggered job {job_id}")
                return True
            else:
                logger.error(f"Job {job_id} not found")
                return False
        except Exception as e:
            logger.error(f"Error running job {job_id}: {e}")
            return False


def main():
    """Main function to run the scheduler from command line."""
    parser = argparse.ArgumentParser(description="YouTube Scraper Scheduler")
    parser.add_argument("--config", type=str, default="../config.yaml", help="Path to configuration file")
    parser.add_argument("--action", type=str, choices=["start", "list", "run", "remove", "pause", "resume"], 
                        default="start", help="Action to perform")
    parser.add_argument("--job-id", type=str, help="Job ID for actions that require it")
    parser.add_argument("--channel", type=str, help="YouTube channel ID to schedule scraping")
    parser.add_argument("--video", type=str, help="YouTube video ID to schedule scraping")
    parser.add_argument("--playlist", type=str, help="YouTube playlist ID to schedule scraping")
    parser.add_argument("--search", type=str, help="YouTube search query to schedule scraping")
    parser.add_argument("--comments", action="store_true", help="Include comments in scraping")
    parser.add_argument("--schedule-type", type=str, choices=["interval", "cron"], default="interval", 
                        help="Type of schedule")
    parser.add_argument("--interval-unit", type=str, choices=["seconds", "minutes", "hours", "days", "weeks"], 
                        default="days", help="Interval unit for interval schedule")
    parser.add_argument("--interval-value", type=int, default=1, 
                        help="Interval value for interval schedule")
    parser.add_argument("--cron-minute", type=str, default="0", 
                        help="Minute field for cron schedule")
    parser.add_argument("--cron-hour", type=str, default="0", 
                        help="Hour field for cron schedule")
    parser.add_argument("--cron-day", type=str, default="*", 
                        help="Day of month field for cron schedule")
    parser.add_argument("--cron-month", type=str, default="*", 
                        help="Month field for cron schedule")
    parser.add_argument("--cron-day-of-week", type=str, default="*", 
                        help="Day of week field for cron schedule")
    
    args = parser.parse_args()
    
    # Initialize scheduler
    scheduler = YouTubeScraperScheduler(args.config)
    
    if args.action == "start":
        # Start the scheduler
        scheduler.start()
        
        # Add new job if specified
        if args.channel or args.video or args.playlist or args.search:
            # Prepare schedule parameters
            if args.schedule_type == "interval":
                interval = {args.interval_unit: args.interval_value}
                cron = None
            else:  # cron
                interval = None
                cron = {
                    "minute": args.cron_minute,
                    "hour": args.cron_hour,
                    "day": args.cron_day,
                    "month": args.cron_month,
                    "day_of_week": args.cron_day_of_week
                }
            
            # Schedule the job
            if args.channel:
                scheduler.schedule_channel_scrape(
                    channel_id=args.channel,
                    include_comments=args.comments,
                    schedule_type=args.schedule_type,
                    interval=interval,
                    cron=cron
                )
            elif args.video:
                scheduler.schedule_video_scrape(
                    video_id=args.video,
                    include_comments=args.comments,
                    schedule_type=args.schedule_type,
                    interval=interval,
                    cron=cron
                )
            elif args.playlist:
                scheduler.schedule_playlist_scrape(
                    playlist_id=args.playlist,
                    include_comments=args.comments,
                    schedule_type=args.schedule_type,
                    interval=interval,
                    cron=cron
                )
            elif args.search:
                scheduler.schedule_search_scrape(
                    query=args.search,
                    include_comments=args.comments,
                    schedule_type=args.schedule_type,
                    interval=interval,
                    cron=cron
                )
        
        # Keep the scheduler running
        try:
            print("Scheduler is running. Press Ctrl+C to exit.")
            while True:
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            scheduler.shutdown()
    
    elif args.action == "list":
        # Start the scheduler to access the job store
        scheduler.start()
        
        # List all jobs
        jobs = scheduler.list_jobs()
        print(f"Scheduled Jobs ({len(jobs)}):")
        for job in jobs:
            print(f"ID: {job['id']}")
            print(f"Name: {job['name']}")
            print(f"Next Run: {job['next_run_time']}")
            print(f"Trigger: {job['trigger']}")
            print("-" * 50)
        
        scheduler.shutdown()
    
    elif args.action == "run":
        if not args.job_id:
            print("Error: --job-id is required for 'run' action")
            return
        
        # Start the scheduler to access the job store
        scheduler.start()
        
        # Run the job
        success = scheduler.run_job_now(args.job_id)
        if success:
            print(f"Job {args.job_id} triggered successfully")
        else:
            print(f"Failed to trigger job {args.job_id}")
        
        scheduler.shutdown()
    
    elif args.action == "remove":
        if not args.job_id:
            print("Error: --job-id is required for 'remove' action")
            return
        
        # Start the scheduler to access the job store
        scheduler.start()
        
        # Remove the job
        success = scheduler.remove_job(args.job_id)
        if success:
            print(f"Job {args.job_id} removed successfully")
        else:
            print(f"Failed to remove job {args.job_id}")
        
        scheduler.shutdown()
    
    elif args.action == "pause":
        if not args.job_id:
            print("Error: --job-id is required for 'pause' action")
            return
        
        # Start the scheduler to access the job store
        scheduler.start()
        
        # Pause the job
        success = scheduler.pause_job(args.job_id)
        if success:
            print(f"Job {args.job_id} paused successfully")
        else:
            print(f"Failed to pause job {args.job_id}")
        
        scheduler.shutdown()
    
    elif args.action == "resume":
        if not args.job_id:
            print("Error: --job-id is required for 'resume' action")
            return
        
        # Start the scheduler to access the job store
        scheduler.start()
        
        # Resume the job
        success = scheduler.resume_job(args.job_id)
        if success:
            print(f"Job {args.job_id} resumed successfully")
        else:
            print(f"Failed to resume job {args.job_id}")
        
        scheduler.shutdown()


if __name__ == "__main__":
    main()
