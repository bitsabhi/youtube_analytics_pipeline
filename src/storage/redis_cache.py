# src/storage/redis_cache.py
from redis import Redis
from typing import Dict, Optional, List, Any, Tuple
import json
import logging
from datetime import datetime, timedelta


class RedisCache:
    def __init__(
            self,
            host: str = 'localhost',
            port: int = 6379,
            db: int = 0,
            prefix: str = 'yt:',
            ttl: int = 3600
    ):
        self.redis = Redis(host=host, port=port, db=db)
        self.prefix = prefix
        self.default_ttl = ttl
        self.logger = logging.getLogger(__name__)

    def _get_key(self, key: str) -> str:
        """Prepend prefix to key"""
        return f"{self.prefix}{key}"

    def set_video_metrics(self, video_id: str, metrics: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        """
        Set real-time metrics for a video
        Returns True if successful, False otherwise
        """
        try:
            key = self._get_key(f"video:{video_id}:metrics")
            pipeline = self.redis.pipeline()

            # Convert all values to strings for Redis storage
            metrics_str = {k: str(v) for k, v in metrics.items()}

            pipeline.hmset(key, metrics_str)
            pipeline.expire(key, ttl or self.default_ttl)
            pipeline.execute()
            return True
        except Exception as e:
            self.logger.error(f"Error setting video metrics: {str(e)}")
            return False

    def get_video_metrics(self, video_id: str) -> Optional[Dict[str, Any]]:
        """
        Get real-time metrics for a video
        Returns None if not found
        """
        try:
            key = self._get_key(f"video:{video_id}:metrics")
            metrics = self.redis.hgetall(key)

            if not metrics:
                return None

            # Convert string values back to appropriate types
            return {
                'views': int(metrics.get(b'views', 0)),
                'likes': int(metrics.get(b'likes', 0)),
                'watch_time': float(metrics.get(b'watch_time', 0)),
                'unique_users': int(metrics.get(b'unique_users', 0)),
                'countries_reached': int(metrics.get(b'countries_reached', 0)),
                'engagement_rate': float(metrics.get(b'engagement_rate', 0)),
                'avg_watch_time': float(metrics.get(b'avg_watch_time', 0))
            }
        except Exception as e:
            self.logger.error(f"Error getting video metrics: {str(e)}")
            return None

    def increment_metrics(self, video_id: str, metrics: Dict[str, int]) -> bool:
        """
        Increment specific metrics for a video
        """
        try:
            key = self._get_key(f"video:{video_id}:metrics")
            pipeline = self.redis.pipeline()

            for metric, value in metrics.items():
                pipeline.hincrby(key, metric, value)

            pipeline.execute()
            return True
        except Exception as e:
            self.logger.error(f"Error incrementing metrics: {str(e)}")
            return False

    def store_event_batch(self, events: List[Dict], batch_id: str) -> bool:
        """
        Store a batch of events temporarily
        """
        try:
            key = self._get_key(f"batch:{batch_id}")
            pipeline = self.redis.pipeline()

            # Store events as JSON string
            pipeline.set(key, json.dumps(events))
            pipeline.expire(key, 300)  # 5 minutes TTL for batch

            pipeline.execute()
            return True
        except Exception as e:
            self.logger.error(f"Error storing event batch: {str(e)}")
            return False

    def get_and_delete_batch(self, batch_id: str) -> Optional[List[Dict]]:
        """
        Retrieve and delete a batch of events
        """
        try:
            key = self._get_key(f"batch:{batch_id}")

            # Get and delete in single transaction
            pipeline = self.redis.pipeline()
            pipeline.get(key)
            pipeline.delete(key)
            results = pipeline.execute()

            if not results[0]:
                return None

            return json.loads(results[0])
        except Exception as e:
            self.logger.error(f"Error retrieving batch: {str(e)}")
            return None

    def add_to_processing_window(self, video_id: str, event: Dict, window_size: int = 300) -> bool:
        """
        Add event to sliding window for processing
        """
        try:
            key = self._get_key(f"window:{video_id}")
            timestamp = event.get('timestamp', datetime.utcnow().timestamp())

            # Add to sorted set with timestamp as score
            self.redis.zadd(key, {json.dumps(event): timestamp})

            # Remove old events outside window
            min_timestamp = datetime.utcnow().timestamp() - window_size
            self.redis.zremrangebyscore(key, '-inf', min_timestamp)

            return True
        except Exception as e:
            self.logger.error(f"Error adding to processing window: {str(e)}")
            return False

    def get_window_events(self, video_id: str) -> List[Dict]:
        """
        Get all events in current processing window
        """
        try:
            key = self._get_key(f"window:{video_id}")
            events = self.redis.zrange(key, 0, -1)
            return [json.loads(event) for event in events]
        except Exception as e:
            self.logger.error(f"Error getting window events: {str(e)}")
            return []

    def cleanup_old_data(self) -> Tuple[int, int]:
        """
        Cleanup expired keys and old data
        Returns tuple of (cleaned_keys, errors)
        """
        try:
            cleaned = 0
            errors = 0

            # Scan for old keys
            for key in self.redis.scan_iter(f"{self.prefix}*"):
                try:
                    # Check if key has TTL
                    if not self.redis.ttl(key):
                        self.redis.delete(key)
                        cleaned += 1
                except Exception:
                    errors += 1

            return cleaned, errors
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")
            return 0, 0

    def healthcheck(self) -> bool:
        """
        Check if Redis connection is healthy
        """
        try:
            return bool(self.redis.ping())
        except Exception:
            return False
