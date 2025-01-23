# src/serving/metrics_service.py
from fastapi import FastAPI, HTTPException
from redis import Redis
from typing import Dict, Optional, List
from datetime import datetime, timedelta
import logging
from ..storage.bigquery_handler import BigQueryHandler
from ..storage.redis_cache import RedisCache
from pydantic import BaseModel


class MetricsService:
    def __init__(
            self,
            redis_client: Optional[Redis] = None,
            bigquery_handler: Optional[BigQueryHandler] = None,
            config: Optional[Dict] = None
    ):
        self.redis = redis_client or RedisCache()
        self.bigquery = bigquery_handler
        self.config = config or {}
        self.logger = logging.getLogger(__name__)

    async def get_video_metrics(self, video_id: str) -> Dict:
        """
        Get real-time metrics for a video from Redis
        Falls back to BigQuery if not in cache
        """
        try:
            # Try to get from Redis first
            metrics = self.redis.get_video_metrics(video_id)

            if metrics:
                return metrics

            # If not in Redis, fetch from BigQuery and cache
            if self.bigquery:
                end_time = datetime.utcnow()
                start_time = end_time - timedelta(minutes=5)

                metrics = self.bigquery.get_video_metrics(
                    video_id,
                    start_time,
                    end_time,
                    self.config.get('bigquery', {}).get('tables', {}).get('events', 'raw_events')
                )

                if metrics:
                    # Cache the metrics
                    self.redis.set_video_metrics(video_id, metrics)
                    return metrics

            raise HTTPException(status_code=404, detail="Video metrics not found")

        except Exception as e:
            self.logger.error(f"Error getting video metrics: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal server error")

    async def update_metrics(self, video_id: str, metrics: Dict) -> bool:
        """
        Update real-time metrics in Redis
        """
        try:
            return self.redis.set_video_metrics(video_id, metrics)
        except Exception as e:
            self.logger.error(f"Error updating metrics: {str(e)}")
            return False

    async def get_historical_metrics(
            self,
            video_id: str,
            start_time: datetime,
            end_time: datetime
    ) -> Dict:
        """
        Get historical metrics from BigQuery
        """
        try:
            if not self.bigquery:
                raise HTTPException(status_code=500, detail="BigQuery handler not configured")

            metrics = self.bigquery.get_video_metrics(
                video_id,
                start_time,
                end_time,
                self.config.get('bigquery', {}).get('tables', {}).get('aggregated', 'hourly_metrics')
            )

            if not metrics:
                raise HTTPException(status_code=404, detail="Historical metrics not found")

            return metrics

        except HTTPException:
            raise
        except Exception as e:
            self.logger.error(f"Error getting historical metrics: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal server error")

    async def get_trending_videos(self, limit: int = 10) -> List[Dict]:
        """
        Get trending videos based on recent engagement
        """
        try:
            # Get all video metrics from Redis
            pipeline = self.redis.redis.pipeline()
            video_keys = self.redis.redis.keys(f"{self.redis.prefix}video:*:metrics")

            if not video_keys:
                return []

            for key in video_keys:
                pipeline.hgetall(key)

            results = pipeline.execute()

            # Process and sort videos by engagement
            videos = []
            for key, metrics in zip(video_keys, results):
                if metrics:
                    video_id = key.decode('utf-8').split(':')[1]
                    videos.append({
                        'video_id': video_id,
                        'views': int(metrics.get(b'views', 0)),
                        'likes': int(metrics.get(b'likes', 0)),
                        'engagement_rate': float(metrics.get(b'engagement_rate', 0))
                    })

            # Sort by engagement rate
            videos.sort(key=lambda x: x['engagement_rate'], reverse=True)
            return videos[:limit]

        except Exception as e:
            self.logger.error(f"Error getting trending videos: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal server error")

    async def aggregate_metrics(
            self,
            video_id: str,
            time_window: timedelta = timedelta(minutes=5)
    ) -> Optional[Dict]:
        """
        Aggregate metrics for a specific time window
        """
        try:
            end_time = datetime.utcnow()
            start_time = end_time - time_window

            if not self.bigquery:
                return None

            return self.bigquery.execute_query(f"""
                SELECT
                    video_id,
                    COUNT(DISTINCT user_id) as unique_viewers,
                    COUNT(CASE WHEN event_type = 'like' THEN 1 END) as likes,
                    SUM(watch_time) as total_watch_time,
                    COUNT(DISTINCT country_code) as countries_reached
                FROM `{self.bigquery.project_id}.{self.bigquery.dataset_id}.raw_events`
                WHERE video_id = @video_id
                AND event_timestamp BETWEEN @start_time AND @end_time
                GROUP BY video_id
            """, {
                'video_id': video_id,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat()
            })

        except Exception as e:
            self.logger.error(f"Error aggregating metrics: {str(e)}")
            return None

    async def healthcheck(self) -> Dict[str, str]:
        """
        Check health of dependent services
        """
        health = {
            'redis': 'healthy' if self.redis.healthcheck() else 'unhealthy',
            'bigquery': 'healthy'
        }

        try:
            if self.bigquery:
                self.bigquery.execute_query("SELECT 1")
        except Exception:
            health['bigquery'] = 'unhealthy'

        return health
