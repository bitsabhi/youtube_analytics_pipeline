# src/serving/api_server.py
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Optional
from datetime import datetime, timedelta
import asyncio
import logging
from google.cloud import bigquery
from redis import Redis
from pydantic import BaseModel

# Initialize FastAPI app
app = FastAPI(title="YouTube Analytics API")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Models
class VideoMetrics(BaseModel):
    video_id: str
    views: int
    likes: int
    watch_time: float
    unique_users: int
    countries_reached: int
    engagement_rate: float
    avg_watch_time: float


class TimeRangeMetrics(BaseModel):
    start_time: datetime
    end_time: datetime
    metrics: VideoMetrics


# Dependencies
async def get_redis():
    # Initialize Redis connection (should be configured via environment variables)
    redis = Redis(host='localhost', port=6379, db=0)
    try:
        yield redis
    finally:
        redis.close()


async def get_bigquery():
    # Initialize BigQuery client
    client = bigquery.Client()
    try:
        yield client
    finally:
        client.close()


# Routes
@app.get("/metrics/{video_id}", response_model=VideoMetrics)
async def get_realtime_metrics(
        video_id: str,
        redis: Redis = Depends(get_redis)
):
    """Get real-time metrics for a specific video"""
    try:
        # Get cached metrics from Redis
        metrics_key = f"video:{video_id}:metrics"
        cached_metrics = redis.hgetall(metrics_key)

        if not cached_metrics:
            raise HTTPException(status_code=404, detail="Video metrics not found")

        return VideoMetrics(
            video_id=video_id,
            views=int(cached_metrics.get(b'views', 0)),
            likes=int(cached_metrics.get(b'likes', 0)),
            watch_time=float(cached_metrics.get(b'watch_time', 0)),
            unique_users=int(cached_metrics.get(b'unique_users', 0)),
            countries_reached=int(cached_metrics.get(b'countries_reached', 0)),
            engagement_rate=float(cached_metrics.get(b'engagement_rate', 0)),
            avg_watch_time=float(cached_metrics.get(b'avg_watch_time', 0))
        )
    except Exception as e:
        logging.error(f"Error fetching metrics: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/metrics/{video_id}/historical", response_model=TimeRangeMetrics)
async def get_historical_metrics(
        video_id: str,
        start_time: datetime,
        end_time: datetime,
        bq_client: bigquery.Client = Depends(get_bigquery)
):
    """Get historical metrics for a specific video within a time range"""
    try:
        query = f"""
        SELECT
            video_id,
            SUM(unique_views) as views,
            SUM(likes) as likes,
            SUM(total_watch_time) as watch_time,
            SUM(unique_users) as unique_users,
            MAX(countries_reached) as countries_reached,
            AVG(engagement_rate) as engagement_rate,
            AVG(avg_watch_time) as avg_watch_time
        FROM
            `your-project.youtube_analytics.hourly_metrics`
        WHERE
            video_id = @video_id
            AND timestamp BETWEEN @start_time AND @end_time
        GROUP BY
            video_id
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("video_id", "STRING", video_id),
                bigquery.ScalarQueryParameter("start_time", "TIMESTAMP", start_time),
                bigquery.ScalarQueryParameter("end_time", "TIMESTAMP", end_time),
            ]
        )

        results = bq_client.query(query, job_config=job_config).result()

        if results.total_rows == 0:
            raise HTTPException(status_code=404, detail="No metrics found for the specified time range")

        row = next(results)
        metrics = VideoMetrics(
            video_id=video_id,
            views=row.views,
            likes=row.likes,
            watch_time=row.watch_time,
            unique_users=row.unique_users,
            countries_reached=row.countries_reached,
            engagement_rate=row.engagement_rate,
            avg_watch_time=row.avg_watch_time
        )

        return TimeRangeMetrics(
            start_time=start_time,
            end_time=end_time,
            metrics=metrics
        )

    except Exception as e:
        logging.error(f"Error fetching historical metrics: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.utcnow()}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
