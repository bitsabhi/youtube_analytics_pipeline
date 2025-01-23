# src/ingestion/pubsub_consumer.py
from google.cloud import pubsub_v1
import json
from typing import Callable


class PubSubConsumer:
    def __init__(self, project_id: str, subscription_name: str):
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(
            project_id, subscription_name
        )

    def consume_messages(self, callback: Callable):
        def callback_wrapper(message):
            try:
                data = json.loads(message.data.decode("utf-8"))
                callback(data)
                message.ack()
            except Exception as e:
                print(f"Error processing message: {e}")
                message.nack()

        streaming_pull_future = self.subscriber.subscribe(
            self.subscription_path, callback=callback_wrapper
        )
        return streaming_pull_future


# src/processing/stream_processor.py
from apache_beam import Pipeline, WindowInto, GroupByKey
from apache_beam.transforms import window
import apache_beam as beam


class StreamProcessor:
    def __init__(self, window_size: int = 300):  # 5 minutes in seconds
        self.window_size = window_size

    def build_pipeline(self):
        pipeline = Pipeline()

        processed = (
                pipeline
                | "Read from PubSub" >> beam.io.ReadFromPubSub(topic="projects/your-project/topics/your-topic")
                | "Parse JSON" >> beam.Map(lambda x: json.loads(x))
                | "Add Timestamp" >> beam.Map(lambda x: beam.window.TimestampedValue(x, x['timestamp']))
                | "Window" >> WindowInto(window.SlidingWindows(self.window_size, 60))
                | "Extract Key-Value" >> beam.Map(lambda x: (x['video_id'], x))
                | "Group by Video" >> GroupByKey()
        )

        return pipeline


# src/storage/bigquery_handler.py
from google.cloud import bigquery
from typing import List, Dict


class BigQueryHandler:
    def __init__(self, project_id: str, dataset_id: str):
        self.client = bigquery.Client(project=project_id)
        self.dataset_id = dataset_id

    def create_table_if_not_exists(self, table_id: str, schema: List[bigquery.SchemaField]):
        dataset_ref = self.client.dataset(self.dataset_id)
        table_ref = dataset_ref.table(table_id)

        try:
            self.client.get_table(table_ref)
        except Exception:
            table = bigquery.Table(table_ref, schema=schema)
            self.client.create_table(table)

    def insert_rows(self, table_id: str, rows: List[Dict]):
        table_ref = f"{self.dataset_id}.{table_id}"
        errors = self.client.insert_rows_json(table_ref, rows)
        if errors:
            raise Exception(f"Errors inserting rows: {errors}")


# src/serving/metrics_service.py
from fastapi import FastAPI, HTTPException
from redis import Redis
from typing import Dict, Optional

app = FastAPI()
redis_client = Redis(host='localhost', port=6379, db=0)


class MetricsService:
    def __init__(self):
        self.redis_client = redis_client

    async def get_video_metrics(self, video_id: str) -> Dict:
        # Get real-time metrics from Redis
        real_time_metrics = self.redis_client.hgetall(f"video:{video_id}")

        if not real_time_metrics:
            raise HTTPException(status_code=404, detail="Video metrics not found")

        return {
            "video_id": video_id,
            "views": int(real_time_metrics.get(b'views', 0)),
            "likes": int(real_time_metrics.get(b'likes', 0)),
            "watch_time": float(real_time_metrics.get(b'watch_time', 0))
        }

    async def update_metrics(self, video_id: str, metrics: Dict):
        # Update real-time metrics in Redis
        pipeline = self.redis_client.pipeline()
        pipeline.hmset(f"video:{video_id}", metrics)
        pipeline.expire(f"video:{video_id}", 3600)  # TTL: 1 hour
        pipeline.execute()
