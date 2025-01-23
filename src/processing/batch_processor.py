# src/processing/batch_processor.py
from typing import List, Dict
import pandas as pd
from google.cloud import bigquery
import logging
from datetime import datetime, timedelta


class BatchProcessor:
    def __init__(self, bq_client: bigquery.Client, project_id: str, dataset_id: str):
        self.bq_client = bq_client
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.logger = logging.getLogger(__name__)

    def process_hourly_aggregation(self, table_id: str, timestamp: datetime) -> Dict:
        """
        Process hourly aggregation for video metrics
        """
        hour_start = timestamp.replace(minute=0, second=0, microsecond=0)
        hour_end = hour_start + timedelta(hours=1)

        query = f"""
        SELECT
            video_id,
            COUNT(DISTINCT CASE WHEN event_type = 'view' THEN user_id END) as unique_views,
            COUNT(CASE WHEN event_type = 'like' THEN 1 END) as likes,
            SUM(CASE WHEN event_type = 'view' THEN watch_time ELSE 0 END) as total_watch_time,
            COUNT(DISTINCT user_id) as unique_users,
            COUNT(DISTINCT country_code) as countries_reached
        FROM
            `{self.project_id}.{self.dataset_id}.{table_id}`
        WHERE
            event_timestamp >= TIMESTAMP('{hour_start.isoformat()}')
            AND event_timestamp < TIMESTAMP('{hour_end.isoformat()}')
        GROUP BY
            video_id
        """

        try:
            df = self.bq_client.query(query).to_dataframe()
            return self._process_aggregation_results(df, hour_start)
        except Exception as e:
            self.logger.error(f"Error in hourly aggregation: {str(e)}")
            raise

    def _process_aggregation_results(self, df: pd.DataFrame, timestamp: datetime) -> Dict:
        """
        Process the aggregation results and prepare metrics
        """
        metrics = {}
        for _, row in df.iterrows():
            metrics[row['video_id']] = {
                'timestamp': timestamp.isoformat(),
                'metrics': {
                    'unique_views': int(row['unique_views']),
                    'likes': int(row['likes']),
                    'total_watch_time': float(row['total_watch_time']),
                    'unique_users': int(row['unique_users']),
                    'countries_reached': int(row['countries_reached']),
                    'engagement_rate': float(row['likes']) / float(row['unique_views']) if row[
                                                                                               'unique_views'] > 0 else 0,
                    'avg_watch_time': float(row['total_watch_time']) / float(row['unique_views']) if row[
                                                                                                         'unique_views'] > 0 else 0
                }
            }
        return metrics

    def save_aggregations(self, table_id: str, metrics: Dict):
        """
        Save aggregated metrics to BigQuery
        """
        rows_to_insert = []
        for video_id, data in metrics.items():
            rows_to_insert.append({
                'video_id': video_id,
                'timestamp': data['timestamp'],
                **data['metrics']
            })

        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"

        try:
            errors = self.bq_client.insert_rows_json(table_ref, rows_to_insert)
            if errors:
                self.logger.error(f"Errors inserting rows: {errors}")
                raise Exception(f"Failed to insert rows: {errors}")
        except Exception as e:
            self.logger.error(f"Error saving aggregations: {str(e)}")
            raise

    def run_daily_cleanup(self, raw_table_id: str, retention_days: int):
        """
        Clean up old raw data based on retention policy
        """
        retention_date = datetime.utcnow() - timedelta(days=retention_days)
        query = f"""
        DELETE FROM `{self.project_id}.{self.dataset_id}.{raw_table_id}`
        WHERE event_timestamp < TIMESTAMP('{retention_date.isoformat()}')
        """

        try:
            self.bq_client.query(query).result()
            self.logger.info(f"Completed cleanup of data older than {retention_days} days")
        except Exception as e:
            self.logger.error(f"Error in daily cleanup: {str(e)}")
            raise
