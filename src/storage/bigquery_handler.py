# src/processing/batch_processor.py
from typing import List, Dict, Optional
import pandas as pd
from google.cloud import bigquery
import logging
from datetime import datetime, timedelta
import yaml


class BatchProcessor:
    def __init__(self, config_path: str = 'config/config.yaml'):
        # Load configuration
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        # Initialize clients
        self.bq_client = bigquery.Client(project=self.config['project']['id'])
        self.logger = logging.getLogger(__name__)

        # Set up table references
        self.raw_table = f"{self.config['project']['id']}.{self.config['bigquery']['dataset_id']}.{self.config['bigquery']['tables']['events']}"
        self.aggregated_table = f"{self.config['project']['id']}.{self.config['bigquery']['dataset_id']}.{self.config['bigquery']['tables']['aggregated']}"

    def process_batch(self, batch_timestamp: Optional[datetime] = None) -> Dict:
        """
        Process a batch of data from the raw events table
        """
        if not batch_timestamp:
            batch_timestamp = datetime.utcnow()

        window_start = batch_timestamp - timedelta(hours=1)

        # Query to process raw events
        query = f"""
        SELECT
            video_id,
            event_timestamp,
            event_type,
            user_id,
            watch_time,
            country_code,
            device_type,
            playback_quality,
            metadata
        FROM {self.raw_table}
        WHERE event_timestamp BETWEEN @start_time AND @end_time
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_time", "TIMESTAMP", window_start),
                bigquery.ScalarQueryParameter("end_time", "TIMESTAMP", batch_timestamp),
            ]
        )

        try:
            # Execute query and get results as DataFrame
            df = self.bq_client.query(query, job_config=job_config).to_dataframe()

            if df.empty:
                self.logger.info("No data found for the specified time window")
                return {}

            # Process the batch
            aggregated_metrics = self._aggregate_metrics(df)

            # Save to aggregated table
            self._save_aggregated_metrics(aggregated_metrics, batch_timestamp)

            return aggregated_metrics

        except Exception as e:
            self.logger.error(f"Error processing batch: {str(e)}")
            raise

    def _aggregate_metrics(self, df: pd.DataFrame) -> Dict:
        """
        Aggregate metrics from raw events
        """
        metrics = {}

        # Group by video_id
        grouped = df.groupby('video_id')

        for video_id, group in grouped:
            metrics[video_id] = {
                'unique_views': len(group[group['event_type'] == 'view']['user_id'].unique()),
                'likes': len(group[group['event_type'] == 'like']),
                'total_watch_time': group[group['event_type'] == 'view']['watch_time'].sum(),
                'unique_users': len(group['user_id'].unique()),
                'countries_reached': len(group['country_code'].unique()),
                'device_breakdown': group['device_type'].value_counts().to_dict(),
                'quality_breakdown': group['playback_quality'].value_counts().to_dict()
            }

            # Calculate engagement rate
            if metrics[video_id]['unique_views'] > 0:
                metrics[video_id]['engagement_rate'] = metrics[video_id]['likes'] / metrics[video_id]['unique_views']
            else:
                metrics[video_id]['engagement_rate'] = 0

        return metrics

    def _save_aggregated_metrics(self, metrics: Dict, timestamp: datetime) -> None:
        """
        Save aggregated metrics to BigQuery
        """
        rows_to_insert = []

        for video_id, video_metrics in metrics.items():
            rows_to_insert.append({
                'video_id': video_id,
                'timestamp': timestamp.isoformat(),
                'metrics': video_metrics
            })

        try:
            errors = self.bq_client.insert_rows_json(self.aggregated_table, rows_to_insert)
            if errors:
                raise Exception(f"Errors inserting rows: {errors}")
        except Exception as e:
            self.logger.error(f"Error saving aggregated metrics: {str(e)}")
            raise


def main():
    """
    Main function to run the batch processor
    """
    logging.basicConfig(level=logging.INFO)
    processor = BatchProcessor()

    while True:
        try:
            # Process hourly batches
            processor.process_batch()
            # Wait for next hour
            time.sleep(3600)  # 1 hour
        except Exception as e:
            logging.error(f"Error in batch processing: {str(e)}")
            time.sleep(60)  # Wait 1 minute before retrying


if __name__ == "__main__":
    main()