#!/usr/bin/env python3
# scripts/monitor.py

import yaml
import time
import redis
import psutil
import logging
from google.cloud import monitoring_v3
from datetime import datetime, timedelta


def load_config():
    with open('config/config.yaml', 'r') as file:
        return yaml.safe_load(file)


class SystemMonitor:
    def __init__(self, config):
        self.config = config
        self.client = monitoring_v3.MetricServiceClient()
        self.project_name = f"projects/{config['project']['id']}"

        # Setup logging
        logging.basicConfig(
            level=config['monitoring']['log_level'],
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('SystemMonitor')

        # Setup Redis connection
        self.redis_client = redis.Redis(
            host=config['redis']['host'],
            port=config['redis']['port'],
            db=config['redis']['db']
        )

    def check_system_metrics(self):
        """Monitor system-level metrics"""
        cpu_percent = psutil.cpu_percent()
        memory_percent = psutil.virtual_memory().percent
        disk_percent = psutil.disk_usage('/').percent

        self.logger.info(f"CPU Usage: {cpu_percent}%")
        self.logger.info(f"Memory Usage: {memory_percent}%")
        self.logger.info(f"Disk Usage: {disk_percent}%")

        # Alert if thresholds are exceeded
        if cpu_percent > 90:
            self.logger.warning("High CPU usage detected!")
        if memory_percent > 90:
            self.logger.warning("High memory usage detected!")
        if disk_percent > 90:
            self.logger.warning("High disk usage detected!")

    def check_api_metrics(self):
        """Monitor API performance metrics"""
        now = time.time()
        window_start = now - 300  # Last 5 minutes

        # Get API latency from Redis
        latency_key = f"{self.config['redis']['key_prefix']}api_latency"
        latencies = self.redis_client.zrangebyscore(latency_key, window_start, now)

        if latencies:
            avg_latency = sum(float(x) for x in latencies) / len(latencies)
            if avg_latency > self.config['monitoring']['alert_threshold']['latency_ms']:
                self.logger.warning(f"High API latency detected: {avg_latency}ms")

    def check_error_rates(self):
        """Monitor error rates"""
        now = time.time()
        window_start = now - 300  # Last 5 minutes

        # Get error counts from Redis
        error_key = f"{self.config['redis']['key_prefix']}error_count"
        request_key = f"{self.config['redis']['key_prefix']}request_count"

        error_count = int(self.redis_client.get(error_key) or 0)
        request_count = int(self.redis_client.get(request_key) or 1)  # Avoid division by zero

        error_rate = (error_count / request_count) * 100
        if error_rate > self.config['monitoring']['alert_threshold']['error_rate_percent']:
            self.logger.warning(f"High error rate detected: {error_rate}%")

    def export_metrics(self):
        """Export metrics to Cloud Monitoring"""
        client = monitoring_v3.MetricServiceClient()

        # Create time series
        now = time.time()
        series = monitoring_v3.TimeSeries()
        series.metric.type = "custom.googleapis.com/youtube_analytics/api_latency"
        series.resource.type = "global"

        # Add data point
        point = monitoring_v3.Point()
        point.value.double_value = self.get_average_latency()
        point.interval.end_time.seconds = int(now)
        series.points = [point]

        client.create_time_series(
            request={"name": self.project_name, "time_series": [series]}
        )

    def run(self):
        """Main monitoring loop"""
        while True:
            try:
                self.check_system_metrics()
                self.check_api_metrics()
                self.check_error_rates()
                self.export_metrics()

                # Wait for next interval
                time.sleep(self.config['monitoring']['metrics_export_interval_seconds'])
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                time.sleep(10)  # Wait before retrying


def main():
    config = load_config()
    monitor = SystemMonitor(config)
    monitor.run()


if __name__ == "__main__":
    main()
