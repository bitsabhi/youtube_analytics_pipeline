# Usage Guide

## Quick Start

1. Clone the repository and set up environment:
```bash
git clone <repository-url>
cd youtube-analytics
python -m venv .venv
source .venv/bin/activate  # or `.venv\Scripts\activate` on Windows
pip install -r requirements.txt
```

2. Configure your environment:
```bash
# Copy your Google Cloud credentials
cp path/to/your/credentials.json config/credentials.json

# Update config/config.yaml with your settings
vim config/config.yaml
```

3. Run the setup script:
```bash
python scripts/setup.py
```

## Running the Services

### Using Docker Compose (Recommended for Development)
```bash
docker-compose up --build
```

### Manual Start
1. Start Redis:
```bash
redis-server
```

2. Start the API server:
```bash
uvicorn src.serving.api_server:app --host 0.0.0.0 --port 8000 --reload
```

3. Start the stream processor:
```bash
python -m src.processing.stream_processor
```

4. Start the monitoring:
```bash
python scripts/monitor.py
```

## API Usage

### Real-time Metrics
```bash
# Get real-time metrics for a video
curl http://localhost:8000/metrics/VIDEO_ID

# Get historical metrics
curl "http://localhost:8000/metrics/VIDEO_ID/historical?start_time=2024-01-01T00:00:00Z&end_time=2024-01-02T00:00:00Z"

# Check health
curl http://localhost:8000/health
```

### Example Response
```json
{
    "video_id": "example123",
    "views": 1000,
    "likes": 150,
    "watch_time": 45000.5,
    "unique_users": 800,
    "countries_reached": 25,
    "engagement_rate": 0.15
}
```

## Monitoring

1. Access metrics dashboard:
```bash
# If using Kubernetes
kubectl port-forward svc/analytics-api-service 8000:80
```

2. View logs:
```bash
# Docker Compose
docker-compose logs -f

# Kubernetes
kubectl logs -f deployment/analytics-api
```

## Configuration Options

### Redis Configuration
- `REDIS_HOST`: Redis host (default: localhost)
- `REDIS_PORT`: Redis port (default: 6379)
- `REDIS_DB`: Redis database number (default: 0)

### API Configuration
- `API_HOST`: API host (default: 0.0.0.0)
- `API_PORT`: API port (default: 8000)
- `API_WORKERS`: Number of API workers (default: 4)

### Processing Configuration
- `WINDOW_SIZE`: Processing window size in seconds (default: 300)
- `BATCH_SIZE`: Batch processing size (default: 1000)

## Troubleshooting

1. Check service health:
```bash
curl http://localhost:8000/health
```

2. Verify Redis connection:
```bash
redis-cli ping
```

3. Test BigQuery access:
```python
from google.cloud import bigquery
client = bigquery.Client()
print(client.query("SELECT 1").result())
```

4. Common Issues:
   - Redis connection refused: Check if Redis is running
   - BigQuery authentication: Verify credentials.json
   - Missing data: Check Pub/Sub subscription
   - High latency: Monitor Redis cache hit rate