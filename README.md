# YouTube Real-Time Analytics System


A scalable system for processing and analyzing YouTube video metrics in real-time.

## Features

- Real-time event processing via Google Cloud Pub/Sub
- Stream processing with Apache Beam
- Historical data storage in BigQuery
- Real-time metrics serving with Redis caching
- RESTful API for metrics access
- Kubernetes and Docker support
- Comprehensive monitoring and alerting

## Architecture

The system consists of four main components:

1. **Ingestion Layer**
   - Event validation
   - Pub/Sub consumer
   - Schema validation

2. **Processing Layer**
   - Stream processing (real-time)
   - Batch processing (historical)

3. **Storage Layer**
   - BigQuery for historical data
   - Redis for real-time metrics

4. **Serving Layer**
   - FastAPI server
   - Metrics service

## Setup

1. Install dependencies:
```bash
python -m venv .venv
source .venv/bin/activate  # or `.venv\Scripts\activate` on Windows
pip install -r requirements.txt
```

2. Configure settings:
   - Update `config/config.yaml` with your settings
   - Place GCP credentials in `config/credentials.json`

3. Run setup script:
```bash
python scripts/setup.py
```

## Development

1. Run locally with Docker Compose:
```bash
docker-compose up --build
```

2. Run tests:
```bash
pytest tests/
```

## Deployment

1. Deploy to Kubernetes:
```bash
kubectl apply -f deployment/kubernetes-config.yaml
```

2. Monitor the system:
```bash
python scripts/monitor.py
```

## API Documentation

The API documentation is available at `/docs` when running the server.

Key endpoints:
- `GET /metrics/{video_id}` - Get real-time metrics
- `GET /metrics/{video_id}/historical` - Get historical metrics
- `GET /health` - Health check endpoint

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
