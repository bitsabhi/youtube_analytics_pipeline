#!/usr/bin/env python3

import yaml
import subprocess
from google.cloud import pubsub_v1
from google.cloud import bigquery


def load_config():
    with open('config/config.yaml', 'r') as file:
        return yaml.safe_load(file)


def setup_pubsub(config):
    """Setup Pub/Sub topic and subscription"""
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    project_id = config['project']['id']
    topic_name = config['pubsub']['topic_name']
    subscription_name = config['pubsub']['subscription_name']

    topic_path = publisher.topic_path(project_id, topic_name)
    subscription_path = subscriber.subscription_path(project_id, subscription_name)

    # Create topic
    try:
        publisher.create_topic(name=topic_path)
        print(f"Created topic: {topic_path}")
    except Exception as e:
        print(f"Topic already exists: {e}")

    # Create subscription
    try:
        subscriber.create_subscription(
            name=subscription_path,
            topic=topic_path,
            ack_deadline_seconds=config['pubsub']['ack_deadline_seconds']
        )
        print(f"Created subscription: {subscription_path}")
    except Exception as e:
        print(f"Subscription already exists: {e}")


def setup_bigquery(config):
    """Setup BigQuery dataset and tables"""
    client = bigquery.Client(project=config['project']['id'])
    dataset_id = config['bigquery']['dataset_id']
    dataset_ref = f"{config['project']['id']}.{dataset_id}"

    # Create dataset
    try:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = config['project']['region']
        client.create_dataset(dataset)
        print(f"Created dataset: {dataset_id}")
    except Exception as e:
        print(f"Dataset already exists: {e}")

    # Create tables
    for table_name in config['bigquery']['tables'].values():
        table_ref = f"{dataset_ref}.{table_name}"
        schema = [
            bigquery.SchemaField("video_id", "STRING"),
            bigquery.SchemaField("event_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("event_type", "STRING"),
            bigquery.SchemaField("watch_time", "FLOAT"),
        ]

        try:
            table = bigquery.Table(table_ref, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=config['bigquery']['partition_field']
            )
            table.clustering_fields = config['bigquery']['clustering_fields']
            client.create_table(table)
            print(f"Created table: {table_name}")
        except Exception as e:
            print(f"Table already exists: {e}")


def main():
    print("Starting setup process...")
    config = load_config()

    # Setup infrastructure
    setup_pubsub(config)
    setup_bigquery(config)

    # Install requirements
    print("Installing Python requirements...")
    subprocess.run(["pip", "install", "-r", "requirements.txt"])

    print("Setup completed successfully!")


if __name__ == "__main__":
    main()
