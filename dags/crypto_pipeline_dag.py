"""
Crypto Data Pipeline - Main Orchestration DAG
Handles the complete flow: API → NiFi → Kafka → MongoDB/S3 → Athena
"""

import os
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

# Import custom utilities
from utils.crypto_api import CryptoAPIClient
from utils.mongodb_utils import MongoDBConnector
from utils.s3_utils import S3Connector

# Default arguments for the DAG
default_args = {
    'owner': 'crypto-pipeline',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# DAG configuration
dag = DAG(
    'crypto_data_pipeline',
    default_args=default_args,
    description='Complete crypto data pipeline with real-time and batch processing',
    schedule_interval=timedelta(minutes=5),  # Run every 5 minutes
    catchup=False,
    tags=['crypto', 'pipeline', 'streaming', 'batch'],
    max_active_runs=1,
    render_template_as_native_obj=True,
)

# Environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC_RAW = os.getenv('KAFKA_TOPIC_CRYPTO_RAW', 'crypto-raw-data')
NIFI_BASE_URL = os.getenv('NIFI_BASE_URL', 'https://nifi:8443')
NIFI_USERNAME = os.getenv('NIFI_USERNAME', 'nifi')
NIFI_PASSWORD = os.getenv('NIFI_PASSWORD', 'nifipassword')

@task
def health_check():
    """
    Perform health checks on all services
    """
    import requests
    import socket
    from kafka import KafkaProducer
    
    services_status = {}
    
    # Check Kafka
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        producer.close()
        services_status['kafka'] = 'healthy'
    except Exception as e:
        services_status['kafka'] = f'unhealthy: {str(e)}'
    
    # Check NiFi
    try:
        response = requests.get(f"{NIFI_BASE_URL}/nifi-api/system-diagnostics", 
                              verify=False, timeout=10)
        services_status['nifi'] = 'healthy' if response.status_code in [200, 401] else 'unhealthy'
    except Exception as e:
        services_status['nifi'] = f'unhealthy: {str(e)}'
    
    # Check MongoDB
    try:
        mongo_client = MongoDBConnector()
        mongo_client.test_connection()
        services_status['mongodb'] = 'healthy'
    except Exception as e:
        services_status['mongodb'] = f'unhealthy: {str(e)}'
    
    # Check S3
    try:
        s3_client = S3Connector()
        s3_client.test_connection()
        services_status['s3'] = 'healthy'
    except Exception as e:
        services_status['s3'] = f'unhealthy: {str(e)}'
    
    print("Service Health Check Results:")
    for service, status in services_status.items():
        print(f"  {service}: {status}")
    
    # Fail if critical services are down
    critical_services = ['kafka', 'mongodb']
    for service in critical_services:
        if 'unhealthy' in services_status.get(service, ''):
            raise Exception(f"Critical service {service} is unhealthy")
    
    return services_status

@task
def fetch_crypto_data():
    """
    Fetch cryptocurrency data from multiple APIs
    """
    crypto_client = CryptoAPIClient()
    
    # Get list of cryptocurrencies to fetch
    crypto_symbols = os.getenv('CRYPTO_SYMBOLS', 'bitcoin,ethereum,cardano').split(',')
    
    all_crypto_data = []
    
    for symbol in crypto_symbols:
        try:
            # Fetch from CoinGecko
            data = crypto_client.get_coin_data(symbol.strip())
            if data:
                all_crypto_data.append(data)
                print(f"Successfully fetched data for {symbol}")
            else:
                print(f"No data returned for {symbol}")
                
        except Exception as e:
            print(f"Error fetching data for {symbol}: {str(e)}")
            continue
    
    if not all_crypto_data:
        raise Exception("No crypto data was successfully fetched")
    
    print(f"Total crypto data points fetched: {len(all_crypto_data)}")
    return all_crypto_data

@task
def send_to_kafka(crypto_data: List[Dict[str, Any]]):
    """
    Send crypto data to Kafka for streaming processing
    """
    from kafka import KafkaProducer
    import time
    
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8') if k else None,
        retries=3,
        retry_backoff_ms=1000,
        acks='all'
    )
    
    sent_count = 0
    
    for data in crypto_data:
        try:
            # Use symbol as key for partitioning
            key = data.get('symbol', 'unknown')
            
            # Add timestamp and pipeline metadata
            enriched_data = {
                **data,
                'pipeline_timestamp': datetime.utcnow().isoformat(),
                'pipeline_source': 'airflow_dag',
                'pipeline_version': '1.0'
            }
            
            # Send to Kafka
            future = producer.send(
                topic=KAFKA_TOPIC_RAW,
                key=key,
                value=enriched_data
            )
            
            # Wait for acknowledgment
            record_metadata = future.get(timeout=10)
            sent_count += 1
            
            print(f"Sent {key} to topic {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
            
        except Exception as e:
            print(f"Error sending data to Kafka: {str(e)}")
            continue
    
    producer.flush()
    producer.close()
    
    print(f"Successfully sent {sent_count}/{len(crypto_data)} messages to Kafka")
    return sent_count

@task
def get_nifi_access_token():
    """
    Get NiFi access token for API calls
    """
    import requests
    import urllib3
    
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    try:
        response = requests.post(
            f"{NIFI_BASE_URL}/nifi-api/access/token",
            data=f"username={NIFI_USERNAME}&password={NIFI_PASSWORD}",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            verify=False,
            timeout=30
        )
        
        if response.status_code in [200, 201]:
            token = response.text.strip()
            print(f"Successfully retrieved NiFi token")
            return token
        else:
            raise Exception(f"Failed to get NiFi token: {response.status_code}")
            
    except Exception as e:
        print(f"Error getting NiFi token: {str(e)}")
        raise

@task
def trigger_nifi_flow(token: str):
    """
    Trigger NiFi flow for additional processing
    """
    import requests
    import urllib3
    
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    # This would contain your actual NiFi Process Group ID
    pg_id = os.getenv('NIFI_PG_ID_CRYPTO', 'your_process_group_id_here')
    
    if pg_id == 'your_process_group_id_here':
        print("NiFi Process Group ID not configured, skipping NiFi trigger")
        return {"status": "skipped", "reason": "no_pg_id"}
    
    try:
        response = requests.put(
            f"{NIFI_BASE_URL}/nifi-api/flow/process-groups/{pg_id}",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            },
            json={"id": pg_id, "state": "RUNNING"},
            verify=False,
            timeout=30
        )
        
        if response.status_code in [200, 201]:
            print("Successfully triggered NiFi flow")
            return {"status": "success", "pg_id": pg_id}
        else:
            raise Exception(f"Failed to trigger NiFi flow: {response.status_code}")
            
    except Exception as e:
        print(f"Error triggering NiFi flow: {str(e)}")
        return {"status": "error", "error": str(e)}

@task
def process_realtime_data(crypto_data: List[Dict[str, Any]]):
    """
    Process and store real-time data in MongoDB
    """
    mongo_client = MongoDBConnector()
    
    processed_count = 0
    
    for data in crypto_data:
        try:
            # Add processing timestamp
            processed_data = {
                **data,
                'processed_timestamp': datetime.utcnow(),
                'data_type': 'realtime',
                'pipeline_stage': 'mongodb_storage'
            }
            
            # Store in MongoDB
            result = mongo_client.insert_crypto_data(processed_data)
            
            if result:
                processed_count += 1
                print(f"Stored {data.get('symbol', 'unknown')} in MongoDB")
                
        except Exception as e:
            print(f"Error processing data for MongoDB: {str(e)}")
            continue
    
    mongo_client.close()
    print(f"Successfully processed {processed_count}/{len(crypto_data)} records to MongoDB")
    return processed_count

@task
def process_batch_data(crypto_data: List[Dict[str, Any]]):
    """
    Process and store batch data in S3 for analytics
    """
    s3_client = S3Connector()
    
    try:
        # Add batch processing metadata
        batch_data = []
        current_time = datetime.utcnow()
        
        for data in crypto_data:
            processed_data = {
                **data,
                'batch_timestamp': current_time.isoformat(),
                'data_type': 'batch',
                'pipeline_stage': 's3_storage',
                'partition_date': current_time.strftime('%Y-%m-%d'),
                'partition_hour': current_time.strftime('%H')
            }
            batch_data.append(processed_data)
        
        # Store in S3 as Parquet
        s3_key = f"crypto-data/year={current_time.year}/month={current_time.month}/day={current_time.day}/hour={current_time.hour}/crypto_data_{current_time.strftime('%Y%m%d_%H%M%S')}.parquet"
        
        result = s3_client.upload_parquet_data(batch_data, s3_key)
        
        if result:
            print(f"Successfully uploaded {len(batch_data)} records to S3: {s3_key}")
            return {"status": "success", "s3_key": s3_key, "record_count": len(batch_data)}
        else:
            raise Exception("Failed to upload data to S3")
            
    except Exception as e:
        print(f"Error processing batch data: {str(e)}")
        raise

@task
def update_athena_tables():
    """
    Update Athena table partitions for new data
    """
    import boto3
    from botocore.exceptions import ClientError
    
    try:
        athena_client = boto3.client('athena')
        
        # Update partitions
        current_time = datetime.utcnow()
        partition_location = f"s3://{os.getenv('AWS_S3_BUCKET_NAME')}/crypto-data/year={current_time.year}/month={current_time.month}/day={current_time.day}/hour={current_time.hour}/"
        
        query = f"""
        ALTER TABLE {os.getenv('ATHENA_DATABASE')}.crypto_data 
        ADD IF NOT EXISTS PARTITION (
            year={current_time.year},
            month={current_time.month},
            day={current_time.day},
            hour={current_time.hour}
        ) LOCATION '{partition_location}'
        """
        
        response = athena_client.start_query_execution(
            QueryString=query,
            ResultConfiguration={
                'OutputLocation': os.getenv('ATHENA_RESULTS_LOCATION')
            },
            WorkGroup=os.getenv('ATHENA_WORKGROUP', 'primary')
        )
        
        print(f"Athena partition update initiated: {response['QueryExecutionId']}")
        return response['QueryExecutionId']
        
    except ClientError as e:
        print(f"Error updating Athena partitions: {str(e)}")
        return None
    except Exception as e:
        print(f"Unexpected error updating Athena: {str(e)}")
        return None

@task
def generate_pipeline_metrics(
    kafka_sent_count: int,
    mongodb_processed_count: int,
    s3_batch_result: Dict[str, Any]
):
    """
    Generate and log pipeline metrics
    """
    metrics = {
        'pipeline_run_timestamp': datetime.utcnow().isoformat(),
        'kafka_messages_sent': kafka_sent_count,
        'mongodb_records_processed': mongodb_processed_count,
        's3_records_uploaded': s3_batch_result.get('record_count', 0) if s3_batch_result else 0,
        's3_upload_status': s3_batch_result.get('status', 'failed') if s3_batch_result else 'failed',
        'pipeline_success_rate': 0.0
    }
    
    # Calculate success rate
    total_attempts = kafka_sent_count
    successful_operations = min(mongodb_processed_count, metrics['s3_records_uploaded'])
    
    if total_attempts > 0:
        metrics['pipeline_success_rate'] = (successful_operations / total_attempts) * 100
    
    print("Pipeline Metrics:")
    for key, value in metrics.items():
        print(f"  {key}: {value}")
    
    # Store metrics in MongoDB for monitoring
    try:
        mongo_client = MongoDBConnector()
        mongo_client.insert_pipeline_metrics(metrics)
        mongo_client.close()
        print("Metrics stored in MongoDB")
    except Exception as e:
        print(f"Error storing metrics: {str(e)}")
    
    return metrics

@task
def cleanup_old_data():
    """
    Clean up old data based on retention policies
    """
    retention_days = int(os.getenv('DATA_RETENTION_DAYS', '30'))
    cutoff_date = datetime.utcnow() - timedelta(days=retention_days)
    
    cleanup_results = {}
    
    # Clean MongoDB old data
    try:
        mongo_client = MongoDBConnector()
        deleted_count = mongo_client.cleanup_old_data(cutoff_date)
        cleanup_results['mongodb_deleted'] = deleted_count
        mongo_client.close()
        print(f"Deleted {deleted_count} old records from MongoDB")
    except Exception as e:
        print(f"Error cleaning MongoDB data: {str(e)}")
        cleanup_results['mongodb_error'] = str(e)
    
    # Clean S3 old data (optional - can be handled by S3 lifecycle policies)
    try:
        s3_client = S3Connector()
        deleted_objects = s3_client.cleanup_old_data(cutoff_date)
        cleanup_results['s3_deleted'] = len(deleted_objects) if deleted_objects else 0
        print(f"Deleted {cleanup_results['s3_deleted']} old objects from S3")
    except Exception as e:
        print(f"Error cleaning S3 data: {str(e)}")
        cleanup_results['s3_error'] = str(e)
    
    return cleanup_results

# Create task groups for better organization
with TaskGroup("data_ingestion", dag=dag) as data_ingestion_group:
    health_check_task = health_check()
    fetch_data_task = fetch_crypto_data()
    
    health_check_task >> fetch_data_task

with TaskGroup("streaming_processing", dag=dag) as streaming_group:
    kafka_task = send_to_kafka(fetch_data_task)
    realtime_processing_task = process_realtime_data(fetch_data_task)
    
    kafka_task >> realtime_processing_task

with TaskGroup("batch_processing", dag=dag) as batch_group:
    batch_processing_task = process_batch_data(fetch_data_task)
    athena_update_task = update_athena_tables()
    
    batch_processing_task >> athena_update_task

with TaskGroup("nifi_integration", dag=dag) as nifi_group:
    nifi_token_task = get_nifi_access_token()
    nifi_trigger_task = trigger_nifi_flow(nifi_token_task)

with TaskGroup("monitoring_cleanup", dag=dag) as monitoring_group:
    metrics_task = generate_pipeline_metrics(
        kafka_task,
        realtime_processing_task,
        batch_processing_task
    )
    cleanup_task = cleanup_old_data()
    
    metrics_task >> cleanup_task

# Set up task dependencies
data_ingestion_group >> [streaming_group, batch_group, nifi_group]
[streaming_group, batch_group] >> monitoring_group

# Add error handling and alerting
@task
def send_failure_alert(context):
    """
    Send alert when pipeline fails
    """
    import smtplib
    from email.mime.text import MIMEText
    
    task_instance = context['task_instance']
    dag_run = context['dag_run']
    
    alert_message = f"""
    Crypto Data Pipeline Failure Alert
    
    DAG: {dag_run.dag_id}
    Task: {task_instance.task_id}
    Execution Date: {dag_run.execution_date}
    Log URL: {task_instance.log_url}
    
    Please check the Airflow UI for more details.
    """
    
    print(f"ALERT: {alert_message}")
    
    # Here you would integrate with your alerting system
    # (email, Slack, PagerDuty, etc.)
    
    return alert_message

# Apply failure callback to all tasks
for task_id in dag.task_dict:
    dag.task_dict[task_id].on_failure_callback = send_failure_alert