"""
S3 Utilities
Handles data storage and retrieval from Amazon S3 for analytics
"""

import os
import io
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from botocore.exceptions import ClientError, NoCredentialsError

class S3Connector:
    """
    S3 client for crypto data storage and analytics
    """
    
    def __init__(self):
        self.aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.aws_region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        self.bucket_name = os.getenv('AWS_S3_BUCKET_NAME')
        self.raw_prefix = os.getenv('AWS_S3_RAW_PREFIX', 'raw-data/')
        self.processed_prefix = os.getenv('AWS_S3_PROCESSED_PREFIX', 'processed-data/')
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
        # Initialize S3 client
        self.s3_client = self._create_s3_client()
        
        # Validate configuration
        self._validate_config()
    
    def _create_s3_client(self):
        """
        Create and configure S3 client
        """
        try:
            if self.aws_access_key and self.aws_secret_key:
                s3_client = boto3.client(
                    's3',
                    aws_access_key_id=self.aws_access_key,
                    aws_secret_access_key=self.aws_secret_key,
                    region_name=self.aws_region
                )
            else:
                # Use IAM role or environment credentials
                s3_client = boto3.client('s3', region_name=self.aws_region)
            
            self.logger.info("S3 client created successfully")
            return s3_client
            
        except NoCredentialsError:
            self.logger.error("AWS credentials not found")
            raise
        except Exception as e:
            self.logger.error(f"Error creating S3 client: {str(e)}")
            raise
    
    def _validate_config(self):
        """
        Validate S3 configuration
        """
        if not self.bucket_name:
            raise ValueError("AWS_S3_BUCKET_NAME environment variable is required")
        
        # Test bucket access
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            self.logger.info(f"Successfully validated access to bucket: {self.bucket_name}")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                self.logger.error(f"Bucket {self.bucket_name} does not exist")
                raise
            elif error_code == '403':
                self.logger.error(f"Access denied to bucket {self.bucket_name}")
                raise
            else:
                self.logger.error(f"Error accessing bucket {self.bucket_name}: {str(e)}")
                raise
    
    def test_connection(self) -> bool:
        """
        Test S3 connection and permissions
        """
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            return True
        except Exception as e:
            self.logger.error(f"S3 connection test failed: {str(e)}")
            return False
    
    def upload_parquet_data(self, data: List[Dict[str, Any]], s3_key: str) -> bool:
        """
        Upload data to S3 in Parquet format
        """
        try:
            if not data:
                self.logger.warning("No data to upload")
                return False
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Handle timestamp columns
            timestamp_columns = ['fetch_timestamp', 'batch_timestamp', 'last_updated', 'ath_date', 'atl_date']
            for col in timestamp_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            
            # Convert to Parquet
            table = pa.Table.from_pandas(df)
            
            # Create buffer
            parquet_buffer = io.BytesIO()
            pq.write_table(table, parquet_buffer)
            parquet_buffer.seek(0)
            
            # Upload to S3
            self.s3_client.upload_fileobj(
                parquet_buffer,
                self.bucket_name,
                s3_key,
                ExtraArgs={
                    'ContentType': 'application/octet-stream',
                    'Metadata': {
                        'record_count': str(len(data)),
                        'upload_timestamp': datetime.utcnow().isoformat(),
                        'data_format': 'parquet',
                        'pipeline_version': '1.0'
                    }
                }
            )
            
            self.logger.info(f"Successfully uploaded {len(data)} records to s3://{self.bucket_name}/{s3_key}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error uploading Parquet data to S3: {str(e)}")
            return False
    
    def upload_json_data(self, data: List[Dict[str, Any]], s3_key: str) -> bool:
        """
        Upload data to S3 in JSON format
        """
        try:
            if not data:
                self.logger.warning("No data to upload")
                return False
            
            # Convert to JSON
            import json
            json_data = json.dumps(data, indent=2, default=str)
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json_data.encode('utf-8'),
                ContentType='application/json',
                Metadata={
                    'record_count': str(len(data)),
                    'upload_timestamp': datetime.utcnow().isoformat(),
                    'data_format': 'json',
                    'pipeline_version': '1.0'
                }
            )
            
            self.logger.info(f"Successfully uploaded {len(data)} records to s3://{self.bucket_name}/{s3_key}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error uploading JSON data to S3: {str(e)}")
            return False
    
    def download_parquet_data(self, s3_key: str) -> Optional[pd.DataFrame]:
        """
        Download and parse Parquet data from S3
        """
        try:
            # Create buffer
            parquet_buffer = io.BytesIO()
            
            # Download from S3
            self.s3_client.download_fileobj(
                self.bucket_name,
                s3_key,
                parquet_buffer
            )
            
            parquet_buffer.seek(0)
            
            # Read Parquet data
            df = pd.read_parquet(parquet_buffer)
            
            self.logger.info(f"Successfully downloaded {len(df)} records from s3://{self.bucket_name}/{s3_key}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error downloading Parquet data from S3: {str(e)}")
            return None
    
    def list_files(self, prefix: str = '', max_keys: int = 1000) -> List[Dict[str, Any]]:
        """
        List files in S3 bucket with given prefix
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    files.append({
                        'key': obj['Key'],
                        'size_bytes': obj['Size'],
                        'last_modified': obj['LastModified'],
                        'etag': obj['ETag'].strip('"')
                    })
            
            self.logger.info(f"Found {len(files)} files with prefix '{prefix}'")
            return files
            
        except Exception as e:
            self.logger.error(f"Error listing S3 files: {str(e)}")
            return []
    
    def delete_file(self, s3_key: str) -> bool:
        """
        Delete a file from S3
        """
        try:
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            
            self.logger.info(f"Successfully deleted s3://{self.bucket_name}/{s3_key}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error deleting S3 file: {str(e)}")
            return False
    
    def cleanup_old_data(self, cutoff_date: datetime) -> List[str]:
        """
        Clean up old data files based on retention policy
        """
        try:
            deleted_files = []
            
            # List all files
            all_files = self.list_files()
            
            for file_info in all_files:
                # Check if file is older than cutoff date
                if file_info['last_modified'].replace(tzinfo=None) < cutoff_date:
                    if self.delete_file(file_info['key']):
                        deleted_files.append(file_info['key'])
            
            self.logger.info(f"Cleaned up {len(deleted_files)} old files from S3")
            return deleted_files
            
        except Exception as e:
            self.logger.error(f"Error cleaning up S3 data: {str(e)}")
            return []
    
    def create_athena_external_table(self, table_name: str, database_name: str) -> bool:
        """
        Create external table in Athena for S3 data
        """
        try:
            athena_client = boto3.client('athena', region_name=self.aws_region)
            
            # Define table schema
            create_table_query = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.{table_name} (
                id string,
                symbol string,
                name string,
                current_price_usd double,
                market_cap_usd bigint,
                total_volume_usd bigint,
                price_change_24h double,
                price_change_percentage_24h double,
                price_change_percentage_7d double,
                price_change_percentage_30d double,
                market_cap_rank int,
                circulating_supply double,
                total_supply double,
                max_supply double,
                ath double,
                ath_date timestamp,
                atl double,
                atl_date timestamp,
                last_updated timestamp,
                fetch_timestamp timestamp,
                batch_timestamp timestamp,
                data_source string,
                pipeline_stage string,
                data_type string
            )
            PARTITIONED BY (
                year int,
                month int,
                day int,
                hour int
            )
            STORED AS PARQUET
            LOCATION 's3://{self.bucket_name}/crypto-data/'
            TBLPROPERTIES (
                'projection.enabled'='true',
                'projection.year.type'='integer',
                'projection.year.range'='2023,2030',
                'projection.month.type'='integer',
                'projection.month.range'='1,12',
                'projection.day.type'='integer',
                'projection.day.range'='1,31',
                'projection.hour.type'='integer',
                'projection.hour.range'='0,23',
                'projection.year.format'='yyyy',
                'projection.month.format'='M',
                'projection.day.format'='d',
                'projection.hour.format'='H',
                'storage.location.template'='s3://{self.bucket_name}/crypto-data/year=${{year}}/month=${{month}}/day=${{day}}/hour=${{hour}}/'
            )
            """
            
            response = athena_client.start_query_execution(
                QueryString=create_table_query,
                ResultConfiguration={
                    'OutputLocation': os.getenv('ATHENA_RESULTS_LOCATION')
                },
                WorkGroup=os.getenv('ATHENA_WORKGROUP', 'primary')
            )
            
            self.logger.info(f"Athena table creation initiated: {response['QueryExecutionId']}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating Athena table: {str(e)}")
            return False
    
    def get_s3_metrics(self) -> Dict[str, Any]:
        """
        Get S3 storage metrics and statistics
        """
        try:
            # Get bucket size and object count
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket_name)
            
            total_size = 0
            total_objects = 0
            file_types = {}
            
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        total_size += obj['Size']
                        total_objects += 1
                        
                        # Track file types
                        file_ext = obj['Key'].split('.')[-1].lower()
                        file_types[file_ext] = file_types.get(file_ext, 0) + 1
            
            # Get raw vs processed data breakdown
            raw_files = self.list_files(prefix=self.raw_prefix)
            processed_files = self.list_files(prefix=self.processed_prefix)
            
            metrics = {
                'bucket_name': self.bucket_name,
                'total_size_bytes': total_size,
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'total_objects': total_objects,
                'raw_data_files': len(raw_files),
                'processed_data_files': len(processed_files),
                'file_types_distribution': file_types,
                'last_calculated': datetime.utcnow().isoformat()
            }
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"Error getting S3 metrics: {str(e)}")
            return {}
    
    def copy_to_processed(self, raw_s3_key: str, processed_s3_key: str) -> bool:
        """
        Copy file from raw to processed location
        """
        try:
            copy_source = {
                'Bucket': self.bucket_name,
                'Key': raw_s3_key
            }
            
            self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=self.bucket_name,
                Key=processed_s3_key,
                MetadataDirective='REPLACE',
                Metadata={
                    'processing_timestamp': datetime.utcnow().isoformat(),
                    'source_key': raw_s3_key,
                    'pipeline_stage': 'processed'
                }
            )
            
            self.logger.info(f"Successfully copied {raw_s3_key} to {processed_s3_key}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error copying S3 file: {str(e)}")
            return False
    
    def generate_presigned_url(self, s3_key: str, expiration: int = 3600) -> Optional[str]:
        """
        Generate presigned URL for S3 object access
        """
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket_name, 'Key': s3_key},
                ExpiresIn=expiration
            )
            
            self.logger.info(f"Generated presigned URL for {s3_key}")
            return url
            
        except Exception as e:
            self.logger.error(f"Error generating presigned URL: {str(e)}")
            return None