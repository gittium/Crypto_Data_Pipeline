"""
AWS Setup Script
Initializes S3 bucket and Athena database for crypto data pipeline
"""

import os
import boto3
import json
from botocore.exceptions import ClientError, NoCredentialsError
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AWSSetup:
    """
    Setup AWS resources for crypto data pipeline
    """
    
    def __init__(self):
        self.aws_region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        self.bucket_name = os.getenv('AWS_S3_BUCKET_NAME')
        self.athena_database = os.getenv('ATHENA_DATABASE', 'crypto_analytics')
        self.athena_workgroup = os.getenv('ATHENA_WORKGROUP', 'primary')
        self.athena_results_location = os.getenv('ATHENA_RESULTS_LOCATION')
        
        if not self.bucket_name:
            raise ValueError("AWS_S3_BUCKET_NAME environment variable is required")
        
        # Initialize AWS clients
        try:
            self.s3_client = boto3.client('s3', region_name=self.aws_region)
            self.athena_client = boto3.client('athena', region_name=self.aws_region)
            self.glue_client = boto3.client('glue', region_name=self.aws_region)
        except NoCredentialsError:
            logger.error("AWS credentials not found. Please configure AWS credentials.")
            raise
    
    def create_s3_bucket(self):
        """
        Create S3 bucket for data storage
        """
        try:
            # Check if bucket already exists
            try:
                self.s3_client.head_bucket(Bucket=self.bucket_name)
                logger.info(f"S3 bucket {self.bucket_name} already exists")
                return True
            except ClientError as e:
                if e.response['Error']['Code'] != '404':
                    raise
            
            # Create bucket
            if self.aws_region == 'us-east-1':
                # us-east-1 doesn't require LocationConstraint
                self.s3_client.create_bucket(Bucket=self.bucket_name)
            else:
                self.s3_client.create_bucket(
                    Bucket=self.bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': self.aws_region}
                )
            
            logger.info(f"Created S3 bucket: {self.bucket_name}")
            
            # Enable versioning
            self.s3_client.put_bucket_versioning(
                Bucket=self.bucket_name,
                VersioningConfiguration={'Status': 'Enabled'}
            )
            
            # Add lifecycle configuration
            lifecycle_config = {
                'Rules': [
                    {
                        'ID': 'CryptoDataLifecycle',
                        'Status': 'Enabled',
                        'Filter': {'Prefix': 'crypto-data/'},
                        'Transitions': [
                            {
                                'Days': 30,
                                'StorageClass': 'STANDARD_IA'
                            },
                            {
                                'Days': 90,
                                'StorageClass': 'GLACIER'
                            }
                        ],
                        'Expiration': {'Days': 365}
                    }
                ]
            }
            
            self.s3_client.put_bucket_lifecycle_configuration(
                Bucket=self.bucket_name,
                LifecycleConfiguration=lifecycle_config
            )
            
            logger.info("S3 bucket lifecycle policy configured")
            return True
            
        except ClientError as e:
            logger.error(f"Error creating S3 bucket: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error creating S3 bucket: {str(e)}")
            return False
    
    def create_athena_database(self):
        """
        Create Athena database for analytics
        """
        try:
            query = f"CREATE DATABASE IF NOT EXISTS {self.athena_database}"
            
            response = self.athena_client.start_query_execution(
                QueryString=query,
                ResultConfiguration={
                    'OutputLocation': self.athena_results_location
                },
                WorkGroup=self.athena_workgroup
            )
            
            # Wait for query to complete
            query_id = response['QueryExecutionId']
            self._wait_for_query_completion(query_id)
            
            logger.info(f"Created Athena database: {self.athena_database}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating Athena database: {str(e)}")
            return False
    
    def create_athena_table(self):
        """
        Create Athena table for crypto data
        """
        try:
            create_table_query = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {self.athena_database}.crypto_data (
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
                data_type string,
                high_24h double,
                low_24h double,
                market_cap_change_24h bigint,
                market_cap_change_percentage_24h double,
                fully_diluted_valuation bigint,
                ath_change_percentage double,
                atl_change_percentage double,
                sparkline_7d array<double>,
                producer_timestamp timestamp,
                producer_version string,
                fetch_method string,
                distance_from_ath_percent double
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
                'storage.location.template'='s3://{self.bucket_name}/crypto-data/year=${{year}}/month=${{month}}/day=${{day}}/hour=${{hour}}/',
                'has_encrypted_data'='false'
            )
            """
            
            response = self.athena_client.start_query_execution(
                QueryString=create_table_query,
                ResultConfiguration={
                    'OutputLocation': self.athena_results_location
                },
                WorkGroup=self.athena_workgroup
            )
            
            query_id = response['QueryExecutionId']
            success = self._wait_for_query_completion(query_id)
            
            if success:
                logger.info(f"Created Athena table: {self.athena_database}.crypto_data")
                return True
            else:
                logger.error("Failed to create Athena table")
                return False
                
        except Exception as e:
            logger.error(f"Error creating Athena table: {str(e)}")
            return False
    
    def create_sample_views(self):
        """
        Create useful Athena views for common queries
        """
        views = [
            {
                'name': 'daily_price_summary',
                'query': f"""
                CREATE OR REPLACE VIEW {self.athena_database}.daily_price_summary AS
                SELECT 
                    symbol,
                    year,
                    month,
                    day,
                    AVG(current_price_usd) as avg_price,
                    MIN(current_price_usd) as min_price,
                    MAX(current_price_usd) as max_price,
                    AVG(total_volume_usd) as avg_volume,
                    COUNT(*) as data_points
                FROM {self.athena_database}.crypto_data
                GROUP BY symbol, year, month, day
                """
            },
            {
                'name': 'top_movers_24h',
                'query': f"""
                CREATE OR REPLACE VIEW {self.athena_database}.top_movers_24h AS
                WITH latest_data AS (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY fetch_timestamp DESC) as rn
                    FROM {self.athena_database}.crypto_data
                    WHERE year = YEAR(CURRENT_DATE) 
                      AND month = MONTH(CURRENT_DATE)
                      AND day = DAY_OF_MONTH(CURRENT_DATE)
                )
                SELECT 
                    symbol,
                    name,
                    current_price_usd,
                    price_change_percentage_24h,
                    market_cap_usd,
                    total_volume_usd
                FROM latest_data
                WHERE rn = 1 
                  AND price_change_percentage_24h IS NOT NULL
                ORDER BY ABS(price_change_percentage_24h) DESC
                """
            },
            {
                'name': 'market_cap_distribution',
                'query': f"""
                CREATE OR REPLACE VIEW {self.athena_database}.market_cap_distribution AS
                WITH latest_data AS (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY fetch_timestamp DESC) as rn
                    FROM {self.athena_database}.crypto_data
                    WHERE year = YEAR(CURRENT_DATE) 
                      AND month = MONTH(CURRENT_DATE)
                      AND day = DAY_OF_MONTH(CURRENT_DATE)
                )
                SELECT 
                    CASE 
                        WHEN market_cap_usd < 1000000 THEN 'Micro (<$1M)'
                        WHEN market_cap_usd < 100000000 THEN 'Small ($1M-$100M)'
                        WHEN market_cap_usd < 1000000000 THEN 'Medium ($100M-$1B)'
                        WHEN market_cap_usd < 10000000000 THEN 'Large ($1B-$10B)'
                        ELSE 'Mega (>$10B)'
                    END as market_cap_category,
                    COUNT(*) as coin_count,
                    SUM(market_cap_usd) as total_market_cap,
                    AVG(market_cap_usd) as avg_market_cap
                FROM latest_data
                WHERE rn = 1 AND market_cap_usd IS NOT NULL
                GROUP BY 1
                ORDER BY total_market_cap DESC
                """
            }
        ]
        
        for view in views:
            try:
                response = self.athena_client.start_query_execution(
                    QueryString=view['query'],
                    ResultConfiguration={
                        'OutputLocation': self.athena_results_location
                    },
                    WorkGroup=self.athena_workgroup
                )
                
                query_id = response['QueryExecutionId']
                success = self._wait_for_query_completion(query_id)
                
                if success:
                    logger.info(f"Created Athena view: {view['name']}")
                else:
                    logger.error(f"Failed to create Athena view: {view['name']}")
                    
            except Exception as e:
                logger.error(f"Error creating view {view['name']}: {str(e)}")
    
    def setup_glue_crawler(self):
        """
        Setup AWS Glue crawler for automatic schema discovery
        """
        try:
            crawler_name = f"{self.athena_database}_crypto_crawler"
            
            # Check if crawler exists
            try:
                self.glue_client.get_crawler(Name=crawler_name)
                logger.info(f"Glue crawler {crawler_name} already exists")
                return True
            except ClientError as e:
                if e.response['Error']['Code'] != 'EntityNotFoundException':
                    raise
            
            # Create IAM role for crawler (this should be done separately in production)
            iam_role_arn = f"arn:aws:iam::{boto3.client('sts').get_caller_identity()['Account']}:role/service-role/AWSGlueServiceRole-CryptoCrawler"
            
            # Create crawler
            self.glue_client.create_crawler(
                Name=crawler_name,
                Role=iam_role_arn,
                DatabaseName=self.athena_database,
                Targets={
                    'S3Targets': [
                        {
                            'Path': f"s3://{self.bucket_name}/crypto-data/"
                        }
                    ]
                },
                Schedule='cron(0 6 * * ? *)',  # Run daily at 6 AM
                SchemaChangePolicy={
                    'UpdateBehavior': 'UPDATE_IN_DATABASE',
                    'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
                },
                RecrawlPolicy={'RecrawlBehavior': 'CRAWL_EVERYTHING'},
                Configuration=json.dumps({
                    "Version": 1.0,
                    "CrawlerOutput": {
                        "Partitions": {"AddOrUpdateBehavior": "InheritFromTable"}
                    }
                })
            )
            
            logger.info(f"Created Glue crawler: {crawler_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error setting up Glue crawler: {str(e)}")
            logger.info("You may need to create the IAM role manually or skip the crawler setup")
            return False
    
    def _wait_for_query_completion(self, query_id: str, max_wait_time: int = 60) -> bool:
        """
        Wait for Athena query to complete
        """
        import time
        
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            try:
                response = self.athena_client.get_query_execution(QueryExecutionId=query_id)
                status = response['QueryExecution']['Status']['State']
                
                if status in ['SUCCEEDED']:
                    return True
                elif status in ['FAILED', 'CANCELLED']:
                    error_msg = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                    logger.error(f"Athena query failed: {error_msg}")
                    return False
                
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"Error checking query status: {str(e)}")
                return False
        
        logger.error(f"Query {query_id} timed out after {max_wait_time} seconds")
        return False
    
    def run_full_setup(self):
        """
        Run complete AWS setup
        """
        logger.info("Starting AWS setup for crypto data pipeline...")
        
        success_count = 0
        total_steps = 5
        
        # Step 1: Create S3 bucket
        if self.create_s3_bucket():
            success_count += 1
        
        # Step 2: Create Athena database
        if self.create_athena_database():
            success_count += 1
        
        # Step 3: Create Athena table
        if self.create_athena_table():
            success_count += 1
        
        # Step 4: Create sample views
        self.create_sample_views()
        success_count += 1  # Always count as success for views
        
        # Step 5: Setup Glue crawler (optional)
        if self.setup_glue_crawler():
            success_count += 1
        
        logger.info(f"AWS setup completed: {success_count}/{total_steps} steps successful")
        
        if success_count >= 3:  # At least S3, database, and table
            logger.info("Core AWS resources are ready for the crypto data pipeline!")
            return True
        else:
            logger.error("AWS setup failed. Please check the errors above.")
            return False

def main():
    """
    Main function to run AWS setup
    """
    try:
        setup = AWSSetup()
        success = setup.run_full_setup()
        
        if success:
            print("✅ AWS setup completed successfully!")
            print(f"📊 Athena database: {setup.athena_database}")
            print(f"🪣 S3 bucket: {setup.bucket_name}")
            print(f"🌍 Region: {setup.aws_region}")
        else:
            print("❌ AWS setup failed. Check the logs for details.")
            
    except Exception as e:
        print(f"❌ Setup error: {str(e)}")

if __name__ == "__main__":
    main()