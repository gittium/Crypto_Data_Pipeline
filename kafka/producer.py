"""
Kafka Producer for Crypto Data
Handles real-time streaming of cryptocurrency data
"""

import os
import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
import requests

class CryptoKafkaProducer:
    """
    Kafka producer for streaming cryptocurrency data
    """
    
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
        self.topic_raw = os.getenv('KAFKA_TOPIC_CRYPTO_RAW', 'crypto-raw-data')
        self.topic_processed = os.getenv('KAFKA_TOPIC_CRYPTO_PROCESSED', 'crypto-processed-data')
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Create producer
        self.producer = self._create_producer()
        
        # API configuration
        self.api_base_url = os.getenv('CRYPTO_API_BASE_URL', 'https://api.coingecko.com/api/v3')
        self.api_key = os.getenv('COINGECKO_API_KEY')
        self.crypto_symbols = os.getenv('CRYPTO_SYMBOLS', 'bitcoin,ethereum,cardano').split(',')
        
        # Production settings
        self.fetch_interval = int(os.getenv('FETCH_INTERVAL_SECONDS', '300'))  # 5 minutes default
        self.batch_size = int(os.getenv('KAFKA_BATCH_SIZE', '100'))
        self.max_retries = int(os.getenv('MAX_RETRIES', '3'))
    
    def _create_producer(self) -> KafkaProducer:
        """
        Create and configure Kafka producer
        """
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None,
                acks='all',  # Wait for all replicas to acknowledge
                retries=self.max_retries,
                retry_backoff_ms=1000,
                batch_size=16384,  # 16KB batch size
                linger_ms=100,  # Wait up to 100ms to batch messages
                buffer_memory=33554432,  # 32MB buffer
                max_request_size=1048576,  # 1MB max request size
                compression_type='gzip',
                enable_idempotence=True,
                request_timeout_ms=30000,
                delivery_timeout_ms=120000,
            )
            
            self.logger.info("Kafka producer created successfully")
            return producer
            
        except Exception as e:
            self.logger.error(f"Error creating Kafka producer: {str(e)}")
            raise
    
    def fetch_crypto_data(self) -> List[Dict[str, Any]]:
        """
        Fetch cryptocurrency data from API
        """
        try:
            # Build API URL for multiple coins
            ids_string = ','.join(self.crypto_symbols)
            url = f"{self.api_base_url}/coins/markets"
            
            params = {
                'vs_currency': 'usd',
                'ids': ids_string,
                'order': 'market_cap_desc',
                'per_page': len(self.crypto_symbols),
                'page': 1,
                'sparkline': 'true',
                'price_change_percentage': '1h,24h,7d,30d'
            }
            
            headers = {
                'User-Agent': 'CryptoKafkaProducer/1.0',
                'Accept': 'application/json'
            }
            
            if self.api_key:
                headers['x-cg-demo-api-key'] = self.api_key
            
            response = requests.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Normalize and enrich data
            normalized_data = []
            current_time = datetime.utcnow()
            
            for coin in data:
                normalized_coin = {
                    # Basic info
                    'id': coin.get('id'),
                    'symbol': coin.get('symbol', '').upper(),
                    'name': coin.get('name'),
                    
                    # Price data
                    'current_price_usd': coin.get('current_price'),
                    'market_cap_usd': coin.get('market_cap'),
                    'market_cap_rank': coin.get('market_cap_rank'),
                    'fully_diluted_valuation': coin.get('fully_diluted_valuation'),
                    'total_volume_usd': coin.get('total_volume'),
                    
                    # 24h data
                    'high_24h': coin.get('high_24h'),
                    'low_24h': coin.get('low_24h'),
                    'price_change_24h': coin.get('price_change_24h'),
                    'price_change_percentage_24h': coin.get('price_change_percentage_24h'),
                    'market_cap_change_24h': coin.get('market_cap_change_24h'),
                    'market_cap_change_percentage_24h': coin.get('market_cap_change_percentage_24h'),
                    
                    # Extended timeframe data
                    'price_change_percentage_1h': coin.get('price_change_percentage_1h_in_currency'),
                    'price_change_percentage_7d': coin.get('price_change_percentage_7d_in_currency'),
                    'price_change_percentage_30d': coin.get('price_change_percentage_30d_in_currency'),
                    
                    # Supply data
                    'circulating_supply': coin.get('circulating_supply'),
                    'total_supply': coin.get('total_supply'),
                    'max_supply': coin.get('max_supply'),
                    
                    # All-time data
                    'ath': coin.get('ath'),
                    'ath_change_percentage': coin.get('ath_change_percentage'),
                    'ath_date': coin.get('ath_date'),
                    'atl': coin.get('atl'),
                    'atl_change_percentage': coin.get('atl_change_percentage'),
                    'atl_date': coin.get('atl_date'),
                    
                    # Metadata
                    'last_updated': coin.get('last_updated'),
                    'sparkline_7d': coin.get('sparkline_in_7d', {}).get('price', []),
                    
                    # Producer metadata
                    'producer_timestamp': current_time.isoformat(),
                    'data_source': 'coingecko_markets',
                    'producer_version': '1.0',
                    'fetch_method': 'bulk_api'
                }
                
                # Calculate additional metrics
                if normalized_coin['current_price_usd'] and normalized_coin['ath']:
                    normalized_coin['distance_from_ath_percent'] = (
                        (normalized_coin['ath'] - normalized_coin['current_price_usd']) / 
                        normalized_coin['ath'] * 100
                    )
                
                normalized_data.append(normalized_coin)
            
            self.logger.info(f"Successfully fetched data for {len(normalized_data)} cryptocurrencies")
            return normalized_data
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request error: {str(e)}")
            return []
        except Exception as e:
            self.logger.error(f"Error fetching crypto data: {str(e)}")
            return []
    
    def send_to_kafka(self, data: List[Dict[str, Any]], topic: str = None) -> Dict[str, int]:
        """
        Send data to Kafka topic
        """
        if not topic:
            topic = self.topic_raw
        
        sent_count = 0
        failed_count = 0
        
        try:
            for record in data:
                try:
                    # Use symbol as partition key for consistent partitioning
                    key = record.get('symbol', 'unknown')
                    
                    # Send message
                    future = self.producer.send(
                        topic=topic,
                        key=key,
                        value=record
                    )
                    
                    # Get result (blocking call with timeout)
                    record_metadata = future.get(timeout=10)
                    sent_count += 1
                    
                    self.logger.debug(f"Sent {key} to {record_metadata.topic}[{record_metadata.partition}] @ offset {record_metadata.offset}")
                    
                except KafkaTimeoutError as e:
                    self.logger.error(f"Timeout sending message for {record.get('symbol', 'unknown')}: {str(e)}")
                    failed_count += 1
                except KafkaError as e:
                    self.logger.error(f"Kafka error sending message for {record.get('symbol', 'unknown')}: {str(e)}")
                    failed_count += 1
                except Exception as e:
                    self.logger.error(f"Unexpected error sending message for {record.get('symbol', 'unknown')}: {str(e)}")
                    failed_count += 1
            
            # Flush producer to ensure all messages are sent
            self.producer.flush(timeout=30)
            
            self.logger.info(f"Kafka send summary - Topic: {topic}, Sent: {sent_count}, Failed: {failed_count}")
            
            return {
                'sent': sent_count,
                'failed': failed_count,
                'total': len(data),
                'topic': topic
            }
            
        except Exception as e:
            self.logger.error(f"Error in send_to_kafka: {str(e)}")
            return {
                'sent': sent_count,
                'failed': failed_count + (len(data) - sent_count),
                'total': len(data),
                'topic': topic,
                'error': str(e)
            }
    
    def fetch_and_stream_continuous(self):
        """
        Continuously fetch and stream crypto data
        """
        self.logger.info(f"Starting continuous streaming every {self.fetch_interval} seconds")
        
        iteration = 0
        while True:
            try:
                iteration += 1
                start_time = time.time()
                
                self.logger.info(f"Iteration {iteration}: Fetching crypto data...")
                
                # Fetch data
                crypto_data = self.fetch_crypto_data()
                
                if crypto_data:
                    # Send to Kafka
                    result = self.send_to_kafka(crypto_data)
                    
                    fetch_time = time.time() - start_time
                    
                    self.logger.info(
                        f"Iteration {iteration} completed in {fetch_time:.2f}s - "
                        f"Sent: {result['sent']}, Failed: {result['failed']}"
                    )
                    
                    # Send metrics
                    metrics = {
                        'iteration': iteration,
                        'timestamp': datetime.utcnow().isoformat(),
                        'fetch_time_seconds': fetch_time,
                        'records_fetched': len(crypto_data),
                        'records_sent': result['sent'],
                        'records_failed': result['failed'],
                        'success_rate': (result['sent'] / len(crypto_data)) * 100 if crypto_data else 0,
                        'producer_id': 'crypto_kafka_producer',
                        'topic': result['topic']
                    }
                    
                    # Send metrics to separate topic
                    self.send_metrics(metrics)
                    
                else:
                    self.logger.warning(f"Iteration {iteration}: No data fetched")
                
                # Wait for next iteration
                sleep_time = max(0, self.fetch_interval - (time.time() - start_time))
                if sleep_time > 0:
                    self.logger.debug(f"Sleeping for {sleep_time:.2f} seconds")
                    time.sleep(sleep_time)
                
            except KeyboardInterrupt:
                self.logger.info("Received interrupt signal, stopping producer")
                break
            except Exception as e:
                self.logger.error(f"Error in continuous streaming iteration {iteration}: {str(e)}")
                time.sleep(60)  # Wait 1 minute before retrying
    
    def send_metrics(self, metrics: Dict[str, Any]):
        """
        Send producer metrics to Kafka
        """
        try:
            metrics_topic = 'crypto-producer-metrics'
            
            future = self.producer.send(
                topic=metrics_topic,
                key='producer_metrics',
                value=metrics
            )
            
            future.get(timeout=5)
            self.logger.debug("Producer metrics sent successfully")
            
        except Exception as e:
            self.logger.error(f"Error sending metrics: {str(e)}")
    
    def send_single_coin_data(self, coin_id: str) -> bool:
        """
        Fetch and send data for a single cryptocurrency
        """
        try:
            url = f"{self.api_base_url}/coins/{coin_id}"
            params = {
                'localization': 'false',
                'tickers': 'false',
                'market_data': 'true',
                'community_data': 'false',
                'developer_data': 'false'
            }
            
            headers = {'User-Agent': 'CryptoKafkaProducer/1.0'}
            if self.api_key:
                headers['x-cg-demo-api-key'] = self.api_key
            
            response = requests.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Normalize data
            normalized_data = {
                'id': data.get('id'),
                'symbol': data.get('symbol', '').upper(),
                'name': data.get('name'),
                'current_price_usd': data.get('market_data', {}).get('current_price', {}).get('usd'),
                'market_cap_usd': data.get('market_data', {}).get('market_cap', {}).get('usd'),
                'producer_timestamp': datetime.utcnow().isoformat(),
                'data_source': 'coingecko_single',
                'fetch_method': 'single_api'
            }
            
            # Send to Kafka
            result = self.send_to_kafka([normalized_data])
            
            return result['sent'] > 0
            
        except Exception as e:
            self.logger.error(f"Error fetching/sending single coin data for {coin_id}: {str(e)}")
            return False
    
    def get_producer_health(self) -> Dict[str, Any]:
        """
        Get producer health status
        """
        try:
            # Test Kafka connection
            metadata = self.producer.metrics()
            
            return {
                'status': 'healthy',
                'bootstrap_servers': self.bootstrap_servers,
                'topics': [self.topic_raw, self.topic_processed],
                'metrics': {
                    'buffer_available_bytes': metadata.get('buffer-available-bytes', {}).get('value', 0),
                    'buffer_total_bytes': metadata.get('buffer-total-bytes', {}).get('value', 0),
                    'connection_count': metadata.get('connection-count', {}).get('value', 0),
                    'failed_sends': metadata.get('record-send-total', {}).get('value', 0)
                },
                'last_check': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'last_check': datetime.utcnow().isoformat()
            }
    
    def close(self):
        """
        Close producer and clean up resources
        """
        try:
            if self.producer:
                self.producer.flush(timeout=30)
                self.producer.close(timeout=30)
                self.logger.info("Kafka producer closed successfully")
        except Exception as e:
            self.logger.error(f"Error closing producer: {str(e)}")

def main():
    """
    Main function to run the producer
    """
    producer = CryptoKafkaProducer()
    
    try:
        # Check producer health
        health = producer.get_producer_health()
        print(f"Producer health: {health['status']}")
        
        if health['status'] == 'healthy':
            # Start continuous streaming
            producer.fetch_and_stream_continuous()
        else:
            print(f"Producer unhealthy: {health.get('error', 'Unknown error')}")
            
    except KeyboardInterrupt:
        print("\nShutting down producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()