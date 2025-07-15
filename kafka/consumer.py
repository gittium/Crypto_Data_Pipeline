"""
Kafka Consumer for Crypto Data
Handles real-time processing of cryptocurrency data streams
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import signal
import sys

class CryptoKafkaConsumer:
    """
    Kafka consumer for processing cryptocurrency data streams
    """
    
    def __init__(self, consumer_group: str = None):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
        self.consumer_group = consumer_group or os.getenv('KAFKA_CONSUMER_GROUP', 'crypto-pipeline-group')
        self.topics = [
            os.getenv('KAFKA_TOPIC_CRYPTO_RAW', 'crypto-raw-data'),
            os.getenv('KAFKA_TOPIC_CRYPTO_PROCESSED', 'crypto-processed-data')
        ]
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Consumer settings
        self.auto_offset_reset = os.getenv('KAFKA_AUTO_OFFSET_RESET', 'latest')
        self.enable_auto_commit = os.getenv('KAFKA_ENABLE_AUTO_COMMIT', 'true').lower() == 'true'
        self.max_poll_records = int(os.getenv('KAFKA_MAX_POLL_RECORDS', '500'))
        
        # Processing settings
        self.batch_size = int(os.getenv('CONSUMER_BATCH_SIZE', '10'))
        self.processing_timeout = int(os.getenv('PROCESSING_TIMEOUT', '30'))
        
        # Create consumer
        self.consumer = self._create_consumer()
        
        # Statistics
        self.stats = {
            'messages_processed': 0,
            'messages_failed': 0,
            'start_time': datetime.utcnow(),
            'last_message_time': None
        }
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        self.shutdown_requested = False
    
    def _create_consumer(self) -> KafkaConsumer:
        """
        Create and configure Kafka consumer
        """
        try:
            consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.consumer_group,
                auto_offset_reset=self.auto_offset_reset,
                enable_auto_commit=self.enable_auto_commit,
                auto_commit_interval_ms=5000,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                max_poll_records=self.max_poll_records,
                max_poll_interval_ms=300000,  # 5 minutes
                fetch_min_bytes=1,
                fetch_max_wait_ms=500,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                key_deserializer=lambda m: m.decode('utf-8') if m else None,
                consumer_timeout_ms=1000,  # 1 second timeout for polling
            )
            
            self.logger.info(f"Kafka consumer created for group: {self.consumer_group}")
            return consumer
            
        except Exception as e:
            self.logger.error(f"Error creating Kafka consumer: {str(e)}")
            raise
    
    def _signal_handler(self, signum, frame):
        """
        Handle shutdown signals gracefully
        """
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.shutdown_requested = True
    
    def process_message(self, message) -> bool:
        """
        Process a single Kafka message
        """
        try:
            # Extract message data
            topic = message.topic
            partition = message.partition
            offset = message.offset
            key = message.key
            value = message.value
            timestamp = datetime.fromtimestamp(message.timestamp / 1000) if message.timestamp else None
            
            self.logger.debug(f"Processing message from {topic}[{partition}] @ offset {offset}")
            
            # Validate message structure
            if not isinstance(value, dict):
                self.logger.warning(f"Invalid message format in {topic}[{partition}] @ {offset}")
                return False
            
            # Add processing metadata
            processed_data = {
                **value,
                'kafka_metadata': {
                    'topic': topic,
                    'partition': partition,
                    'offset': offset,
                    'key': key,
                    'timestamp': timestamp.isoformat() if timestamp else None,
                    'consumer_group': self.consumer_group,
                    'processing_timestamp': datetime.utcnow().isoformat()
                }
            }
            
            # Route message based on topic
            if topic == os.getenv('KAFKA_TOPIC_CRYPTO_RAW', 'crypto-raw-data'):
                success = self._process_raw_crypto_data(processed_data)
            elif topic == os.getenv('KAFKA_TOPIC_CRYPTO_PROCESSED', 'crypto-processed-data'):
                success = self._process_processed_crypto_data(processed_data)
            else:
                self.logger.warning(f"Unknown topic: {topic}")
                success = False
            
            if success:
                self.stats['messages_processed'] += 1
                self.stats['last_message_time'] = datetime.utcnow()
            else:
                self.stats['messages_failed'] += 1
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error processing message: {str(e)}")
            self.stats['messages_failed'] += 1
            return False
    
    def _process_raw_crypto_data(self, data: Dict[str, Any]) -> bool:
        """
        Process raw cryptocurrency data
        """
        try:
            symbol = data.get('symbol', 'UNKNOWN')
            current_price = data.get('current_price_usd')
            
            if not current_price:
                self.logger.warning(f"Missing price data for {symbol}")
                return False
            
            # Perform data validation and enrichment
            enriched_data = self._enrich_crypto_data(data)
            
            # Here you could:
            # 1. Store in MongoDB for real-time dashboards
            # 2. Apply real-time analytics
            # 3. Trigger alerts based on price changes
            # 4. Forward to other systems
            
            self.logger.debug(f"Processed raw data for {symbol}: ${current_price}")
            
            # Example: Log significant price movements
            price_change_24h = data.get('price_change_percentage_24h')
            if price_change_24h and abs(price_change_24h) > 10:
                self.logger.info(f"ALERT: {symbol} has {price_change_24h:.2f}% price change in 24h")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing raw crypto data: {str(e)}")
            return False
    
    def _process_processed_crypto_data(self, data: Dict[str, Any]) -> bool:
        """
        Process already processed cryptocurrency data
        """
        try:
            symbol = data.get('symbol', 'UNKNOWN')
            
            # Handle processed data (e.g., aggregated data, technical indicators)
            self.logger.debug(f"Processed processed data for {symbol}")
            
            # Example processing: Calculate moving averages, RSI, etc.
            # This is where you'd implement more sophisticated analytics
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing processed crypto data: {str(e)}")
            return False
    
    def _enrich_crypto_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich cryptocurrency data with additional calculated fields
        """
        try:
            enriched = data.copy()
            
            # Calculate additional metrics
            current_price = data.get('current_price_usd')
            market_cap = data.get('market_cap_usd')
            volume = data.get('total_volume_usd')
            
            if current_price and market_cap:
                # Calculate volume to market cap ratio
                enriched['volume_market_cap_ratio'] = volume / market_cap if volume and market_cap else None
            
            # Add price categories
            if current_price:
                if current_price < 0.01:
                    enriched['price_category'] = 'micro'
                elif current_price < 1:
                    enriched['price_category'] = 'low'
                elif current_price < 100:
                    enriched['price_category'] = 'medium'
                elif current_price < 1000:
                    enriched['price_category'] = 'high'
                else:
                    enriched['price_category'] = 'ultra_high'
            
            # Add market cap categories
            if market_cap:
                if market_cap < 1e6:  # $1M
                    enriched['market_cap_category'] = 'nano'
                elif market_cap < 1e8:  # $100M
                    enriched['market_cap_category'] = 'micro'
                elif market_cap < 1e9:  # $1B
                    enriched['market_cap_category'] = 'small'
                elif market_cap < 1e10:  # $10B
                    enriched['market_cap_category'] = 'medium'
                elif market_cap < 1e11:  # $100B
                    enriched['market_cap_category'] = 'large'
                else:
                    enriched['market_cap_category'] = 'mega'
            
            # Add volatility indicators
            price_change_24h = data.get('price_change_percentage_24h')
            if price_change_24h is not None:
                abs_change = abs(price_change_24h)
                if abs_change < 2:
                    enriched['volatility_24h'] = 'low'
                elif abs_change < 5:
                    enriched['volatility_24h'] = 'medium'
                elif abs_change < 10:
                    enriched['volatility_24h'] = 'high'
                else:
                    enriched['volatility_24h'] = 'extreme'
            
            # Add processing timestamp
            enriched['enrichment_timestamp'] = datetime.utcnow().isoformat()
            
            return enriched
            
        except Exception as e:
            self.logger.error(f"Error enriching crypto data: {str(e)}")
            return data
    
    def consume_messages(self):
        """
        Main consumer loop
        """
        self.logger.info(f"Starting consumer for topics: {self.topics}")
        self.logger.info(f"Consumer group: {self.consumer_group}")
        
        try:
            while not self.shutdown_requested:
                try:
                    # Poll for messages
                    message_batch = self.consumer.poll(timeout_ms=1000)
                    
                    if not message_batch:
                        continue
                    
                    # Process messages by topic partition
                    for topic_partition, messages in message_batch.items():
                        self.logger.debug(f"Processing {len(messages)} messages from {topic_partition}")
                        
                        for message in messages:
                            if self.shutdown_requested:
                                break
                            
                            success = self.process_message(message)
                            
                            if not success:
                                # Handle failed message processing
                                self.logger.error(f"Failed to process message from {topic_partition} @ offset {message.offset}")
                    
                    # Commit offsets if auto-commit is disabled
                    if not self.enable_auto_commit:
                        self.consumer.commit()
                
                except Exception as e:
                    self.logger.error(f"Error in consumer loop: {str(e)}")
                    # Continue processing other messages
                    continue
        
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt")
        except Exception as e:
            self.logger.error(f"Fatal error in consumer: {str(e)}")
        finally:
            self.close()
    
    def get_consumer_stats(self) -> Dict[str, Any]:
        """
        Get consumer statistics
        """
        current_time = datetime.utcnow()
        uptime = current_time - self.stats['start_time']
        
        return {
            'consumer_group': self.consumer_group,
            'topics': self.topics,
            'messages_processed': self.stats['messages_processed'],
            'messages_failed': self.stats['messages_failed'],
            'success_rate': (
                self.stats['messages_processed'] / 
                (self.stats['messages_processed'] + self.stats['messages_failed'])
                * 100
            ) if (self.stats['messages_processed'] + self.stats['messages_failed']) > 0 else 0,
            'uptime_seconds': uptime.total_seconds(),
            'last_message_time': self.stats['last_message_time'].isoformat() if self.stats['last_message_time'] else None,
            'current_time': current_time.isoformat()
        }
    
    def get_topic_offsets(self) -> Dict[str, Dict[str, int]]:
        """
        Get current topic partition offsets
        """
        try:
            offsets = {}
            
            for topic in self.topics:
                partitions = self.consumer.partitions_for_topic(topic)
                if partitions:
                    topic_offsets = {}
                    for partition in partitions:
                        tp = (topic, partition)
                        try:
                            position = self.consumer.position(tp)
                            topic_offsets[str(partition)] = position
                        except Exception as e:
                            topic_offsets[str(partition)] = f"error: {str(e)}"
                    offsets[topic] = topic_offsets
            
            return offsets
            
        except Exception as e:
            self.logger.error(f"Error getting topic offsets: {str(e)}")
            return {}
    
    def reset_offsets(self, reset_type: str = 'latest'):
        """
        Reset consumer offsets
        """
        try:
            if reset_type == 'latest':
                self.consumer.seek_to_end()
            elif reset_type == 'earliest':
                self.consumer.seek_to_beginning()
            else:
                self.logger.error(f"Invalid reset type: {reset_type}")
                return False
            
            self.logger.info(f"Reset offsets to {reset_type}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error resetting offsets: {str(e)}")
            return