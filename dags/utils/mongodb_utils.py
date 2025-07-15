"""
MongoDB Utilities
Handles connections and operations with MongoDB for real-time data storage
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure, DuplicateKeyError, PyMongoError
import certifi

class MongoDBConnector:
    """
    MongoDB client for crypto data storage and retrieval
    """
    
    def __init__(self):
        self.host = os.getenv('MONGODB_HOST', 'mongodb')
        self.port = int(os.getenv('MONGODB_PORT', '27017'))
        self.username = os.getenv('MONGODB_USERNAME', 'crypto_user')
        self.password = os.getenv('MONGODB_PASSWORD', 'crypto_password')
        self.database_name = os.getenv('MONGODB_DATABASE', 'crypto_db')
        self.auth_source = os.getenv('MONGODB_AUTH_SOURCE', 'admin')
        
        self.collection_prices = os.getenv('MONGODB_COLLECTION_PRICES', 'crypto_prices')
        self.collection_market_data = os.getenv('MONGODB_COLLECTION_MARKET_DATA', 'market_data')
        self.collection_metrics = 'pipeline_metrics'
        
        self.client = None
        self.db = None
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
        # Initialize connection
        self._connect()
    
    def _connect(self):
        """
        Establish connection to MongoDB
        """
        try:
            # Build connection string
            if self.username and self.password:
                connection_string = f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/{self.database_name}?authSource={self.auth_source}"
            else:
                connection_string = f"mongodb://{self.host}:{self.port}/{self.database_name}"
            
            self.client = MongoClient(
                connection_string,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000,
                socketTimeoutMS=10000,
                maxPoolSize=10,
                tlsCAFile=certifi.where()
            )
            
            # Test connection
            self.client.admin.command('ping')
            self.db = self.client[self.database_name]
            
            # Create indexes for better performance
            self._create_indexes()
            
            self.logger.info("Successfully connected to MongoDB")
            
        except ConnectionFailure as e:
            self.logger.error(f"Failed to connect to MongoDB: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error connecting to MongoDB: {str(e)}")
            raise
    
    def _create_indexes(self):
        """
        Create necessary indexes for optimal performance
        """
        try:
            # Crypto prices collection indexes
            prices_collection = self.db[self.collection_prices]
            
            # Compound index for symbol and timestamp
            prices_collection.create_index([
                ('symbol', ASCENDING),
                ('fetch_timestamp', DESCENDING)
            ], name='symbol_timestamp_idx')
            
            # Index for timestamp queries
            prices_collection.create_index([
                ('fetch_timestamp', DESCENDING)
            ], name='timestamp_idx')
            
            # Index for market cap rank
            prices_collection.create_index([
                ('market_cap_rank', ASCENDING)
            ], name='market_cap_rank_idx')
            
            # Unique index to prevent duplicates
            prices_collection.create_index([
                ('symbol', ASCENDING),
                ('fetch_timestamp', ASCENDING),
                ('data_source', ASCENDING)
            ], unique=True, name='unique_price_record_idx')
            
            # Market data collection indexes
            market_collection = self.db[self.collection_market_data]
            market_collection.create_index([
                ('fetch_timestamp', DESCENDING)
            ], name='market_timestamp_idx')
            
            # Pipeline metrics collection indexes
            metrics_collection = self.db[self.collection_metrics]
            metrics_collection.create_index([
                ('pipeline_run_timestamp', DESCENDING)
            ], name='metrics_timestamp_idx')
            
            self.logger.info("MongoDB indexes created successfully")
            
        except Exception as e:
            self.logger.warning(f"Error creating indexes: {str(e)}")
    
    def test_connection(self) -> bool:
        """
        Test MongoDB connection
        """
        try:
            self.client.admin.command('ping')
            return True
        except Exception as e:
            self.logger.error(f"MongoDB connection test failed: {str(e)}")
            return False
    
    def insert_crypto_data(self, data: Dict[str, Any]) -> bool:
        """
        Insert cryptocurrency data into MongoDB
        """
        try:
            collection = self.db[self.collection_prices]
            
            # Add metadata
            data_with_metadata = {
                **data,
                'inserted_at': datetime.utcnow(),
                'document_version': 1
            }
            
            result = collection.insert_one(data_with_metadata)
            
            if result.inserted_id:
                self.logger.debug(f"Inserted crypto data for {data.get('symbol', 'unknown')}")
                return True
            else:
                self.logger.error("Failed to insert crypto data")
                return False
                
        except DuplicateKeyError:
            self.logger.warning(f"Duplicate crypto data for {data.get('symbol', 'unknown')} at {data.get('fetch_timestamp')}")
            return True  # Consider duplicate as success
        except PyMongoError as e:
            self.logger.error(f"MongoDB error inserting crypto data: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error inserting crypto data: {str(e)}")
            return False
    
    def insert_market_data(self, data: Dict[str, Any]) -> bool:
        """
        Insert market overview data into MongoDB
        """
        try:
            collection = self.db[self.collection_market_data]
            
            data_with_metadata = {
                **data,
                'inserted_at': datetime.utcnow(),
                'document_version': 1
            }
            
            result = collection.insert_one(data_with_metadata)
            
            if result.inserted_id:
                self.logger.debug("Inserted market data")
                return True
            else:
                self.logger.error("Failed to insert market data")
                return False
                
        except PyMongoError as e:
            self.logger.error(f"MongoDB error inserting market data: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error inserting market data: {str(e)}")
            return False
    
    def insert_pipeline_metrics(self, metrics: Dict[str, Any]) -> bool:
        """
        Insert pipeline metrics into MongoDB
        """
        try:
            collection = self.db[self.collection_metrics]
            
            metrics_with_metadata = {
                **metrics,
                'inserted_at': datetime.utcnow()
            }
            
            result = collection.insert_one(metrics_with_metadata)
            
            if result.inserted_id:
                self.logger.debug("Inserted pipeline metrics")
                return True
            else:
                self.logger.error("Failed to insert pipeline metrics")
                return False
                
        except PyMongoError as e:
            self.logger.error(f"MongoDB error inserting metrics: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error inserting metrics: {str(e)}")
            return False
    
    def get_latest_crypto_prices(self, symbols: List[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get latest cryptocurrency prices
        """
        try:
            collection = self.db[self.collection_prices]
            
            # Build query
            query = {}
            if symbols:
                query['symbol'] = {'$in': [s.upper() for s in symbols]}
            
            # Get latest prices for each symbol
            pipeline = [
                {'$match': query},
                {'$sort': {'fetch_timestamp': -1}},
                {'$group': {
                    '_id': '$symbol',
                    'latest_data': {'$first': '$ROOT'}
                }},
                {'$replaceRoot': {'newRoot': '$latest_data'}},
                {'$limit': limit}
            ]
            
            results = list(collection.aggregate(pipeline))
            self.logger.debug(f"Retrieved {len(results)} latest crypto prices")
            return results
            
        except PyMongoError as e:
            self.logger.error(f"MongoDB error getting latest prices: {str(e)}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error getting latest prices: {str(e)}")
            return []
    
    def get_price_history(self, symbol: str, hours: int = 24) -> List[Dict[str, Any]]:
        """
        Get price history for a specific cryptocurrency
        """
        try:
            collection = self.db[self.collection_prices]
            
            start_time = datetime.utcnow() - timedelta(hours=hours)
            
            query = {
                'symbol': symbol.upper(),
                'fetch_timestamp': {'$gte': start_time.isoformat()}
            }
            
            results = list(collection.find(query).sort('fetch_timestamp', ASCENDING))
            self.logger.debug(f"Retrieved {len(results)} price history records for {symbol}")
            return results
            
        except PyMongoError as e:
            self.logger.error(f"MongoDB error getting price history: {str(e)}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error getting price history: {str(e)}")
            return []
    
    def get_top_gainers_losers(self, limit: int = 10) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get top gainers and losers based on 24h price change
        """
        try:
            collection = self.db[self.collection_prices]
            
            # Get latest data for each symbol
            pipeline = [
                {'$sort': {'fetch_timestamp': -1}},
                {'$group': {
                    '_id': '$symbol',
                    'latest_data': {'$first': '$ROOT'}
                }},
                {'$replaceRoot': {'newRoot': '$latest_data'}},
                {'$match': {'price_change_percentage_24h': {'$exists': True, '$ne': None}}}
            ]
            
            latest_data = list(collection.aggregate(pipeline))
            
            # Sort by price change percentage
            gainers = sorted(
                [d for d in latest_data if d.get('price_change_percentage_24h', 0) > 0],
                key=lambda x: x.get('price_change_percentage_24h', 0),
                reverse=True
            )[:limit]
            
            losers = sorted(
                [d for d in latest_data if d.get('price_change_percentage_24h', 0) < 0],
                key=lambda x: x.get('price_change_percentage_24h', 0)
            )[:limit]
            
            return {
                'gainers': gainers,
                'losers': losers
            }
            
        except PyMongoError as e:
            self.logger.error(f"MongoDB error getting gainers/losers: {str(e)}")
            return {'gainers': [], 'losers': []}
        except Exception as e:
            self.logger.error(f"Unexpected error getting gainers/losers: {str(e)}")
            return {'gainers': [], 'losers': []}
    
    def get_market_summary(self) -> Dict[str, Any]:
        """
        Get market summary statistics
        """
        try:
            prices_collection = self.db[self.collection_prices]
            market_collection = self.db[self.collection_market_data]
            
            # Get latest market data
            latest_market = market_collection.find_one(
                sort=[('fetch_timestamp', DESCENDING)]
            )
            
            # Get total number of tracked cryptocurrencies
            total_cryptos = prices_collection.distinct('symbol')
            
            # Get average price change
            pipeline = [
                {'$sort': {'fetch_timestamp': -1}},
                {'$group': {
                    '_id': '$symbol',
                    'latest_data': {'$first': '$ROOT'}
                }},
                {'$replaceRoot': {'newRoot': '$latest_data'}},
                {'$match': {'price_change_percentage_24h': {'$exists': True, '$ne': None}}},
                {'$group': {
                    '_id': None,
                    'avg_price_change_24h': {'$avg': '$price_change_percentage_24h'},
                    'total_market_cap': {'$sum': '$market_cap_usd'},
                    'total_volume': {'$sum': '$total_volume_usd'}
                }}
            ]
            
            stats = list(prices_collection.aggregate(pipeline))
            
            summary = {
                'total_cryptocurrencies_tracked': len(total_cryptos),
                'last_updated': datetime.utcnow().isoformat(),
                'data_source': 'mongodb_aggregation'
            }
            
            if latest_market:
                summary.update({
                    'global_market_cap_usd': latest_market.get('total_market_cap_usd'),
                    'global_volume_24h_usd': latest_market.get('total_volume_24h_usd'),
                    'bitcoin_dominance': latest_market.get('bitcoin_dominance_percentage'),
                    'market_cap_change_24h': latest_market.get('market_cap_change_percentage_24h')
                })
            
            if stats:
                summary.update({
                    'average_price_change_24h': stats[0].get('avg_price_change_24h'),
                    'tracked_total_market_cap': stats[0].get('total_market_cap'),
                    'tracked_total_volume': stats[0].get('total_volume')
                })
            
            return summary
            
        except PyMongoError as e:
            self.logger.error(f"MongoDB error getting market summary: {str(e)}")
            return {}
        except Exception as e:
            self.logger.error(f"Unexpected error getting market summary: {str(e)}")
            return {}
    
    def cleanup_old_data(self, cutoff_date: datetime) -> int:
        """
        Remove old data based on retention policy
        """
        try:
            total_deleted = 0
            
            # Clean crypto prices
            prices_result = self.db[self.collection_prices].delete_many({
                'fetch_timestamp': {'$lt': cutoff_date.isoformat()}
            })
            total_deleted += prices_result.deleted_count
            
            # Clean market data
            market_result = self.db[self.collection_market_data].delete_many({
                'fetch_timestamp': {'$lt': cutoff_date.isoformat()}
            })
            total_deleted += market_result.deleted_count
            
            # Clean old metrics (keep last 90 days)
            metrics_cutoff = datetime.utcnow() - timedelta(days=90)
            metrics_result = self.db[self.collection_metrics].delete_many({
                'pipeline_run_timestamp': {'$lt': metrics_cutoff.isoformat()}
            })
            total_deleted += metrics_result.deleted_count
            
            self.logger.info(f"Cleaned up {total_deleted} old records from MongoDB")
            return total_deleted
            
        except PyMongoError as e:
            self.logger.error(f"MongoDB error during cleanup: {str(e)}")
            return 0
        except Exception as e:
            self.logger.error(f"Unexpected error during cleanup: {str(e)}")
            return 0
    
    def get_pipeline_metrics(self, hours: int = 24) -> List[Dict[str, Any]]:
        """
        Get pipeline performance metrics
        """
        try:
            collection = self.db[self.collection_metrics]
            
            start_time = datetime.utcnow() - timedelta(hours=hours)
            
            query = {
                'pipeline_run_timestamp': {'$gte': start_time.isoformat()}
            }
            
            results = list(collection.find(query).sort('pipeline_run_timestamp', DESCENDING))
            self.logger.debug(f"Retrieved {len(results)} pipeline metrics")
            return results
            
        except PyMongoError as e:
            self.logger.error(f"MongoDB error getting pipeline metrics: {str(e)}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error getting pipeline metrics: {str(e)}")
            return []
    
    def create_aggregated_views(self):
        """
        Create materialized views for common queries
        """
        try:
            # Create hourly aggregated data view
            hourly_pipeline = [
                {
                    '$addFields': {
                        'hour': {
                            '$dateFromString': {
                                'dateString': '$fetch_timestamp',
                                'format': '%Y-%m-%dT%H:00:00.000Z'
                            }
                        }
                    }
                },
                {
                    '$group': {
                        '_id': {
                            'symbol': '$symbol',
                            'hour': '$hour'
                        },
                        'avg_price': {'$avg': '$current_price_usd'},
                        'max_price': {'$max': '$current_price_usd'},
                        'min_price': {'$min': '$current_price_usd'},
                        'avg_volume': {'$avg': '$total_volume_usd'},
                        'data_points': {'$sum': 1},
                        'last_updated': {'$max': '$fetch_timestamp'}
                    }
                },
                {
                    '$out': 'crypto_prices_hourly'
                }
            ]
            
            self.db[self.collection_prices].aggregate(hourly_pipeline)
            self.logger.info("Created hourly aggregated view")
            
        except Exception as e:
            self.logger.error(f"Error creating aggregated views: {str(e)}")
    
    def get_database_stats(self) -> Dict[str, Any]:
        """
        Get database statistics and health information
        """
        try:
            stats = self.db.command('dbStats')
            
            # Get collection stats
            collections_stats = {}
            for collection_name in [self.collection_prices, self.collection_market_data, self.collection_metrics]:
                try:
                    coll_stats = self.db.command('collStats', collection_name)
                    collections_stats[collection_name] = {
                        'document_count': coll_stats.get('count', 0),
                        'size_bytes': coll_stats.get('size', 0),
                        'avg_obj_size': coll_stats.get('avgObjSize', 0),
                        'indexes': coll_stats.get('nindexes', 0)
                    }
                except Exception:
                    collections_stats[collection_name] = {'error': 'Unable to get stats'}
            
            return {
                'database_name': self.database_name,
                'total_size_bytes': stats.get('dataSize', 0),
                'total_documents': sum([cs.get('document_count', 0) for cs in collections_stats.values() if 'error' not in cs]),
                'collections': collections_stats,
                'timestamp': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error getting database stats: {str(e)}")
            return {}
    
    def close(self):
        """
        Close MongoDB connection
        """
        if self.client:
            self.client.close()
            self.logger.info("MongoDB connection closed")