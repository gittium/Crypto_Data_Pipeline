// MongoDB Initialization Script
// Creates database, user, and initial collections for crypto data pipeline

print('Starting MongoDB initialization for crypto data pipeline...');

// Switch to crypto database
db = db.getSiblingDB('crypto_db');

// Create user for the application
try {
    db.createUser({
        user: 'crypto_user',
        pwd: 'crypto_password',
        roles: [
            {
                role: 'readWrite',
                db: 'crypto_db'
            }
        ]
    });
    print('✅ Created crypto_user successfully');
} catch (error) {
    print('⚠️  User crypto_user might already exist:', error.message);
}

// Create collections with validation schemas
try {
    // Crypto prices collection
    db.createCollection('crypto_prices', {
        validator: {
            $jsonSchema: {
                bsonType: 'object',
                required: ['symbol', 'fetch_timestamp'],
                properties: {
                    symbol: {
                        bsonType: 'string',
                        description: 'Cryptocurrency symbol (e.g., BTC, ETH)'
                    },
                    name: {
                        bsonType: 'string',
                        description: 'Full name of the cryptocurrency'
                    },
                    current_price_usd: {
                        bsonType: ['double', 'null'],
                        minimum: 0,
                        description: 'Current price in USD'
                    },
                    market_cap_usd: {
                        bsonType: ['long', 'double', 'null'],
                        minimum: 0,
                        description: 'Market capitalization in USD'
                    },
                    total_volume_usd: {
                        bsonType: ['long', 'double', 'null'],
                        minimum: 0,
                        description: '24h trading volume in USD'
                    },
                    price_change_percentage_24h: {
                        bsonType: ['double', 'null'],
                        description: '24h price change percentage'
                    },
                    market_cap_rank: {
                        bsonType: ['int', 'null'],
                        minimum: 1,
                        description: 'Market cap ranking'
                    },
                    fetch_timestamp: {
                        bsonType: 'string',
                        description: 'ISO timestamp when data was fetched'
                    },
                    data_source: {
                        bsonType: 'string',
                        description: 'Source of the data (e.g., coingecko, coinapi)'
                    }
                }
            }
        }
    });
    print('✅ Created crypto_prices collection with schema validation');
} catch (error) {
    print('⚠️  crypto_prices collection might already exist:', error.message);
}

try {
    // Market data collection for global market statistics
    db.createCollection('market_data', {
        validator: {
            $jsonSchema: {
                bsonType: 'object',
                required: ['fetch_timestamp', 'data_source'],
                properties: {
                    total_market_cap_usd: {
                        bsonType: ['long', 'double', 'null'],
                        minimum: 0,
                        description: 'Total cryptocurrency market cap in USD'
                    },
                    total_volume_24h_usd: {
                        bsonType: ['long', 'double', 'null'],
                        minimum: 0,
                        description: 'Total 24h trading volume in USD'
                    },
                    bitcoin_dominance_percentage: {
                        bsonType: ['double', 'null'],
                        minimum: 0,
                        maximum: 100,
                        description: 'Bitcoin market cap dominance percentage'
                    },
                    ethereum_dominance_percentage: {
                        bsonType: ['double', 'null'],
                        minimum: 0,
                        maximum: 100,
                        description: 'Ethereum market cap dominance percentage'
                    },
                    active_cryptocurrencies: {
                        bsonType: ['int', 'null'],
                        minimum: 0,
                        description: 'Number of active cryptocurrencies'
                    },
                    fetch_timestamp: {
                        bsonType: 'string',
                        description: 'ISO timestamp when data was fetched'
                    },
                    data_source: {
                        bsonType: 'string',
                        description: 'Source of the data'
                    }
                }
            }
        }
    });
    print('✅ Created market_data collection with schema validation');
} catch (error) {
    print('⚠️  market_data collection might already exist:', error.message);
}

try {
    // Pipeline metrics collection for monitoring
    db.createCollection('pipeline_metrics', {
        validator: {
            $jsonSchema: {
                bsonType: 'object',
                required: ['pipeline_run_timestamp'],
                properties: {
                    pipeline_run_timestamp: {
                        bsonType: 'string',
                        description: 'ISO timestamp of pipeline run'
                    },
                    kafka_messages_sent: {
                        bsonType: ['int', 'long'],
                        minimum: 0,
                        description: 'Number of messages sent to Kafka'
                    },
                    mongodb_records_processed: {
                        bsonType: ['int', 'long'],
                        minimum: 0,
                        description: 'Number of records processed to MongoDB'
                    },
                    s3_records_uploaded: {
                        bsonType: ['int', 'long'],
                        minimum: 0,
                        description: 'Number of records uploaded to S3'
                    },
                    pipeline_success_rate: {
                        bsonType: 'double',
                        minimum: 0,
                        maximum: 100,
                        description: 'Pipeline success rate percentage'
                    }
                }
            }
        }
    });
    print('✅ Created pipeline_metrics collection with schema validation');
} catch (error) {
    print('⚠️  pipeline_metrics collection might already exist:', error.message);
}

// Create indexes for optimal performance
print('Creating indexes for optimal performance...');

try {
    // Crypto prices indexes
    db.crypto_prices.createIndex({ symbol: 1, fetch_timestamp: -1 }, { name: 'symbol_timestamp_idx' });
    db.crypto_prices.createIndex({ fetch_timestamp: -1 }, { name: 'timestamp_idx' });
    db.crypto_prices.createIndex({ market_cap_rank: 1 }, { name: 'market_cap_rank_idx' });
    db.crypto_prices.createIndex({ 
        symbol: 1, 
        fetch_timestamp: 1, 
        data_source: 1 
    }, { 
        unique: true, 
        name: 'unique_price_record_idx',
        partialFilterExpression: {
            symbol: { $exists: true },
            fetch_timestamp: { $exists: true },
            data_source: { $exists: true }
        }
    });
    db.crypto_prices.createIndex({ current_price_usd: 1 }, { name: 'price_idx' });
    db.crypto_prices.createIndex({ market_cap_usd: -1 }, { name: 'market_cap_idx' });
    
    print('✅ Created indexes for crypto_prices collection');
} catch (error) {
    print('⚠️  Error creating crypto_prices indexes:', error.message);
}

try {
    // Market data indexes
    db.market_data.createIndex({ fetch_timestamp: -1 }, { name: 'market_timestamp_idx' });
    db.market_data.createIndex({ data_source: 1 }, { name: 'market_source_idx' });
    
    print('✅ Created indexes for market_data collection');
} catch (error) {
    print('⚠️  Error creating market_data indexes:', error.message);
}

try {
    // Pipeline metrics indexes
    db.pipeline_metrics.createIndex({ pipeline_run_timestamp: -1 }, { name: 'metrics_timestamp_idx' });
    db.pipeline_metrics.createIndex({ pipeline_success_rate: -1 }, { name: 'success_rate_idx' });
    
    print('✅ Created indexes for pipeline_metrics collection');
} catch (error) {
    print('⚠️  Error creating pipeline_metrics indexes:', error.message);
}

// Insert sample configuration document
try {
    db.config.insertOne({
        _id: 'pipeline_config',
        version: '1.0',
        created_at: new Date(),
        settings: {
            data_retention_days: 30,
            max_records_per_batch: 1000,
            api_rate_limit_per_minute: 100,
            supported_cryptocurrencies: [
                'bitcoin', 'ethereum', 'cardano', 'polkadot', 'chainlink',
                'litecoin', 'stellar', 'dogecoin', 'binancecoin', 'ripple'
            ],
            alert_thresholds: {
                price_change_24h_percent: 10,
                volume_spike_multiplier: 5,
                market_cap_change_percent: 15
            }
        }
    });
    print('✅ Created pipeline configuration document');
} catch (error) {
    print('⚠️  Configuration document might already exist:', error.message);
}

// Insert sample data for testing (optional)
try {
    var sampleData = [
        {
            symbol: 'BTC',
            name: 'Bitcoin',
            current_price_usd: 45000.00,
            market_cap_usd: NumberLong("850000000000"),
            total_volume_usd: NumberLong("15000000000"),
            price_change_percentage_24h: 2.5,
            market_cap_rank: 1,
            fetch_timestamp: new Date().toISOString(),
            data_source: 'sample_data',
            inserted_at: new Date(),
            document_version: 1
        },
        {
            symbol: 'ETH',
            name: 'Ethereum',
            current_price_usd: 3200.00,
            market_cap_usd: NumberLong("380000000000"),
            total_volume_usd: NumberLong("8000000000"),
            price_change_percentage_24h: 1.8,
            market_cap_rank: 2,
            fetch_timestamp: new Date().toISOString(),
            data_source: 'sample_data',
            inserted_at: new Date(),
            document_version: 1
        }
    ];
    
    db.crypto_prices.insertMany(sampleData);
    print('✅ Inserted sample crypto data for testing');
} catch (error) {
    print('⚠️  Error inserting sample data:', error.message);
}

// Create a view for latest prices
try {
    db.createView('latest_crypto_prices', 'crypto_prices', [
        {
            $sort: { fetch_timestamp: -1 }
        },
        {
            $group: {
                _id: '$symbol',
                latest_data: { $first: '$$ROOT' }
            }
        },
        {
            $replaceRoot: { newRoot: '$latest_data' }
        },
        {
            $sort: { market_cap_rank: 1 }
        }
    ]);
    print('✅ Created latest_crypto_prices view');
} catch (error) {
    print('⚠️  View might already exist:', error.message);
}

// Create a view for top movers
try {
    db.createView('top_movers_24h', 'latest_crypto_prices', [
        {
            $match: {
                price_change_percentage_24h: { $exists: true, $ne: null }
            }
        },
        {
            $addFields: {
                abs_price_change: { $abs: '$price_change_percentage_24h' }
            }
        },
        {
            $sort: { abs_price_change: -1 }
        },
        {
            $limit: 20
        },
        {
            $project: {
                symbol: 1,
                name: 1,
                current_price_usd: 1,
                price_change_percentage_24h: 1,
                market_cap_usd: 1,
                market_cap_rank: 1,
                fetch_timestamp: 1
            }
        }
    ]);
    print('✅ Created top_movers_24h view');
} catch (error) {
    print('⚠️  View might already exist:', error.message);
}

// Show collection stats
print('\n📊 Database Statistics:');
try {
    var stats = db.stats();
    print('Database:', stats.db);
    print('Collections:', stats.collections);
    print('Data Size:', (stats.dataSize / 1024 / 1024).toFixed(2), 'MB');
    print('Index Size:', (stats.indexSize / 1024 / 1024).toFixed(2), 'MB');
} catch (error) {
    print('⚠️  Could not get database stats:', error.message);
}

print('\n📋 Collection List:');
try {
    db.getCollectionNames().forEach(function(collection) {
        var count = db.getCollection(collection).countDocuments();
        print('-', collection + ':', count, 'documents');
    });
} catch (error) {
    print('⚠️  Could not list collections:', error.message);
}

print('\n🎉 MongoDB initialization completed successfully!');
print('✅ Database: crypto_db');
print('✅ User: crypto_user');
print('✅ Collections: crypto_prices, market_data, pipeline_metrics');
print('✅ Indexes: Optimized for queries');
print('✅ Views: latest_crypto_prices, top_movers_24h');
print('✅ Sample data: Ready for testing');

print('\n🔗 Connection string:');
print('mongodb://crypto_user:crypto_password@mongodb:27017/crypto_db?authSource=crypto_db');

print('\n📖 Next steps:');
print('1. Start the Airflow pipeline');
print('2. Run the Kafka producer');
print('3. Monitor data ingestion in Grafana');
print('4. Query data using MongoDB Compass or CLI');