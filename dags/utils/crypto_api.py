"""
Crypto API Client
Handles data fetching from various cryptocurrency APIs
"""

import os
import requests
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
import logging

class CryptoAPIClient:
    """
    Client for fetching cryptocurrency data from multiple sources
    """
    
    def __init__(self):
        self.coingecko_api_key = os.getenv('COINGECKO_API_KEY')
        self.coinapi_key = os.getenv('COINAPI_KEY')
        self.base_url = os.getenv('CRYPTO_API_BASE_URL', 'https://api.coingecko.com/api/v3')
        self.rate_limit = int(os.getenv('CRYPTO_API_RATE_LIMIT', '100'))
        self.last_request_time = 0
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
    def _rate_limit_sleep(self):
        """
        Implement rate limiting between API calls
        """
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        min_interval = 60 / self.rate_limit  # requests per minute
        
        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _make_request(self, url: str, params: Dict = None, headers: Dict = None) -> Optional[Dict]:
        """
        Make HTTP request with error handling and retry logic
        """
        self._rate_limit_sleep()
        
        default_headers = {
            'User-Agent': 'CryptoPipeline/1.0',
            'Accept': 'application/json'
        }
        
        if headers:
            default_headers.update(headers)
        
        if self.coingecko_api_key and 'coingecko' in url:
            default_headers['x-cg-demo-api-key'] = self.coingecko_api_key
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.get(
                    url,
                    params=params,
                    headers=default_headers,
                    timeout=30
                )
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    # Rate limited, wait and retry
                    wait_time = 2 ** attempt  # Exponential backoff
                    self.logger.warning(f"Rate limited, waiting {wait_time} seconds")
                    time.sleep(wait_time)
                    continue
                else:
                    self.logger.error(f"API request failed: {response.status_code} - {response.text}")
                    return None
                    
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request exception (attempt {attempt + 1}): {str(e)}")
                if attempt == max_retries - 1:
                    return None
                time.sleep(2 ** attempt)
        
        return None
    
    def get_coin_data(self, coin_id: str) -> Optional[Dict[str, Any]]:
        """
        Get comprehensive data for a specific cryptocurrency
        """
        url = f"{self.base_url}/coins/{coin_id}"
        params = {
            'localization': 'false',
            'tickers': 'true',
            'market_data': 'true',
            'community_data': 'false',
            'developer_data': 'false',
            'sparkline': 'true'
        }
        
        data = self._make_request(url, params)
        
        if not data:
            return None
        
        # Extract and normalize relevant data
        try:
            normalized_data = {
                'id': data.get('id'),
                'symbol': data.get('symbol', '').upper(),
                'name': data.get('name'),
                'current_price_usd': data.get('market_data', {}).get('current_price', {}).get('usd'),
                'market_cap_usd': data.get('market_data', {}).get('market_cap', {}).get('usd'),
                'total_volume_usd': data.get('market_data', {}).get('total_volume', {}).get('usd'),
                'price_change_24h': data.get('market_data', {}).get('price_change_24h'),
                'price_change_percentage_24h': data.get('market_data', {}).get('price_change_percentage_24h'),
                'price_change_percentage_7d': data.get('market_data', {}).get('price_change_percentage_7d'),
                'price_change_percentage_30d': data.get('market_data', {}).get('price_change_percentage_30d'),
                'market_cap_rank': data.get('market_cap_rank'),
                'circulating_supply': data.get('market_data', {}).get('circulating_supply'),
                'total_supply': data.get('market_data', {}).get('total_supply'),
                'max_supply': data.get('market_data', {}).get('max_supply'),
                'ath': data.get('market_data', {}).get('ath', {}).get('usd'),
                'ath_date': data.get('market_data', {}).get('ath_date', {}).get('usd'),
                'atl': data.get('market_data', {}).get('atl', {}).get('usd'),
                'atl_date': data.get('market_data', {}).get('atl_date', {}).get('usd'),
                'last_updated': data.get('last_updated'),
                'data_source': 'coingecko',
                'fetch_timestamp': datetime.utcnow().isoformat(),
                'sparkline_7d': data.get('market_data', {}).get('sparkline_7d', {}).get('price', []),
            }
            
            # Add additional calculated metrics
            if normalized_data['current_price_usd'] and normalized_data['ath']:
                normalized_data['distance_from_ath_percent'] = (
                    (normalized_data['ath'] - normalized_data['current_price_usd']) / 
                    normalized_data['ath'] * 100
                )
            
            return normalized_data
            
        except Exception as e:
            self.logger.error(f"Error normalizing data for {coin_id}: {str(e)}")
            return None
    
    def get_market_overview(self) -> Optional[Dict[str, Any]]:
        """
        Get overall cryptocurrency market data
        """
        url = f"{self.base_url}/global"
        data = self._make_request(url)
        
        if not data or 'data' not in data:
            return None
        
        try:
            market_data = data['data']
            normalized_data = {
                'total_market_cap_usd': market_data.get('total_market_cap', {}).get('usd'),
                'total_volume_24h_usd': market_data.get('total_volume', {}).get('usd'),
                'bitcoin_dominance_percentage': market_data.get('market_cap_percentage', {}).get('btc'),
                'ethereum_dominance_percentage': market_data.get('market_cap_percentage', {}).get('eth'),
                'active_cryptocurrencies': market_data.get('active_cryptocurrencies'),
                'markets': market_data.get('markets'),
                'market_cap_change_percentage_24h': market_data.get('market_cap_change_percentage_24h_usd'),
                'data_source': 'coingecko_global',
                'fetch_timestamp': datetime.utcnow().isoformat(),
            }
            
            return normalized_data
            
        except Exception as e:
            self.logger.error(f"Error normalizing global market data: {str(e)}")
            return None
    
    def get_trending_coins(self) -> List[Dict[str, Any]]:
        """
        Get trending cryptocurrencies
        """
        url = f"{self.base_url}/search/trending"
        data = self._make_request(url)
        
        if not data or 'coins' not in data:
            return []
        
        trending_data = []
        
        try:
            for coin in data['coins']:
                coin_info = coin.get('item', {})
                normalized_coin = {
                    'id': coin_info.get('id'),
                    'name': coin_info.get('name'),
                    'symbol': coin_info.get('symbol'),
                    'market_cap_rank': coin_info.get('market_cap_rank'),
                    'trending_score': coin_info.get('score', 0),
                    'data_source': 'coingecko_trending',
                    'fetch_timestamp': datetime.utcnow().isoformat(),
                }
                trending_data.append(normalized_coin)
            
            return trending_data
            
        except Exception as e:
            self.logger.error(f"Error processing trending coins data: {str(e)}")
            return []
    
    def get_multiple_coins_data(self, coin_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Get data for multiple coins efficiently
        """
        # CoinGecko supports bulk requests
        ids_string = ','.join(coin_ids)
        url = f"{self.base_url}/coins/markets"
        params = {
            'vs_currency': 'usd',
            'ids': ids_string,
            'order': 'market_cap_desc',
            'per_page': len(coin_ids),
            'page': 1,
            'sparkline': 'true',
            'price_change_percentage': '1h,24h,7d,30d'
        }
        
        data = self._make_request(url, params)
        
        if not data:
            return []
        
        normalized_data = []
        
        try:
            for coin in data:
                normalized_coin = {
                    'id': coin.get('id'),
                    'symbol': coin.get('symbol', '').upper(),
                    'name': coin.get('name'),
                    'current_price_usd': coin.get('current_price'),
                    'market_cap_usd': coin.get('market_cap'),
                    'market_cap_rank': coin.get('market_cap_rank'),
                    'fully_diluted_valuation': coin.get('fully_diluted_valuation'),
                    'total_volume_usd': coin.get('total_volume'),
                    'high_24h': coin.get('high_24h'),
                    'low_24h': coin.get('low_24h'),
                    'price_change_24h': coin.get('price_change_24h'),
                    'price_change_percentage_24h': coin.get('price_change_percentage_24h'),
                    'price_change_percentage_7d': coin.get('price_change_percentage_7d_in_currency'),
                    'price_change_percentage_30d': coin.get('price_change_percentage_30d_in_currency'),
                    'market_cap_change_24h': coin.get('market_cap_change_24h'),
                    'market_cap_change_percentage_24h': coin.get('market_cap_change_percentage_24h'),
                    'circulating_supply': coin.get('circulating_supply'),
                    'total_supply': coin.get('total_supply'),
                    'max_supply': coin.get('max_supply'),
                    'ath': coin.get('ath'),
                    'ath_change_percentage': coin.get('ath_change_percentage'),
                    'ath_date': coin.get('ath_date'),
                    'atl': coin.get('atl'),
                    'atl_change_percentage': coin.get('atl_change_percentage'),
                    'atl_date': coin.get('atl_date'),
                    'last_updated': coin.get('last_updated'),
                    'sparkline_7d': coin.get('sparkline_in_7d', {}).get('price', []),
                    'data_source': 'coingecko_markets',
                    'fetch_timestamp': datetime.utcnow().isoformat(),
                }
                normalized_data.append(normalized_coin)
            
            return normalized_data
            
        except Exception as e:
            self.logger.error(f"Error processing multiple coins data: {str(e)}")
            return []
    
    def get_fear_greed_index(self) -> Optional[Dict[str, Any]]:
        """
        Get Fear & Greed Index data (alternative API)
        """
        url = "https://api.alternative.me/fng/"
        data = self._make_request(url)
        
        if not data or 'data' not in data:
            return None
        
        try:
            fng_data = data['data'][0]
            normalized_data = {
                'fear_greed_value': int(fng_data.get('value', 0)),
                'fear_greed_classification': fng_data.get('value_classification'),
                'timestamp': fng_data.get('timestamp'),
                'data_source': 'alternative_me',
                'fetch_timestamp': datetime.utcnow().isoformat(),
            }
            
            return normalized_data
            
        except Exception as e:
            self.logger.error(f"Error processing Fear & Greed Index data: {str(e)}")
            return None