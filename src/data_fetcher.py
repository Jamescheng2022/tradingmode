"""
数据获取模块 - 使用 AkShare 获取 A股/香港/美股/期货数据
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import akshare as ak
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataFetcher:
    """数据获取器"""
    
    def __init__(self, lookback_days=120):
        self.lookback_days = lookback_days
        self.cache = {}
    
def get_daily_data(self, symbol, start_date=None, end_date=None):
        """
        获取日线数据
        支持: A股ETF (510300), 期货连续 (rb9999), 港股 (HK0001) 等
        """
        if end_date is None:
            end_date = datetime.now().date()
        if start_date is None:
            start_date = end_date - timedelta(days=self.lookback_days)
        
        try:
            # 缓存检查
            cache_key = f"{symbol}_{start_date}_{end_date}"
            if cache_key in self.cache:
                return self.cache[cache_key]
            
            logger.info(f"Fetching data for {symbol} from {start_date} to {end_date}")
            
            # 根据符号类型选择数据源
            if self._is_etf(symbol):
                df = self._fetch_etf_data(symbol, start_date, end_date)
            elif self._is_futures(symbol):
                df = self._fetch_futures_data(symbol, start_date, end_date)
            elif self._is_hk_stock(symbol):
                df = self._fetch_hk_data(symbol, start_date, end_date)
            elif self._is_forex(symbol):
                df = self._fetch_forex_data(symbol, start_date, end_date)
            else:
                df = self._fetch_etf_data(symbol, start_date, end_date)
            
            # 数据清理
            df = self._clean_data(df)
            self.cache[cache_key] = df
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {str(e)}")
            return pd.DataFrame()
    
def _fetch_etf_data(self, code, start_date, end_date):
        """获取 ETF 日线数据"""
        try:
            df = ak.fund_etf_hist(
                symbol=code,
                start_date=str(start_date).replace('-', ''),
                end_date=str(end_date).replace('-', ''),
                period="daily"
            )
            df['date'] = pd.to_datetime(df['日期'])
            df['close'] = pd.to_numeric(df['净值'], errors='coerce')
            df['volume'] = pd.to_numeric(df['成交数量'], errors='coerce')
            return df[['date', 'close', 'volume']].copy()
        except Exception as e:
            logger.warning(f"Failed to fetch ETF {code}: {str(e)}")
            return pd.DataFrame()
    
def _fetch_futures_data(self, code, start_date, end_date):
        """获取期货数据"""
        try:
            df = ak.futures_daily(
                symbol=code,
                start_date=str(start_date).replace('-', ''),
                end_date=str(end_date).replace('-', '')
            )
            df['date'] = pd.to_datetime(df['日期'])
            df['close'] = pd.to_numeric(df['收盘价'], errors='coerce')
            df['volume'] = pd.to_numeric(df['成交量'], errors='coerce')
            return df[['date', 'close', 'volume']].copy()
        except Exception as e:
            logger.warning(f"Failed to fetch futures {code}: {str(e)}")
            return pd.DataFrame()
    
def _fetch_hk_data(self, code, start_date, end_date):
        """获取港股数据"""
        try:
            df = ak.hk_fund_hist(
                symbol=code,
                start_date=str(start_date).replace('-', ''),
                end_date=str(end_date).replace('-', ''),
                period="daily"
            )
            df['date'] = pd.to_datetime(df['日期'])
            df['close'] = pd.to_numeric(df['收盘价'], errors='coerce')
            df['volume'] = pd.to_numeric(df['成交量'], errors='coerce')
            return df[['date', 'close', 'volume']].copy()
        except Exception as e:
            logger.warning(f"Failed to fetch HK data {code}: {str(e)}")
            return pd.DataFrame()
    
def _fetch_forex_data(self, pair, start_date, end_date):
        """获取外汇数据（汇率）"""
        try:
            logger.warning(f"Forex data for {pair} not fully supported, returning empty")
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"Failed to fetch forex {pair}: {str(e)}")
            return pd.DataFrame()
    
    @staticmethod
    def _is_etf(symbol):
        """判断是否为 ETF"""
        return symbol.startswith(('51', '15', '5')) and len(symbol) == 6 and symbol.isdigit()
    
    @staticmethod
    def _is_futures(symbol):
        """判断是否为期货"""
        return any(x in symbol for x in ['9999', 'rb', 'i', 'j', 'l', 'pp', 'a', 'b', 'm', 'c', 'y'])
    
    @staticmethod
    def _is_hk_stock(symbol):
        """判断是否为港股"""
        return symbol.startswith('HK') or symbol.startswith('0') and len(symbol) == 5
    
    @staticmethod
    def _is_forex(symbol):
        """判断是否为外汇"""
        return any(x in symbol for x in ['USDCNH', 'EURUSD', 'GBPUSD'])
    
    @staticmethod
    def _clean_data(df):
        """数据清理"""
        if df.empty:
            return df
        
        df = df.dropna(subset=['close', 'volume'])
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)
        
        # 删除成交量为0的行
        df = df[df['volume'] > 0]
        
        return df