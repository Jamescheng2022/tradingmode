"""
技术指标计算模块
使用 pandas_ta 库计算 EMA, RSI, ADX, ATR 等指标
"""

import pandas as pd
import numpy as np
import pandas_ta as ta
import logging

logger = logging.getLogger(__name__)

class IndicatorCalculator:
    """技术指标计算器"""
    
    @staticmethod
    def calculate_ema(df, period=20, column='close'):
        """计算指数移动平均线 (EMA)"""
        if len(df) < period:
            return pd.Series([np.nan] * len(df), index=df.index)
        
        return df[column].ewm(span=period, adjust=False).mean()
    
    @staticmethod
    def calculate_rsi(df, period=14, column='close'):
        """计算相对强弱指数 (RSI)"""
        if len(df) < period:
            return pd.Series([np.nan] * len(df), index=df.index)
        
        return ta.rsi(df[column], length=period)
    
    @staticmethod
    def calculate_adx(df, period=14):
        """计算平均趋向指数 (ADX)"""
        if len(df) < period * 2:
            return pd.Series([np.nan] * len(df), index=df.index)
        
        try:
            adx = ta.adx(df['high'] if 'high' in df.columns else df['close'],
                         df['low'] if 'low' in df.columns else df['close'],
                         df['close'], length=period)
            return adx.iloc[:, 0]  # 返回 ADX 列
        except:
            # 如果没有 high/low，用 close 计算简化版
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            return rsi
    
    @staticmethod
    def calculate_atr(df, period=14):
        """计算平均真实波幅 (ATR)"""
        if len(df) < period:
            return pd.Series([np.nan] * len(df), index=df.index)
        
        try:
            return ta.atr(df['high'] if 'high' in df.columns else df['close'],
                         df['low'] if 'low' in df.columns else df['close'],
                         df['close'], length=period)
        except:
            # 简化计算：使用收盘价的变化
            tr = abs(df['close'].diff())
            return tr.rolling(period).mean()
    
    @staticmethod
    def calculate_volume_avg(df, period=20):
        """计算平均成交量"""
        return df['volume'].rolling(window=period).mean()
    
    @staticmethod
    def calculate_returns(df, periods=[20, 60]):
        """计算多周期收益率"""
        results = {}
        for p in periods:
            results[f'return_{p}d'] = df['close'].pct_change(periods=p)
        return pd.DataFrame(results, index=df.index)
    
    @staticmethod
    def detect_golden_cross(ema_short, ema_long):
        """
        检测黄金交叉
        返回: True 如果短期EMA刚刚穿过长期EMA（向上）
        """
        if len(ema_short) < 2 or len(ema_long) < 2:
            return False
        
        # 前一天: 短期 < 长期
        # 今天: 短期 > 长期
        prev_cross = ema_short.iloc[-2] < ema_long.iloc[-2]
        curr_cross = ema_short.iloc[-1] > ema_long.iloc[-1]
        
        return prev_cross and curr_cross
    
    @staticmethod
    def detect_death_cross(ema_short, ema_long):
        """
        检测死亡交叉
        返回: True 如果短期EMA刚刚穿过长期EMA（向下）
        """
        if len(ema_short) < 2 or len(ema_long) < 2:
            return False
        
        prev_cross = ema_short.iloc[-2] > ema_long.iloc[-2]
        curr_cross = ema_short.iloc[-1] < ema_long.iloc[-1]
        
        return prev_cross and curr_cross
    
    @staticmethod
    def detect_trend_breakdown(close_price, ema_short, days=2):
        """
        检测趋势破坏
        返回: True 如果连续 days 天收盘价 < 短期EMA
        """
        if len(close_price) < days:
            return False
        
        recent = close_price.iloc[-days:]
        ema_recent = ema_short.iloc[-days:]
        
        return (recent < ema_recent).all()