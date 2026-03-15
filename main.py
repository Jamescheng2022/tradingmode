"""
主入口脚本
每日 14:30 自动运行，生成交易信号并推送
"""

import sys
import logging
import os
from datetime import datetime

# 创建日志目录
os.makedirs('logs', exist_ok=True)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/quant_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def main():
    """主函数"""
    
    try:
        logger.info("=" * 70)
        logger.info("ETF 轮动量化交易系统启动")
        logger.info(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 70)
        
        # 导入系统
        from src.system import TrendRotationSystem
        
        # 初始化系统
        system = TrendRotationSystem(config_path='config.yaml')
        
        # 生成每日信号
        result = system.generate_daily_signals()
        
        # 可选: 执行信号 (模拟或真实)
        # system.execute_signals(result['signals'], total_equity=100000)
        
        logger.info("\n" + "=" * 70)
        logger.info("执行完成!")
        logger.info("=" * 70)
        
        return 0
    
    except Exception as e:
        logger.error(f"执行出错: {str(e)}", exc_info=True)
        return 1

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)