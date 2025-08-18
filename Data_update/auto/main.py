"""
自动化定时更新模块 (Automated Scheduled Update Module)

该模块提供不同时间点的自动化数据更新功能，用于定时执行数据更新任务。
主要功能包括：
1. 7:05时间点的数据更新
2. 15:15时间点的数据更新  
3. 18:00时间点的数据更新

作者: 数据更新团队
创建时间: 2025年
版本: 1.0
"""

from Data_update.update_main import daily_update_705,daily_update_1515,daily_update_1800

def time_705():
    """
    7:05时间点数据更新函数
    
    在每天7:05执行数据更新任务，通常用于早盘前的数据准备。
    
    Args:
        None
        
    Returns:
        None
        
    Note:
        该时间点通常用于更新前一日收盘后的数据
    """
    daily_update_705()

def time_1515():
    """
    15:15时间点数据更新函数
    
    在每天15:15执行数据更新任务，通常用于午盘后的数据更新。
    
    Args:
        None
        
    Returns:
        None
        
    Note:
        该时间点通常用于更新当日午盘的数据
    """
    daily_update_1515()

def time_1800():
    """
    18:00时间点数据更新函数
    
    在每天18:00执行数据更新任务，通常用于收盘后的完整数据更新。
    
    Args:
        None
        
    Returns:
        None
        
    Note:
        该时间点通常用于更新当日完整的收盘数据
    """
    daily_update_1800()

# time_1515()