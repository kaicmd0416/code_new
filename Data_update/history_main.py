"""
历史数据更新主模块 (Historical Data Update Main Module)

该模块用于执行历史数据的批量更新操作，与日常更新模块不同，
该模块允许指定具体的开始和结束日期范围进行数据更新。
主要功能包括：
1. 历史市场数据更新
2. 历史评分数据更新
3. 历史因子数据更新
4. 历史宏观数据更新

作者: 数据更新团队
创建时间: 2025年
版本: 1.0
"""

import sys
import os
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
from FactorData_update.factor_update import FactorData_update
from MktData_update.MktData_update_main import MktData_update_main,CBData_update_main
from Score_update.score_update_main import score_update_main
from File_moving.File_moving import File_moving
from Time_tools.time_tools import time_tools
from TimeSeries_update.time_series_data_update import timeSeries_data_update
import global_tools as gt
from L4Data_update.L4_running_main import L4_update_main
from vix.vix_calculation import VIX_calculation_main
from MacroData_update.MacroData_upate_main import MacroData_update_main

def MarketData_history_main(start_date, end_date, is_sql):
    """
    历史市场数据更新主函数
    
    在指定的日期范围内更新市场相关的所有历史数据，包括：
    - 基础市场数据
    - VIX波动率指数数据
    - 时间序列数据
    - 宏观时间序列数据
    - 可转债数据
    
    Args:
        start_date (str): 开始日期，格式为'YYYY-MM-DD'
        end_date (str): 结束日期，格式为'YYYY-MM-DD'
        is_sql (bool): 是否执行SQL更新操作
        
    Returns:
        None
        
    Note:
        该函数用于批量更新历史数据，适用于数据补全或重新计算场景
    """
    MktData_update_main(start_date,end_date,is_sql)
    tdu = timeSeries_data_update(start_date, end_date)
    VIX_calculation_main(start_date,end_date, False,is_sql)
    tdu.Mktdata_update_main()
    tdu.macrodata_update_main()
    CBData_update_main(start_date,end_date,is_sql)

def ScoreData_history_main(start_date, end_date, is_sql):
    """
    历史评分数据更新主函数
    
    在指定的日期范围内更新评分相关的历史数据。
    
    Args:
        start_date (str): 开始日期，格式为'YYYY-MM-DD'
        end_date (str): 结束日期，格式为'YYYY-MM-DD'
        is_sql (bool): 是否执行SQL更新操作
        
    Returns:
        None
        
    Note:
        - 评分类型固定为'fm'
        - 适用于历史评分数据的重新计算或补全
    """
    score_type = 'fm'
    score_update_main(score_type, start_date,end_date,is_sql)

def FactorData_history_main(start_date, end_date, is_sql):
    """
    历史因子数据更新主函数
    
    在指定的日期范围内更新因子相关的历史数据，包括：
    - 因子基础数据
    - 因子时间序列数据
    
    Args:
        start_date (str): 开始日期，格式为'YYYY-MM-DD'
        end_date (str): 结束日期，格式为'YYYY-MM-DD'
        is_sql (bool): 是否执行SQL更新操作
        
    Returns:
        None
        
    Note:
        适用于历史因子数据的重新计算或补全
    """
    fu = FactorData_update(start_date,end_date,is_sql)
    tdu = timeSeries_data_update(start_date, end_date)
    fu.FactorData_update_main()
    tdu.Factordata_update_main()

def MacroData_history_main(start_date, end_date, is_sql):
    """
    历史宏观数据更新主函数
    
    在指定的日期范围内更新宏观经济相关的历史数据。
    
    Args:
        start_date (str): 开始日期，格式为'YYYY-MM-DD'
        end_date (str): 结束日期，格式为'YYYY-MM-DD'
        is_sql (bool): 是否执行SQL更新操作
        
    Returns:
        None
        
    Note:
        适用于历史宏观数据的重新计算或补全
    """
    MacroData_update_main(start_date, end_date, is_sql)

if __name__ == '__main__':
    # 示例：设置历史数据更新的日期范围和参数
    start_date = '2025-07-22'
    end_date = '2025-07-22'
    is_sql = True
    
    # 根据需要进行的历史数据更新类型，取消相应注释
    # MarketData_history_main(start_date, end_date, is_sql)
    ScoreData_history_main(start_date, end_date, is_sql)
    # FactorData_history_main(start_date, end_date, is_sql)
    # MacroData_history_main(start_date, end_date, is_sql)

