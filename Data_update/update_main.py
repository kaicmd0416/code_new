"""
数据更新主模块 (Data Update Main Module)

该模块是整个数据更新系统的核心入口，负责协调各个数据更新子模块的执行。
主要功能包括：
1. 市场数据更新 (Market Data Update)
2. 宏观数据更新 (Macro Data Update) 
3. 评分数据更新 (Score Data Update)
4. 因子数据更新 (Factor Data Update)
5. L4数据更新 (L4 Data Update)
6. 自动化日常更新流程

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
from Data_checking.data_check import DataCheck
import global_tools as gt
from L4Data_update.L4_running_main import L4_update_main
from vix.vix_calculation import VIX_calculation_main
from MacroData_update.MacroData_upate_main import MacroData_update_main
from TimeSeries_update.time_series_data_update import timeSeries_data_update

def MarketData_update_main(is_sql=True):
    """
    市场数据更新主函数
    
    负责更新市场相关的所有数据，包括：
    - 基础市场数据
    - VIX波动率指数数据
    - 时间序列数据
    - 可转债数据
    
    Args:
        is_sql (bool): 是否执行SQL更新操作，默认为True
        
    Returns:
        None
        
    Note:
        - 自动计算目标日期和回滚日期
        - 回滚10个工作日用于时间序列数据
        - 回滚3个工作日用于市场数据
    """
    tt = time_tools()
    date = tt.target_date_decision_mkt()
    date = gt.strdate_transfer(date)
    #回滚
    start_date2=date
    for i in range(10):
        start_date2=gt.last_workday_calculate(start_date2)
    # 回滚
    start_date = date
    for i in range(3):
            start_date = gt.last_workday_calculate(start_date)
    tdu=timeSeries_data_update(start_date2,date)
    MktData_update_main(start_date, date,is_sql)
    VIX_calculation_main(start_date, date, False,is_sql)
    tdu.Mktdata_update_main()
    CBData_update_main(start_date, date,is_sql)

def Macrodata_update_main(is_sql=True):
    """
    宏观数据更新主函数
    
    负责更新宏观经济相关的数据，包括：
    - 宏观基础数据
    - 宏观时间序列数据
    
    Args:
        is_sql (bool): 是否执行SQL更新操作，默认为True
        
    Returns:
        None
        
    Note:
        - 自动计算目标日期和回滚日期
        - 回滚10个工作日用于时间序列数据
        - 回滚3个工作日用于宏观数据
    """
    tt = time_tools()
    date = tt.target_date_decision_mkt()
    date = gt.strdate_transfer(date)
    # 回滚
    start_date2 = date
    for i in range(10):
        start_date2 = gt.last_workday_calculate(start_date2)
    # 回滚
    start_date = date
    for i in range(3):
        start_date = gt.last_workday_calculate(start_date)
    tdu = timeSeries_data_update(start_date2, date)
    MacroData_update_main(start_date, date, is_sql)
    tdu.macrodata_update_main()

def ScoreData_update_main(is_sql=True):
    """
    评分数据更新主函数
    
    负责更新评分相关的数据，使用'fm'评分类型。
    
    Args:
        is_sql (bool): 是否执行SQL更新操作，默认为True
        
    Returns:
        None
        
    Note:
        - 使用评分专用的日期决策逻辑
        - 评分类型固定为'fm'
    """
    tt = time_tools()
    date = tt.target_date_decision_score()
    date = gt.strdate_transfer(date)
    score_type = 'fm'
    score_update_main(score_type, date, date,is_sql)

def FactorData_update_main(is_sql=True):
    """
    因子数据更新主函数
    
    负责更新因子相关的数据，包括：
    - 因子基础数据
    - 因子时间序列数据
    
    Args:
        is_sql (bool): 是否执行SQL更新操作，默认为True
        
    Returns:
        None
        
    Note:
        - 使用因子专用的日期决策逻辑
        - 回滚10个工作日用于时间序列数据
        - 回滚3个工作日用于因子数据
    """
    tt = time_tools()
    date = tt.target_date_decision_factor()
    date = gt.strdate_transfer(date)
    # 回滚
    start_date2 = date
    for i in range(10):
        start_date2 = gt.last_workday_calculate(start_date2)
    # 回滚
    start_date = date
    for i in range(3):
            start_date = gt.last_workday_calculate(start_date)
    tdu = timeSeries_data_update(start_date2, date)
    fu = FactorData_update(start_date, date,is_sql)
    fu.FactorData_update_main()
    tdu.Factordata_update_main()

def L4Data_update_main(is_sql=True):
    """
    L4数据更新主函数
    
    负责更新L4相关的数据。
    
    Args:
        is_sql (bool): 是否执行SQL更新操作，默认为True
        
    Returns:
        None
    """
    L4_update_main(is_sql)

def daily_update_auto():
    """
    自动化日常更新主函数
    
    执行完整的日常数据更新流程，按以下顺序进行：
    1. 文件移动操作
    2. 市场数据更新
    3. 评分数据更新
    4. 因子数据更新
    5. 宏观数据更新
    6. L4数据更新
    7. 数据检查
    
    Args:
        None
        
    Returns:
        None
        
    Note:
        这是系统的主要入口函数，用于自动化执行所有数据更新任务
    """
    fm = File_moving()
    fm.file_moving_update_main()
    MarketData_update_main()
    ScoreData_update_main()
    FactorData_update_main()
    Macrodata_update_main()
    L4Data_update_main()
    DC=DataCheck()
    DC.DataCheckmain()

if __name__ == '__main__':
    # fm = File_moving()
    # fm.file_moving_update_main()
    daily_update_auto()