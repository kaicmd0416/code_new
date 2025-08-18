"""
时间工具模块 (Time Tools Module)

该模块提供时间相关的工具类，用于处理数据更新系统中的时间决策逻辑。
主要功能包括：
1. 时间区间判断
2. 评分数据目标日期决策
3. 市场数据目标日期决策
4. 因子数据目标日期决策

作者: 数据更新团队
创建时间: 2025年
版本: 1.0
"""

import datetime
from datetime import date
import os
import sys
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt
import global_setting.global_dic as glv
import pandas as pd
import os

class time_tools:
    """
    时间工具类
    
    提供各种时间相关的决策和判断功能，支持不同数据类型的时间逻辑。
    """
    
    def time_zoom_decision(self):
        """
        时间区间决策函数
        
        根据当前时间判断当前处于哪个时间区间，并返回对应的区间名称。
        
        Args:
            None
            
        Returns:
            str: 当前激活的时间区间名称
            
        Note:
            - 读取配置文件中的时间区间设置
            - 比较当前时间与各区间的时间范围
            - 返回第一个匹配的激活区间
        """
        inputpath = glv.get('time_tools_config')
        df_config = pd.read_excel(inputpath, sheet_name='time_zoon')
        df_config['start_time'] = df_config['start_time'].apply(lambda x: x.strftime("%H:%M"))
        df_config['end_time'] = df_config['end_time'].apply(lambda x: x.strftime("%H:%M"))
        time_now = datetime.datetime.now().strftime("%H:%M")
        df_config['now'] = time_now
        df_config['status'] = 'Not_activate'
        df_config.loc[(df_config['now'] >= df_config['start_time']) & (df_config['now'] <= df_config['end_time']), [
            'status']] = 'activate'
        zoom_list = df_config[df_config['status'] == 'activate']['zoom_name'].tolist()
        return zoom_list[0]

    def target_date_decision_score(self):
        """
        评分数据目标日期决策函数
        
        根据当前时间和关键时间点，决定评分数据更新的目标日期。
        
        Args:
            None
            
        Returns:
            date: 评分数据更新的目标日期
            
        Note:
            - 使用'time_1'时间区间的关键时间点
            - 在工作日且超过关键时间时，使用下一个工作日
            - 在非工作日时，使用下一个工作日
            - 在工作日但未超过关键时间时，使用当前日期
        """
        inputpath = glv.get('time_tools_config')
        df_config = pd.read_excel(inputpath, sheet_name='critical_time')
        critical_time = df_config[df_config['zoom_name'] == 'time_1']['critical_time'].tolist()[0]
        critical_time = critical_time.strftime("%H:%M")
        if gt.is_workday_auto() == True:
            today = date.today()
            next_day = gt.next_workday_calculate(today)
            time_now = datetime.datetime.now().strftime("%H:%M")
            if time_now >= critical_time:
                return next_day
            else:
                today = gt.strdate_transfer(today)
                return today
        else:
            today = date.today()
            next_day = gt.next_workday_calculate(today)
            return next_day

    def target_date_decision_mkt(self):
        """
        市场数据目标日期决策函数
        
        根据当前时间和关键时间点，决定市场数据更新的目标日期。
        
        Args:
            None
            
        Returns:
            date: 市场数据更新的目标日期
            
        Note:
            - 使用'time_2'时间区间的关键时间点
            - 在工作日且超过关键时间时，使用当前日期
            - 在非工作日时，使用上一个工作日
            - 在工作日但未超过关键时间时，使用上一个工作日
        """
        inputpath = glv.get('time_tools_config')
        df_config = pd.read_excel(inputpath, sheet_name='critical_time')
        critical_time = df_config[df_config['zoom_name'] == 'time_2']['critical_time'].tolist()[0]
        critical_time = critical_time.strftime("%H:%M")
        if gt.is_workday_auto() == True:
            today = date.today()
            last_day = gt.last_workday_calculate(today)
            time_now = datetime.datetime.now().strftime("%H:%M")
            if time_now >= critical_time:
                return today
            else:
                return last_day
        else:
            today = date.today()
            last_day = gt.last_workday_calculate(today)
            return last_day

    def target_date_decision_factor(self):
        """
        因子数据目标日期决策函数
        
        根据当前时间和关键时间点，决定因子数据更新的目标日期。
        
        Args:
            None
            
        Returns:
            date: 因子数据更新的目标日期
            
        Note:
            - 使用'time_3'时间区间的关键时间点
            - 在工作日且超过关键时间时，使用当前日期
            - 在非工作日时，使用上一个工作日
            - 在工作日但未超过关键时间时，使用上一个工作日
        """
        inputpath = glv.get('time_tools_config')
        df_config = pd.read_excel(inputpath, sheet_name='critical_time')
        critical_time = df_config[df_config['zoom_name'] == 'time_3']['critical_time'].tolist()[0]
        critical_time = critical_time.strftime("%H:%M")
        if gt.is_workday_auto() == True:
            today = date.today()
            last_day = gt.last_workday_calculate(today)
            time_now = datetime.datetime.now().strftime("%H:%M")
            if time_now >= critical_time:
                return today
            else:
                return last_day
        else:
            today = date.today()
            last_day = gt.last_workday_calculate(today)
            return last_day




