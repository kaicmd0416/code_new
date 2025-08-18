"""
全局配置字典模块 (Global Configuration Dictionary Module)

该模块负责管理整个数据更新系统的全局配置路径，通过读取Excel配置文件
来动态构建各种数据类型的路径映射字典。
主要功能包括：
1. 配置文件路径查找
2. 路径配置处理
3. 全局路径字典初始化
4. 路径获取接口

作者: 数据更新团队
创建时间: 2025年
版本: 1.0
"""

import pandas as pd
import os
from pathlib import Path

def get_top_dir_path(current_path, levels_up=2):
    """
    从当前路径向上退指定层数，获取顶层目录的完整路径。
    
    Args:
        current_path (Path): 当前路径（Path对象）
        levels_up (int): 向上退的层数，默认为2
        
    Returns:
        Path: 顶层目录的完整路径（Path对象）
        
    Note:
        用于定位项目的根目录位置
    """
    for _ in range(levels_up):
        current_path = current_path.parent
    return current_path

def config_path_finding():
    """
    查找配置文件路径
    
    通过向上遍历目录结构，查找包含'config'文件夹的路径。
    
    Args:
        None
        
    Returns:
        str: 配置文件所在目录的路径
        
    Note:
        - 最多向上遍历10层目录
        - 查找名为'config'的文件夹
        - 返回config文件夹的父目录路径
    """
    inputpath = os.path.split(os.path.realpath(__file__))[0]
    inputpath_output=None
    should_break=False
    for i in range(10):
        if should_break:
            break
        inputpath = os.path.dirname(inputpath)
        input_list = os.listdir(inputpath)
        for input in input_list:
            if should_break:
                break
            if str(input)=='config':
                inputpath_output=os.path.join(inputpath,input)
                inputpath_output=os.path.dirname(inputpath_output)
                should_break=True
    return inputpath_output

def config_path_processing():
    """
    处理配置文件路径
    
    读取Excel配置文件，构建各种数据类型的路径映射。
    
    Args:
        None
        
    Returns:
        pandas.DataFrame: 包含数据类型和对应路径的DataFrame
        
    Note:
        - 读取'sub_folder'和'main_folder'两个工作表
        - 根据MPON、RON、SON等标志构建完整路径
        - 支持相对路径和绝对路径的配置
        - 包含配置文件验证逻辑
    """
    # 获取当前文件的磁盘
    current_drive = os.path.splitdrive(os.path.dirname(__file__))[0]
    # 获取当前文件的绝对路径
    current_file_path = Path(__file__).resolve()
    # 获取顶层目录的名称
    top_dir_name = get_top_dir_path(current_file_path, levels_up=2)
    # 获取输入路径
    inputpath = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    inputpath_config = os.path.join(inputpath, 'config_path\data_update_path_config.xlsx')
    #E:\Optimizer\Data_update\data_update_path_config.xlsx

    # 读取Excel文件
    # df_sub = pd.read_excel(inputpath_config, sheet_name='sub_folder')
    # df_main = pd.read_excel(inputpath_config, sheet_name='main_folder')
    try:
        df_sub = pd.read_excel(inputpath_config, sheet_name='sub_folder')
        df_main = pd.read_excel(inputpath_config, sheet_name='main_folder')
    except FileNotFoundError:
        print(f"配置文件未找到，请检查路径: {inputpath_config}")
        return
    inputpath_config_sbjzq=config_path_finding()
    # 合并DataFrame
    df_sub = df_sub.merge(df_main, on='folder_type', how='left')
    # 检查是否有行的MPON和RON都为1
    if ((df_sub['MPON'] == 1) & (df_sub['RON'] == 1)).any():
        print(f"{inputpath_config}配置文件有问题：存在MPON和RON都为1的行")
        return
    # 构建完整路径
    df_sub['path'] = df_sub['path'] + os.sep + df_sub['folder_name']
    # 筛选出SON为1的行，并添加最上层的项目名
    df_sub.loc[df_sub['MPON'] == 1, 'path'] = df_sub.loc[df_sub['MPON'] == 1, 'path'].apply(
        lambda x: os.path.join(top_dir_name, x))
    # 筛选出RON为1的行，并添加磁盘名
    df_sub.loc[df_sub['RON'] == 1, 'path'] = df_sub.loc[df_sub['RON'] == 1, 'path'].apply(
        lambda x: os.path.join(current_drive,os.sep, x))
    df_sub.loc[df_sub['RON'] == 'config', ['path']] = df_sub.loc[df_sub['RON'] == 'config']['path'].apply(
        lambda x: os.path.join(inputpath_config_sbjzq, x)).tolist()
    # 选择需要的列
    df_sub = df_sub[['data_type', 'path']]
    return df_sub

def _init():
    """
    初始化全局路径字典
    
    读取配置文件并构建全局路径字典，供其他模块使用。
    
    Args:
        None
        
    Returns:
        dict: 数据类型到路径的映射字典
        
    Note:
        - 将DataFrame转换为字典格式
        - 设置全局变量inputpath_dic
        - 在模块导入时自动执行
    """
    df=config_path_processing()
    df.set_index('data_type',inplace=True,drop=True)
    global inputpath_dic
    inputpath_dic=df.to_dict()
    inputpath_dic=inputpath_dic.get('path')
    return inputpath_dic

def get(name):
    """
    获取指定数据类型的路径
    
    Args:
        name (str): 数据类型名称
        
    Returns:
        str: 对应的路径，如果未找到则返回'not found'
        
    Note:
        提供统一的路径获取接口
    """
    try:
        return inputpath_dic[name]
    except:
        return 'not found'

# 模块导入时自动初始化
_init()
# print(dic)
# config_path_processing()
