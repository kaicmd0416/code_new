#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
call_matlab_opt.py - 简化版MATLAB调用脚本
"""
import os
import sys
import time
import json
import logging
import traceback
from multiprocessing import Pool, cpu_count
# 检查MATLAB Engine API是否可用
try:
    import matlab.engine
    MATLAB_AVAILABLE = True
except ImportError:
    MATLAB_AVAILABLE = False
    print("警告: 未安装MATLAB Engine API，请使用命令安装: cd [MATLAB安装目录]/extern/engines/python && python setup.py install")

# 脚本目录路径
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def load_config(config_file='call_matlab_opt_config.json'):
    """加载配置文件"""
    config_path = os.path.join(SCRIPT_DIR, config_file)
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"加载配置文件失败: {e}")
        return {}

def setup_logging(config, process_name='main'):
    """设置日志"""
    log_level = config.get('logging', {}).get('level', 'INFO')
    log_file = config.get('logging', {}).get('file', 'matlab_opt.log')
    
    # 获取完整的日志文件路径
    log_path = os.path.join(SCRIPT_DIR, log_file)
    
    level = getattr(logging, log_level.upper(), logging.INFO)
    
    # 创建格式化器
    formatter = logging.Formatter(
        '%(asctime)s.%(msecs)03d - %(process)d - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 创建文件处理器
    file_handler = logging.FileHandler(log_path, encoding='utf-8', mode='a')
    file_handler.setFormatter(formatter)
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # 配置根日志记录器
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # 清除现有的处理器
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # 添加新的处理器
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # 记录初始日志
    root_logger.info(f"日志配置完成 - 进程: {process_name}, 日志文件路径: {log_path}")
    print(f"日志文件保存在: {log_path}")  # 添加控制台输出，方便用户查看

def process_single_path(args):
    """处理单个路径的函数，用于多进程调用"""
    path, path_yes, matlab_config = args
    if not MATLAB_AVAILABLE:
        logging.error("MATLAB Engine API未安装，无法处理")
        return 0, 1
    
    # 为子进程设置日志
    setup_logging(matlab_config, f"worker_{os.getpid()}")
    
    function_name = matlab_config.get('function_name', 'optimizer_matlab_func_v2')
    worker_count = matlab_config.get('worker_count', 4)
    scripts_dir = os.path.join(SCRIPT_DIR, matlab_config.get('scripts_dir', ''))
    
    try:
        # 启动MATLAB引擎
        logging.info(f"进程 {os.getpid()} 开始处理路径: {path}")
        eng = matlab.engine.start_matlab()
        
        # 添加脚本目录到MATLAB路径
        if os.path.exists(scripts_dir):
            eng.addpath(scripts_dir)
            logging.info(f"进程 {os.getpid()} 添加MATLAB脚本目录: {scripts_dir}")
        else:
            logging.error(f"MATLAB脚本目录不存在: {scripts_dir}")
            eng.quit()
            return 0, 1

        start_time = time.time()
        
        try:
            # 调用MATLAB函数
            result = getattr(eng, function_name)(path, path_yes, float(worker_count), nargout=3)
            
            # 解析结果
            if result and len(result) >= 3:
                final_weight = result[0]
                barra_info = result[1]
                industry_info = result[2]
            
            success = 1
            logging.info(f"进程 {os.getpid()} 路径处理成功: {path}")
            
        except Exception as e:
            logging.error(f"进程 {os.getpid()} 处理路径时出错: {path}, 错误: {e}")
            logging.error(traceback.format_exc())
            success = 0
        
        elapsed = time.time() - start_time
        logging.info(f"进程 {os.getpid()} 路径处理完成，用时: {elapsed:.2f}秒")
        
        # 关闭MATLAB引擎
        eng.quit()
        logging.info(f"进程 {os.getpid()} MATLAB引擎已关闭")
        
        return success, 1
        
    except Exception as e:
        logging.error(f"进程 {os.getpid()} MATLAB处理过程出错: {e}")
        logging.error(traceback.format_exc())
        return 0, 1

def run_parallel_optimization(all_paths, all_paths_yes, config):
    """并行运行MATLAB优化"""
    # 获取配置
    matlab_config = config.get('matlab', {})
    engine_count = min(matlab_config.get('engine_count', 1), cpu_count())
    
    if not all_paths:
        logging.error("没有找到任何需要处理的路径")
        return 0.0, 0.0
    
    # 记录开始时间
    start_time = time.time()
    
    # 检查路径是否重复
    unique_paths = set(all_paths)
    if len(unique_paths) != len(all_paths):
        logging.warning(f"发现重复路径！总路径数: {len(all_paths)}, 唯一路径数: {len(unique_paths)}")
        # 打印重复的路径
        path_counts = {}
        for path in all_paths:
            path_counts[path] = path_counts.get(path, 0) + 1
        for path, count in path_counts.items():
            if count > 1:
                logging.warning(f"路径 {path} 重复了 {count} 次")
    
    # 准备参数列表
    args_list = [(path, path_yes, matlab_config) for path, path_yes in zip(all_paths, all_paths_yes)]
    logging.info(f"准备处理 {len(args_list)} 个路径，使用 {engine_count} 个进程")
    
    # 使用进程池并行处理
    total_success = 0
    total_processed = 0
    processed_paths = set()  # 用于跟踪已处理的路径
    
    with Pool(processes=engine_count) as pool:
        results = pool.map(process_single_path, args_list)
        
        for (success, processed), (path, _, _) in zip(results, args_list):
            if path in processed_paths:
                logging.error(f"警告：路径 {path} 被重复处理！")
            processed_paths.add(path)
            total_success += success
            total_processed += processed
    
    # 验证所有路径是否都被处理
    if len(processed_paths) != len(all_paths):
        logging.error(f"路径处理不完整！应处理 {len(all_paths)} 个路径，实际处理 {len(processed_paths)} 个路径")
        missing_paths = set(all_paths) - processed_paths
        logging.error(f"未处理的路径: {missing_paths}")
    
    # 计算总耗时和成功率
    total_elapsed = time.time() - start_time
    success_rate = total_success / total_processed if total_processed > 0 else 0.0
    
    logging.info(f"所有处理完成，总用时: {total_elapsed:.2f}秒")
    logging.info(f"总成功率: {success_rate:.2%} ({total_success}/{total_processed})")
    
    return success_rate, total_elapsed

def call_matlab_running_main(all_path, all_path_yes):
    """主函数"""
    # 加载配置
    config = load_config()
    if not config:
        print("无法加载配置，使用默认配置")
        config = {
            "matlab": {
                "function_name": "optimizer_matlab_func_v2",
                "scripts_dir": "Optimizer_python/matlab",
                "worker_count": 4,
                "engine_count": 1
            },
            "logging": {
                "level": "INFO",
                "file": "matlab_opt.log"
            }
        }
    
    # 设置主进程日志
    setup_logging(config, "main")
    
    # 运行优化
    success_rate, total_elapsed = run_parallel_optimization(all_path, all_path_yes, config)
    
    # 输出结果
    if success_rate > 0:
        logging.info(f"MATLAB优化完成，成功率: {success_rate:.2%}，总用时: {total_elapsed:.2f}秒")
        return True
    else:
        logging.error("MATLAB优化失败")
        return False

# if __name__ == "__main__":
#     try:
#         result = main()
#         sys.exit(0 if result else 1)
#     except Exception as e:
#         print(f"程序执行出错: {e}")
#         traceback.print_exc()
#         sys.exit(1)
