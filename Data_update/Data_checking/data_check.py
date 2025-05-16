import pandas as pd
import os
import sys
import global_setting.global_dic as glv
from Time_tools.time_tools import time_tools
import sys
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import global_tools as gt
from setup_logger.logger_setup import setup_logger2
import io
import contextlib
import yaml
import pymysql
from datetime import datetime

class DataCheck:
    def __init__(self,target_date=None): #对应你要交易的日期
        self.target_date=target_date
        self.tt = time_tools()
        self.logger = setup_logger2('DataCheck')
        if target_date==None:
            self.target_date_score = self.tt.target_date_decision_score()
            self.target_date_other = self.tt.target_date_decision_mkt()
        else:
            self.target_date_score = target_date
            self.target_date_other = gt.last_workday_calculate(target_date)
        self.db_config = self._load_db_config()
        self.conn = None
        self.cursor = None

    def _load_db_config(self):
        """加载数据库配置信息"""
        try:
            config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 
                                     'config_project', 'Data_update', 'sql_connection.yaml')
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            self.logger.info("数据库配置加载成功")
            return config['database']
        except Exception as e:
            self.logger.error(f"加载数据库配置失败: {str(e)}")
            raise

    def connect_db(self):
        """建立数据库连接"""
        try:
            self.conn = pymysql.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database']
            )
            self.cursor = self.conn.cursor()
            self.logger.info("数据库连接成功")
        except Exception as e:
            self.logger.error(f"数据库连接失败: {str(e)}")
            raise

    def close_db(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        self.logger.info("数据库连接已关闭")

    def check_database_updates(self):
        """
        检查数据库中所有表的更新状态
        基于每个表的主键和valuation_date字段检查数据是否更新到最新日期
        对于多维数据表，检查最新日期的数据完整性
        """
        self.logger.info("开始检查数据库更新状态")
        try:
            self.connect_db()
            
            # 获取所有表名
            self.cursor.execute("SHOW TABLES")
            tables = self.cursor.fetchall()
            
            for table in tables:
                table_name = table[0]
                self.logger.info(f"\n检查表 {table_name} 的更新状态:")
                
                try:
                    # 获取表的主键信息
                    self.cursor.execute(f"""
                        SELECT COLUMN_NAME 
                        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                        WHERE TABLE_SCHEMA = '{self.db_config['database']}' 
                        AND TABLE_NAME = '{table_name}' 
                        AND CONSTRAINT_NAME = 'PRIMARY'
                    """)
                    primary_keys = [pk[0] for pk in self.cursor.fetchall()]
                    
                    if not primary_keys:
                        self.logger.warning(f"表 {table_name} 没有主键，跳过检查")
                        continue
                    
                    # 检查是否有valuation_date字段
                    self.cursor.execute(f"""
                        SELECT COLUMN_NAME 
                        FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_SCHEMA = '{self.db_config['database']}' 
                        AND TABLE_NAME = '{table_name}' 
                        AND COLUMN_NAME = 'valuation_date'
                    """)
                    has_valuation_date = self.cursor.fetchone()
                    
                    if not has_valuation_date:
                        self.logger.warning(f"表 {table_name} 没有valuation_date字段，跳过检查")
                        continue
                    
                    # 获取最新的valuation_date
                    self.cursor.execute(f"""
                        SELECT MAX(valuation_date) 
                        FROM {table_name}
                    """)
                    latest_date = self.cursor.fetchone()[0]
                    
                    if not latest_date:
                        self.logger.warning(f"表 {table_name} 没有数据")
                        continue
                    
                    # 转换为datetime对象进行比较
                    latest_date = pd.to_datetime(latest_date).date()
                    target_date = pd.to_datetime(self.target_date_other).date()
                    
                    if latest_date < target_date:
                        self.logger.error(f"表 {table_name} 数据未更新到最新日期")
                        self.logger.error(f"最新数据日期: {latest_date}, 目标日期: {target_date}")
                        continue
                    
                    # 检查最新日期的数据完整性
                    # 1. 获取所有主键组合的计数
                    pk_columns = ', '.join(primary_keys)
                    self.cursor.execute(f"""
                        SELECT {pk_columns}, COUNT(*) as count
                        FROM {table_name}
                        GROUP BY {pk_columns}
                        ORDER BY count DESC
                    """)
                    pk_counts = self.cursor.fetchall()
                    
                    if not pk_counts:
                        self.logger.warning(f"表 {table_name} 没有主键组合数据")
                        continue
                    
                    # 2. 获取最新日期的数据
                    self.cursor.execute(f"""
                        SELECT {pk_columns}
                        FROM {table_name}
                        WHERE valuation_date = '{latest_date}'
                    """)
                    latest_date_data = self.cursor.fetchall()
                    
                    # 3. 获取所有唯一的主键组合
                    self.cursor.execute(f"""
                        SELECT DISTINCT {pk_columns}
                        FROM {table_name}
                    """)
                    all_unique_pks = self.cursor.fetchall()
                    
                    # 4. 检查最新日期的数据完整性
                    if len(latest_date_data) < len(all_unique_pks):
                        self.logger.error(f"表 {table_name} 在最新日期 {latest_date} 的数据不完整")
                        self.logger.error(f"应有 {len(all_unique_pks)} 条记录，实际有 {len(latest_date_data)} 条记录")
                        
                        # 找出缺失的主键组合
                        missing_pks = set(all_unique_pks) - set(latest_date_data)
                        self.logger.error("缺失的主键组合:")
                        for pk in missing_pks:
                            pk_str = ', '.join(str(x) for x in pk)
                            self.logger.error(f"  - {pk_str}")
                    else:
                        self.logger.info(f"表 {table_name} 数据已更新到最新日期 {latest_date} 且数据完整")
                    
                except Exception as e:
                    self.logger.error(f"检查表 {table_name} 时发生错误: {str(e)}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"数据库检查过程中发生错误: {str(e)}")
        finally:
            self.close_db()
            
        self.logger.info("数据库更新状态检查完成")

    def capture_file_withdraw_output(self, func, *args, **kwargs):
        """捕获file_withdraw的输出并记录到日志"""
        with io.StringIO() as buf, contextlib.redirect_stdout(buf):
            result = func(*args, **kwargs)
            output = buf.getvalue()
            if output.strip():
                self.logger.info(output.strip())
        return result

    def rrScore_updateChecking(self):
        self.logger.info("开始检查 rrScore 更新状态")
        inputpath_score_config = glv.get('score_mode')
        df_config = pd.read_excel(inputpath_score_config)
        df_config['score_type2'] = df_config['score_type'].apply(lambda x: str(x)[:2])
        mode_type_using_list = df_config[(df_config['score_type2'] == 'rr')]['score_mode'].tolist()
        for mode_type in mode_type_using_list:
            outputpath = glv.get('output_score')
            outputpath2 = os.path.join(outputpath, 'rr_' + str(mode_type))
            mode_name = df_config[df_config['score_mode'] == mode_type]['mode_name'].tolist()[0]
            outputpath3 = os.path.join(outputpath, mode_name)
            available_date = gt.last_workday_calculate(self.target_date_score)
            available_date = gt.intdate_transfer(available_date)
            inputpath_file2 = self.capture_file_withdraw_output(gt.file_withdraw, outputpath2, available_date)
            inputpath_file3 = self.capture_file_withdraw_output(gt.file_withdraw, outputpath3, available_date)
            if inputpath_file2 == None or inputpath_file3 == None:
                self.logger.error(f'fm_score在最新时间:{self.target_date_score}更新出现错误')
            else:
                self.logger.info(f'fm_score已经更新到最新日期:{self.target_date_score}')

    def combineScore_updateChecking(self):
        self.logger.info("开始检查 combineScore 更新状态")
        inputpath_score_config = glv.get('mode_dic')
        df_config = pd.read_excel(inputpath_score_config)
        df_config['base_score2'] = df_config['base_score'].apply(lambda x: str(x)[:2])
        mode_type_using_list = df_config[(df_config['base_score2'] == 'co')]['base_score'].tolist()
        for mode_type in mode_type_using_list:
            outputpath = glv.get('output_score')
            outputpath2 = os.path.join(outputpath, mode_type)
            available_date = gt.last_workday_calculate(self.target_date_score)
            available_date = gt.intdate_transfer(available_date)
            inputpath_file2 = self.capture_file_withdraw_output(gt.file_withdraw, outputpath2, available_date)
            if inputpath_file2 == None:
                self.logger.error(f'combine_score在最新时间:{self.target_date_score}更新出现错误')
            else:
                self.logger.info(f'combine_score已经更新到最新日期:{self.target_date_score}')

    def config_withdraw(self):
        inputpath = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        inputpath_config = os.path.join(inputpath, 'config_path\data_update_path_config.xlsx')
        df=pd.read_excel(inputpath_config)
        def output_finding(x):
            if 'output' in x:
                return 'Yes'
            else:
                return 'False'
        df['is_output']=df['data_type'].apply(lambda x: output_finding(x))
        df=df[df['is_output']=='Yes']
        df=df[~(df['data_type'].isin(['output_score','output_timeseries','output_destination','output_prod','output_l4']))]
        output_list=df['data_type'].tolist()
        return output_list

    def checking_crossSectiondata_main(self):
        self.logger.info("开始数据检查主流程")
        outputlist=self.config_withdraw()
        further_list=['output_indexexposure','output_indexcomponent','output_portfolio']
        for data_type in outputlist:
            inputpath=glv.get(data_type)
            if data_type in further_list:
                try:
                    input_list=os.listdir(inputpath)
                except:
                    self.logger.error(f'找不到 {inputpath}')
                    continue
                if data_type=='output_portfolio':
                    available_date=gt.intdate_transfer(self.target_date_score)
                    input_list=[i for i in input_list if 'a1' in i or 'a3' in i or 'gms' in i or 'ubp500' in i]
                else:
                    available_date = gt.intdate_transfer(self.target_date_other)
                for input in input_list:
                    inputpath_daily=os.path.join(inputpath,input)
                    try:
                         inputpath_daily = self.capture_file_withdraw_output(gt.file_withdraw, inputpath_daily, available_date)
                    except:
                        self.logger.error(f'找不到 {inputpath_daily}')
            else:
                try:
                    if 'outputpath' in data_type:
                         available_date = gt.last_workday_calculate(self.target_date_other)
                         available_date=gt.intdate_transfer(available_date)
                    else:
                         available_date = gt.intdate_transfer(self.target_date_other)
                    inputpath_daily = self.capture_file_withdraw_output(gt.file_withdraw, inputpath, available_date)
                except:
                    self.logger.error(f'找不到 {inputpath}')
        self.logger.info("数据检查主流程完成")

    def checking_timeseriesdata_main(self):
        self.logger = setup_logger2('TimeseriesDataCheck')
        outputpath_ori = glv.get('output_timeseries')
        file_list = os.listdir(outputpath_ori)
        # 对每个数据类型进行处理
        for file_name in file_list:
            outputpath = os.path.join(outputpath_ori, file_name)

            # 检查目录是否存在
            if not os.path.exists(outputpath):
                self.logger.info(f"Directory {file_name} not found")
                continue

            # 获取该目录下的所有文件
            files = os.listdir(outputpath)
            if not files:
                self.logger.info(f"No files found in {file_name}")
                continue

            self.logger.info(f"\nChecking {file_name}:")
            # 处理该目录下的每个文件
            for file in files:
                try:
                    df = gt.readcsv(os.path.join(outputpath, file))
                    start_date = df['valuation_date'].min()
                    end_date = df['valuation_date'].max()
                    expected_dates = gt.working_days_list(start_date, self.target_date_other)
                    actual_dates = df['valuation_date'].unique().tolist()
                    missing_dates = list(set(expected_dates) - set(actual_dates))
                    missing_dates.sort()
                    if missing_dates:
                        # 将长列表分成多行输出
                        self.logger.info(f'Missing data for {file}:')
                        # 每行最多显示10个日期
                        for i in range(0, len(missing_dates), 10):
                            chunk = missing_dates[i:i + 10]
                            self.logger.info(f'Dates {i + 1}-{i + len(chunk)}: {chunk}')
                    else:
                        self.logger.info(f'{file} already updated to the latest date {end_date}')
                except Exception as e:
                    self.logger.error(f"Error processing {file} in {file_name}: {str(e)}")



if __name__ == '__main__':
    dc=DataCheck('2025-05-12')
    dc.timeselecting_check()