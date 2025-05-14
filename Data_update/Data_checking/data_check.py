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