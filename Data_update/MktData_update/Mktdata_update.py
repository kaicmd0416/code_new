import os
import pandas as pd
import global_setting.global_dic as glv
import sys
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt
from datetime import datetime
from tools_func.tools_func import *
from MktData_update.Mktdata_preparing import (indexdata_prepare, stockData_preparing,
                                              indexComponent_prepare,futureData_preparing,
                                              optionData_preparing,etfData_preparing,CBData_preparing,LHB_amt_prepare
                                              ,NLB_amt_prepare,FutureDifference_prepare)
import re
from setup_logger.logger_setup import setup_logger
import io
import contextlib
def capture_file_withdraw_output(func, *args, **kwargs):
    """捕获file_withdraw的输出并记录到日志"""
    logger = setup_logger('Mktdata_update_sql')
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        result = func(*args, **kwargs)
        output = buf.getvalue()
        if output.strip():
            logger.info(output.strip())
    return result
class indexData_update:
    def __init__(self,start_date,end_date,is_sql):
        self.is_sql=is_sql
        self.start_date = start_date
        self.end_date = end_date
        self.logger = setup_logger('Mktdata_update')
        self.logger.info('\n' + '*'*50 + '\nMARKET DATA UPDATE PROCESSING\n' + '*'*50)
    def source_priority_withdraw(self):
        inputpath_config = glv.get('data_source_priority')
        df_config = pd.read_excel(inputpath_config, sheet_name='index_data')
        return df_config
    def standardize_column_names(self,df):
        # 创建列名映射字典
        column_mapping = {
            # 代码相关
            'code': 'code',
            'ts_code': 'code',
            'qtid': 'code',
            'CODE': 'code',
            # 收盘价相关
            'close': 'close',
            'closeprice': 'close',
            'close_price': 'close',
            'CLOSE': 'close',
            'ClosePrice': 'close',
            # 前收盘价相关
            'pre_close': 'pre_close',
            'prevClose': 'pre_close',
            'prev_close': 'pre_close',
            'prevcloseprice': 'pre_close',
            'PRE_CLOSE': 'pre_close',
            'PrevClosePrice': 'pre_close',
            #开盘价相关
            'OPEN' : 'open',
            'open' : 'open',
            #最高价相关
            'HIGH' : 'high',
            'high' : 'high',
            'hi' : 'high',
            #最低价相关
            'LOW' : 'low',
            'low' : 'low',
            'lo' : 'low',
            #成交量相关：
            'vol' : 'volume',
            'VOLUME' : 'volume',
            #成交金额相关：
            'value' : 'amt',
            'AMT' : 'amt',
            'return' : 'pct_chg',
            'PCT_CHG' : 'pct_chg',
            'ret' : 'return',
            'turn' :'turn_over',
        }
        # 处理列名：先转小写
        df.columns = df.columns.str.lower()
        # 处理列名：替换空格为下划线
        df.columns = df.columns.str.replace(' ', '_')
        # 处理列名：移除特殊字符
        df.columns = df.columns.str.replace('[^\w\s]', '')
        # 创建小写的映射字典
        lower_mapping = {k.lower(): v for k, v in column_mapping.items()}
        # 应用标准化映射
        renamed_columns = {col: lower_mapping.get(col, col) for col in df.columns}
        df = df.rename(columns=renamed_columns)
        # 获取所有标准化后的列名
        standardized_columns = set(column_mapping.values())
        # 只保留在映射字典中定义的列
        columns_to_keep = [col for col in df.columns if col in standardized_columns]
        df = df[columns_to_keep]
        # 定义固定的列顺序
        fixed_columns = ['code','open','high','low','close','pre_close','pct_chg','volume','amt','turn_over']
        # 只选择实际存在的列，并按固定顺序排列
        existing_columns = [col for col in fixed_columns if col in df.columns]
        df = df[existing_columns]
        return df
    def index_code_rename(self,df):
        # Create a mapping dictionary for the specific codes
        code_mapping = {
            '932000': '932000.CSI',
            '000510': '000510.CSI',
            '999004': '999004.SSI'
        }
        # Function to check and replace codes
        def replace_code(code):
            for old_code, new_code in code_mapping.items():
                if old_code in code:
                    return new_code
            return code
        # Apply the replacement function to the code column
        df['code'] = df['code'].apply(replace_code)
        return df
    def index_data_update_main(self):
        self.logger.info('\nProcessing index data update...')
        df_config = self.source_priority_withdraw()
        outputpath_index_return_base = glv.get('output_indexdata')
        gt.folder_creator2(outputpath_index_return_base)
        input_list=os.listdir(outputpath_index_return_base)
        if len(input_list)==0:
            if self.start_date > '2023-06-01':
                start_date = '2023-06-01'
            else:
                start_date = self.start_date
        else:
            start_date=self.start_date
        working_days_list=gt.working_days_list(start_date,self.end_date)
        if self.is_sql == True:
            inputpath_configsql = glv.get('config_sql')
            sm=gt.sqlSaving_main(inputpath_configsql,'indexData',delete=True)
        for available_date in working_days_list:
            self.logger.info(f'Processing date: {available_date}')
            available_date=gt.intdate_transfer(available_date)
            ir=indexdata_prepare(available_date)
            outputpath_index_return = os.path.join(outputpath_index_return_base,
                                                   'indexdata_' + available_date + '.csv')
            df_config.sort_values(by='rank', inplace=True)
            source_name_list = df_config['source_name'].tolist()
            df_indexdata = pd.DataFrame()
            df_index_wind = ir.raw_wind_index_data_withdraw()
            df_index_tushare = ir.raw_tushare_index_data_withdraw()
            df_index_jy = ir.raw_jy_index_data_withdraw()
            try:
                df_index_wind = self.standardize_column_names(df_index_wind)
                df_index_wind=self.index_code_rename(df_index_wind)
            except:
                df_index_wind = pd.DataFrame()
            try:
                df_index_tushare = self.standardize_column_names(df_index_tushare)
                df_index_tushare=self.index_code_rename(df_index_tushare)
            except:
                df_index_tushare = pd.DataFrame()
            try:
                df_index_jy = self.standardize_column_names(df_index_jy)
                df_index_jy=self.index_code_rename(df_index_jy)
                df_index_jy.drop_duplicates('code',keep='first',inplace=True)
            except:
                df_index_jy = pd.DataFrame()
            if len(df_index_wind)>0 and len(df_index_tushare)==0 and len(df_index_jy)==0:
                df_indexdata=df_index_wind
                self.logger.info('indexdata使用的数据源是: wind')
            elif len(df_index_wind)==0 and len(df_index_tushare)>0 and len(df_index_jy)==0:
                df_indexdata=df_index_tushare
                df_indexdata['turn_over']=None
                self.logger.info('indexdata使用的数据源是: tushare')
            elif len(df_index_wind)==0 and len(df_index_tushare)==0 and len(df_index_jy)>0:
                df_indexdata=df_index_jy
                df_indexdata=df_indexdata[df_indexdata['code'].isin(['000016.SH','000076.SH','000300.SH','000510.CSI','000852.SH','000905.SH','399303.SZ','932000.CSI','999004.SSI'])]
                df_indexdata['turn_over']=None
                self.logger.info('indexdata使用的数据源是: jy')
            elif len(df_index_wind)==0 and len(df_index_tushare)==0 and len(df_index_jy)==0:
                df_indexdata = pd.DataFrame()
                self.logger.warning(f'No data available for date: {available_date}')
                continue
            else:
                 # 获取所有唯一的code
                if len(df_index_wind)!=0:
                    if 'code' not in df_index_tushare.columns:
                        all_codes= list(set(df_index_wind['code']))
                    else:
                        all_codes = list(set(df_index_wind['code']) | set(df_index_tushare['code']))
                    all_codes.sort()
                # 保持Wind的列顺序，并添加Tushare独有的列
                    wind_columns = df_index_wind.columns.tolist()
                    tushare_only_columns = [col for col in df_index_tushare.columns if col not in wind_columns]
                    all_columns = wind_columns + tushare_only_columns
                else:
                    all_codes=list(set(df_index_tushare['code']))
                    all_columns=df_index_tushare.columns.tolist()
                # 创建新的DataFrame，包含所有列
                df_indexdata = pd.DataFrame(columns=all_columns)
                # 设置code列
                df_indexdata['code'] = all_codes
                if source_name_list[0]=='wind':
                    df_priority=df_index_wind
                    df_bu=df_index_tushare
                    df_bu2=df_index_jy
                else:
                    df_priority=df_index_tushare
                    df_bu=df_index_wind
                    df_bu2=df_index_jy
                # 对于每一列（除了code列），优先使用Wind的数据，如果Wind的数据有缺失则使用Tushare的数据补充
                for col in all_columns:
                    if col == 'code':
                        continue
                    # 先使用Wind的数据
                    if col in df_priority.columns:
                        # 找出df_stock中在df_priority中存在的代码
                        valid_codes = df_indexdata['code'].isin(df_priority['code'])
                        # 只对存在的代码进行赋值
                        if valid_codes.any():
                            df_indexdata.loc[valid_codes, col] = df_priority.set_index('code').loc[df_indexdata.loc[valid_codes, 'code'], col].values
                        # 对不存在的代码设置为None
                        df_indexdata.loc[~valid_codes, col] = None
                    else:
                        df_indexdata[col] = None
                    
                    # 如果Wind的数据有缺失，用Tushare的数据补充
                    if col in df_bu.columns:
                        # 找出当前列中为NaN的行
                        mask = df_indexdata[col].isna()
                        if mask.any():
                            # 只更新那些为NaN且code在df_bu中存在的行
                            codes_to_update = df_indexdata.loc[mask, 'code']
                            codes_in_bu = df_bu['code'].unique()
                            valid_mask = codes_to_update.isin(codes_in_bu)
                            if valid_mask.any():
                                idx_to_update = df_indexdata.loc[mask].index[valid_mask]
                                codes_valid = codes_to_update[valid_mask]
                                df_indexdata.loc[idx_to_update, col] = df_bu.set_index('code').loc[codes_valid, col].values
                    # 如果Wind的数据有缺失，用jy的数据补充
                    if col in df_bu2.columns:
                        # 找出当前列中为NaN的行
                        mask = df_indexdata[col].isna()
                        if mask.any():
                            # 只更新那些为NaN且code在df_bu2中存在的行
                            codes_to_update = df_indexdata.loc[mask, 'code']
                            codes_in_bu2 = df_bu2['code'].unique()  # 修正：使用df_bu2而不是df_bu
                            valid_mask = codes_to_update.isin(codes_in_bu2)
                            if valid_mask.any():
                                idx_to_update = df_indexdata.loc[mask].index[valid_mask]
                                codes_valid = codes_to_update[valid_mask]
                                df_indexdata.loc[idx_to_update, col] = df_bu2.set_index('code').loc[codes_valid, col].values
                self.logger.info('index_data使用的数据源是: wind和tushare和jy合并数据')
            if len(df_indexdata) != 0:
                df_indexdata['pct_chg']=df_indexdata['pct_chg']/100
                df_indexdata['valuation_date']=gt.strdate_transfer(available_date)
                df_indexdata=df_indexdata[['valuation_date']+df_indexdata.columns.tolist()[:-1]]
                df_indexdata.to_csv(outputpath_index_return, index=False, encoding='gbk')
                self.logger.info(f'Successfully saved index data for date: {available_date}')
                if self.is_sql==True:
                    now = datetime.now()
                    df_indexdata['update_time'] = now
                    capture_file_withdraw_output(sm.df_to_sql, df_indexdata)
            else:
                self.logger.warning(f'index_data {available_date} 四个数据源都没有数据')

class indexComponent_update:
    def __init__(self,start_date,end_date,is_sql):
        self.is_sql=is_sql
        self.start_date=start_date
        self.end_date=end_date
        self.logger = setup_logger('Mktdata_update')
        self.logger.info('\nProcessing index component update...')

    def file_name_withdraw(self,index_type):
        if index_type == '上证50':
            return 'sz50'
        elif index_type == '沪深300':
            return 'hs300'
        elif index_type == '中证500':
            return 'zz500'
        elif index_type == '中证1000':
            return 'zz1000'
        elif index_type == '中证2000':
            return 'zz2000'
        elif index_type=='国证2000':
            return 'gz2000'
        else:
            return 'zzA500'

    def source_priority_withdraw(self):
        inputpath_config = glv.get('data_source_priority')
        df_config = pd.read_excel(inputpath_config, sheet_name='index_component')
        return df_config

    def index_dic_processing(self):
        dic_index = {'上证50': 'sz50', '沪深300': 'hs300', '中证500': 'zz500', '中证1000': 'zz1000',
                     '中证2000': 'zz2000', '中证A500': 'zzA500','国证2000':'gz2000'}
        return dic_index

    def index_component_update_main(self):
        df_config = self.source_priority_withdraw()
        df_config.sort_values(by='rank', inplace=True)
        source_name_list = df_config['source_name'].tolist()
        dic_index = self.index_dic_processing()
        outputpath_component = glv.get('output_indexcomponent')
        outputpath_port = glv.get('output_portfolio')
        if self.is_sql == True:
            inputpath_configsql = glv.get('config_sql')
            sm=gt.sqlSaving_main(inputpath_configsql,'indexComponent',delete=True)
            sm2=gt.sqlSaving_main(inputpath_configsql,'Portfolio',delete=True)
        for index_type in ['上证50', '沪深300', '中证500', '中证1000', '中证2000', '中证A500','国证2000']:
            index_code = dic_index[index_type]
            self.logger.info(f'\nProcessing index type: {index_type}')
            file_name = self.file_name_withdraw(index_type)
            outputpath_component_update_base = os.path.join(outputpath_component, file_name)
            outputpath_port_update_base = os.path.join(outputpath_port, str(index_code) + '_comp')
            gt.folder_creator2(outputpath_component_update_base)
            gt.folder_creator2(outputpath_port_update_base)
            input_list=os.listdir(outputpath_component_update_base)
            if len(input_list)==0:
                if self.start_date > '2023-06-01':
                    start_date = '2023-06-01'
                else:
                    start_date = self.start_date
            else:
                start_date=self.start_date
            working_days_list=gt.working_days_list(start_date,self.end_date)
            for available_date in working_days_list:
                target_date = gt.next_workday_calculate(available_date)
                target_date = gt.intdate_transfer(target_date)
                self.logger.info(f'Processing date: {available_date}')
                available_date=gt.intdate_transfer(available_date)
                if index_type == '中证2000' and int(available_date) < 20230901:
                    available_date2 = '20230901'
                elif index_type == '中证A500' and int(available_date) < 20241008:
                    available_date2 = '20241008'
                else:
                    available_date2 = available_date
                df_daily = pd.DataFrame()
                outputpath_port_update = os.path.join(outputpath_port_update_base,
                                                      str(index_code) + '_comp_' + target_date + '.csv')
                outputpath_component_update = os.path.join(outputpath_component_update_base,
                                                           index_code + 'ComponentWeight_' + available_date + '.csv')
                ic = indexComponent_prepare(available_date2)
                for source_name in source_name_list:
                    if source_name == 'jy':
                        df_daily =ic.raw_jy_index_component_preparing(index_type)
                    elif source_name == 'wind':
                        df_daily =ic.raw_wind_index_component_preparing(index_type)
                    else:
                        self.logger.warning('聚源和Windows都暂无数据')
                    if len(df_daily) != 0:
                        self.logger.info(f'{index_type}_component使用的数据源是: {source_name}')
                        break
                if len(df_daily) != 0:
                    df_daily['valuation_date']=gt.strdate_transfer(available_date)
                    df_daily['organization']=index_code
                    other_columns=[i for i in df_daily.columns.tolist() if i!='valuation_date']
                    df_daily=df_daily[['valuation_date']+other_columns]
                    df_daily.to_csv(outputpath_component_update, index=False)
                    df_port = df_daily[['code', 'weight']]
                    df_port['valuation_date']=gt.strdate_transfer(target_date)
                    df_port['portfolio_name']=str(index_code) + '_comp'
                    df_port=df_port[['valuation_date','portfolio_name','code','weight']]
                    df_port.to_csv(outputpath_port_update, index=False)
                    self.logger.info(f'Successfully saved {index_type} component data for date: {available_date}')
                    if self.is_sql == True:
                        now = datetime.now()
                        df_daily['update_time'] = now
                        df_port['update_time']=now
                        capture_file_withdraw_output(sm.df_to_sql, df_daily,'organization',index_code)
                        capture_file_withdraw_output(sm2.df_to_sql, df_port, 'portfolio_name', str(index_code) + '_comp')
                else:
                    self.logger.warning(f'{index_type}_component在{available_date}暂无数据')


class stockData_update:
    def __init__(self,start_date,end_date,is_sql):
        self.is_sql=is_sql
        self.start_date=start_date
        self.end_date=end_date
        self.logger = setup_logger('Mktdata_update')
        self.logger.info('\nProcessing stock data update...')

    def source_priority_withdraw(self):
        inputpath_config = glv.get('data_source_priority')
        df_config = pd.read_excel(inputpath_config, sheet_name='stock')
        return df_config

    def standardize_column_names(self,df):
        # 创建列名映射字典
        column_mapping = {
            # 代码相关
            'code': 'code',
            'ts_code': 'code',
            'qtid': 'code',
            'CODE': 'code',
            # 收盘价相关
            'close': 'close',
            'closeprice': 'close',
            'close_price': 'close',
            'CLOSE': 'close',
            'ClosePrice': 'close',
            # 前收盘价相关
            'pre_close': 'pre_close',
            'prevClose': 'pre_close',
            'prev_close': 'pre_close',
            'prevcloseprice': 'pre_close',
            'PRE_CLOSE': 'pre_close',
            'PrevClosePrice': 'pre_close',
            #开盘价相关
            'OPEN' : 'open',
            'open' : 'open',
            #最高价相关
            'HIGH' : 'high',
            'high' : 'high',
            'hi' : 'high',
            #最低价相关
            'LOW' : 'low',
            'low' : 'low',
            'lo' : 'low',
            #成交量相关：
            'vol' : 'volume',
            'VOLUME' : 'volume',
            #成交金额相关：
            'value' : 'amt',
            'AMT' : 'amt',
            'return' : 'pct_chg',
            'ret' : 'pct_chg',
            'vwap':'vwap',
            'adjfactor':'adjfactor',
            'ratioAdjFactor' : 'adjfactor',
            'tradeStatus' : 'trade_status',
            'tarde_status' : 'trade_status',
            'trade_status': 'trade_status',
        }
        # 处理列名：先转小写
        df.columns = df.columns.str.lower()
        # 处理列名：替换空格为下划线
        df.columns = df.columns.str.replace(' ', '_')
        # 处理列名：移除特殊字符
        df.columns = df.columns.str.replace('[^\w\s]', '')
        # 创建小写的映射字典
        lower_mapping = {k.lower(): v for k, v in column_mapping.items()}
        # 应用标准化映射
        renamed_columns = {col: lower_mapping.get(col, col) for col in df.columns}
        df = df.rename(columns=renamed_columns)
        # 获取所有标准化后的列名
        standardized_columns = set(column_mapping.values())
        # 只保留在映射字典中定义的列
        columns_to_keep = [col for col in df.columns if col in standardized_columns]
        df = df[columns_to_keep]
        # 定义固定的列顺序
        fixed_columns = ['code','open','high','low','close','pre_close','pct_chg','vwap','volume','amt','adjfactor','trade_status']
        # 只选择实际存在的列，并按固定顺序排列
        existing_columns = [col for col in fixed_columns if col in df.columns]
        df = df[existing_columns]
        return df

    def stock_data_update_main(self):
        df_config = self.source_priority_withdraw()
        outputpath_stock = glv.get('output_stock')
        gt.folder_creator2(outputpath_stock)
        input_list1=os.listdir(outputpath_stock)
        if len(input_list1)==0:
            if self.start_date > '2023-06-01':
                start_date = '2023-06-01'
            else:
                start_date = self.start_date
        else:
            start_date=self.start_date
        working_days_list=gt.working_days_list(start_date,self.end_date)
        if self.is_sql == True:
            inputpath_configsql = glv.get('config_sql')
            sm=gt.sqlSaving_main(inputpath_configsql,'stockData',delete=True)
        for available_date in working_days_list:
            self.logger.info(f'Processing date: {available_date}')
            df_stock=pd.DataFrame()
            available_date=gt.intdate_transfer(available_date)
            st = stockData_preparing(available_date)
            outputpath_stock_daily = os.path.join(outputpath_stock, 'stockdata_' + available_date + '.csv')
            df_config.sort_values(by='rank', inplace=True)
            source_name_list = df_config['source_name'].tolist()
            df_stock_wind=st.raw_wind_stockdata_withdraw()
            df_stock_jy=st.raw_jy_stockdata_withdraw()
            try:
                df_stock_wind = self.standardize_column_names(df_stock_wind)
            except:
                df_stock_wind = pd.DataFrame()
            try:
                df_stock_jy = self.standardize_column_names(df_stock_jy)
            except:
                df_stock_jy = pd.DataFrame()
            if len(df_stock_wind)>0 and len(df_stock_jy)==0:
                df_stock=df_stock_wind
                df_stock.rename(columns={'adjfactor':'adjfactor_wind'},inplace=True)
                df_stock['adjfactor_jy']=None
                self.logger.info('stock_data使用的数据源是: wind')
            elif len(df_stock_wind)==0 and len(df_stock_jy)>0:
                df_stock=df_stock_jy
                df_stock.rename(columns={'adjfactor':'adjfactor_jy'},inplace=True)
                df_stock['adjfactor_wind']=None
                self.logger.info('stock_data使用的数据源是: jy')
            elif len(df_stock_wind) >0 and len(df_stock_jy) > 0:
                df_stock_wind.rename(columns={'adjfactor':'adjfactor_wind'},inplace=True)
                df_stock_jy.rename(columns={'adjfactor': 'adjfactor_jy'}, inplace=True)
                all_codes = list(set(df_stock_wind['code']) | set(df_stock_jy['code']))
                all_codes.sort()
                # 保持Wind的列顺序，并添加Tushare独有的列
                wind_columns = df_stock_wind.columns.tolist()
                jy_only_columns = [col for col in df_stock_jy.columns if col not in wind_columns]
                all_columns = wind_columns + jy_only_columns

                # 创建新的DataFrame，包含所有列
                df_stock = pd.DataFrame(columns=all_columns)
                # 设置code列
                df_stock['code'] = all_codes
                if source_name_list[0] == 'wind':
                    df_priority = df_stock_wind
                    df_bu = df_stock_jy
                else:
                    df_priority = df_stock_jy
                    df_bu = df_stock_wind
                # 对于每一列（除了code列），优先使用Wind的数据，如果Wind的数据有缺失则使用Tushare的数据补充
                for col in all_columns:
                    if col == 'code':
                        continue
                    # 先使用Wind的数据
                    if col in df_priority.columns:
                        # 找出df_stock中在df_priority中存在的代码
                        valid_codes = df_stock['code'].isin(df_priority['code'])
                        # 只对存在的代码进行赋值
                        if valid_codes.any():
                            df_stock.loc[valid_codes, col] = df_priority.set_index('code').loc[df_stock.loc[valid_codes, 'code'], col].values
                        # 对不存在的代码设置为None
                        df_stock.loc[~valid_codes, col] = None
                    else:
                        df_stock[col] = None

                    # 如果Wind的数据有缺失，用Tushare的数据补充
                    if col in df_bu.columns:
                        # 找出当前列中为NaN的行
                        mask = df_stock[col].isna()
                        if mask.any():
                            # 只更新那些为NaN且code在df_bu中存在的行
                            codes_to_update = df_stock.loc[mask, 'code']
                            codes_in_bu = df_bu['code'].unique()
                            valid_mask = codes_to_update.isin(codes_in_bu)
                            if valid_mask.any():
                                idx_to_update = df_stock.loc[mask].index[valid_mask]
                                codes_valid = codes_to_update[valid_mask]
                                df_stock.loc[idx_to_update, col] = df_bu.set_index('code').loc[codes_valid, col].values
                self.logger.info('option_data使用的数据源是: wind和tushare的合并数据')
            else:
                df_stock=pd.DataFrame()
                self.logger.warning(f'stock_data {available_date} 两个数据源都没有数据')
                continue
            if len(df_stock) != 0:
                available_date2=gt.strdate_transfer(available_date)
                df_stock['valuation_date']=available_date2
                # 重新排列列顺序：valuation_date最前，adjfactor_jy和adjfactor_wind最后
                cols = df_stock.columns.tolist()
                # 移除valuation_date, adjfactor_jy, adjfactor_wind
                cols = [c for c in cols if c not in ['valuation_date', 'adjfactor_jy', 'adjfactor_wind']]
                # 只保留实际存在的adjfactor_jy和adjfactor_wind
                adj_cols = [c for c in ['adjfactor_jy', 'adjfactor_wind'] if c in df_stock.columns]
                df_stock = df_stock[['valuation_date'] + cols + adj_cols]
                df_stock.to_csv(outputpath_stock_daily, index=False)
                self.logger.info(f'Successfully saved stock data for date: {available_date}')
                if self.is_sql==True:
                    now = datetime.now()
                    df_stock['update_time'] = now
                    capture_file_withdraw_output(sm.df_to_sql, df_stock)
            else:
                self.logger.warning(f'stock_data {available_date} 四个数据源更新有问题')

class futureData_update:
    def __init__(self,start_date,end_date,is_sql):
        self.is_sql=is_sql
        self.start_date=start_date
        self.end_date=end_date
        self.logger = setup_logger('Mktdata_update')
        self.logger.info('\nProcessing future data update...')

    def source_priority_withdraw(self):
        inputpath_config = glv.get('data_source_priority')
        df_config = pd.read_excel(inputpath_config, sheet_name='future')
        return df_config

    def standardize_column_names(self,df):
        # 创建列名映射字典
        column_mapping = {
            # 代码相关
            'code': 'code',
            'ts_code': 'code',
            'qtid': 'code',
            'CODE': 'code',
            # 结算价相关
            'settle': 'settle',
            'settleprice': 'settle',
            'settle_price': 'settle',
            'SETTLE': 'settle',
            'settlement': 'settle',
            'settlementprice': 'settle',
            'settlement_price': 'settle',
            # 前结算价相关
            'pre_settle': 'pre_settle',
            'presettle': 'pre_settle',
            'prev_settle': 'pre_settle',
            'prevsettleprice': 'pre_settle',
            'PRE_SETTLE': 'pre_settle',
            'PrevSettlePrice': 'pre_settle',
            # 收盘价相关
            'close': 'close',
            'closeprice': 'close',
            'close_price': 'close',
            'CLOSE': 'close',
            'ClosePrice': 'close',
            # 前收盘价相关
            'pre_close': 'pre_close',
            'preclose': 'pre_close',
            'prev_close': 'pre_close',
            'prevcloseprice': 'pre_close',
            'PRE_CLOSE': 'pre_close',
            'PrevClosePrice': 'pre_close',
            # 合约乘数相关
            'multiplier': 'multiplier',
            'contractmultiplier': 'multiplier',
            'contract_multiplier': 'multiplier',
            'CONTRACTMULTIPLIER': 'multiplier',
            #开盘价相关
            'OPEN' : 'open',
            'open' : 'open',
            #最高价相关
            'HIGH' : 'high',
            'high' : 'high',
            #最低价相关
            'LOW' : 'low',
            'low' : 'low',
            #成交量相关：
            'vol' : 'volume',
            'VOLUME' : 'volume',
            #成交金额相关：
            'amount' : 'amt',
            'AMT' : 'amt',
            #持仓量：
            'oi' : 'oi',
            'OI' : 'oi',
            #持仓量变化
            'oi_chg' : 'oi_chg',
            'oi_CHG': 'oi_chg'
        }
        # 处理列名：先转小写
        df.columns = df.columns.str.lower()
        # 处理列名：替换空格为下划线
        df.columns = df.columns.str.replace(' ', '_')
        # 处理列名：移除特殊字符
        df.columns = df.columns.str.replace('[^\w\s]', '')
        # 创建小写的映射字典
        lower_mapping = {k.lower(): v for k, v in column_mapping.items()}
        # 应用标准化映射
        renamed_columns = {col: lower_mapping.get(col, col) for col in df.columns}
        df = df.rename(columns=renamed_columns)
        # 获取所有标准化后的列名
        standardized_columns = set(column_mapping.values())
        # 只保留在映射字典中定义的列
        columns_to_keep = [col for col in df.columns if col in standardized_columns]
        df = df[columns_to_keep]
        # 定义固定的列顺序
        fixed_columns = ['code', 'close', 'settle', 'pre_close', 'pre_settle','open','high','low','volume','amt','oi','oi_chg', 'multiplier']
        # 只选择实际存在的列，并按固定顺序排列
        existing_columns = [col for col in fixed_columns if col in df.columns]
        df = df[existing_columns]
        return df

    def get_code_multiplier(self,df):
        inputpath_future_uni = glv.get('input_futuredata_info_tushare')
        df_uni = gt.readcsv(inputpath_future_uni)
        df_uni = df_uni[['ts_code', 'multiplier', 'per_unit']]
        df_uni.columns=['code','multiplier', 'per_unit']
        df_uni.loc[df_uni['multiplier'].isna(), ['multiplier']] = df_uni[df_uni['multiplier'].isna()]['per_unit']
        df_uni = df_uni[['code', 'multiplier']]
        df_uni.columns=['code','multiplier']
        df=df.merge(df_uni,on='code',how='left')
        return df

    def future_data_update_main(self):
        df_config = self.source_priority_withdraw()
        outputpath_future_base = glv.get('output_futuredata')
        gt.folder_creator2(outputpath_future_base)
        input_list1=os.listdir(outputpath_future_base)
        if len(input_list1)==0:
            if self.start_date > '2024-01-01':
                start_date = '2024-01-01'
            else:
                start_date = self.start_date
        else:
            start_date=self.start_date
        working_days_list=gt.working_days_list(start_date,self.end_date)
        if self.is_sql == True:
            inputpath_configsql = glv.get('config_sql')
            sm=gt.sqlSaving_main(inputpath_configsql,'futureData',delete=True)
        for available_date in working_days_list:
            self.logger.info(f'Processing date: {available_date}')
            available_date=gt.intdate_transfer(available_date)
            ft = futureData_preparing(available_date)
            outputpath_future = os.path.join( outputpath_future_base, 'futureData_' + available_date + '.csv')
            df_config.sort_values(by='rank', inplace=True)
            source_name_list = df_config['source_name'].tolist()
            df_future = pd.DataFrame()
            for source_name in source_name_list:
                if source_name == 'wind':
                    df_future = ft.raw_wind_futuredata_withdraw()
                elif source_name == 'tushare':
                    df_future = ft.raw_tushare_futuredata_withdraw()
                else:
                    df_future = ft.raw_tushare_futuredata_withdraw()
                try:
                    df_future = self.standardize_column_names(df_future)
                    # 如果multiplier不在列名中且存在code列，则添加multiplier列
                    if 'multiplier' not in df_future.columns and 'code' in df_future.columns:
                        df_future = self.get_code_multiplier(df_future)
                except:
                    pass
                if len(df_future) != 0:
                    self.logger.info(f'future_data使用的数据源是: {source_name}')
                    break
            if len(df_future) != 0:
                df_future['valuation_date']=gt.strdate_transfer(available_date)
                df_future=df_future[['valuation_date']+df_future.columns.tolist()[:-1]]
                df_future.to_csv(outputpath_future, index=False)
                self.logger.info(f'Successfully saved future data for date: {available_date}')
                if self.is_sql==True:
                    now = datetime.now()
                    df_future['update_time'] = now
                    capture_file_withdraw_output(sm.df_to_sql, df_future)
            else:
                self.logger.warning(f'stock_data {available_date} 三个数据源更新有问题')

class optionData_update:
    def __init__(self,start_date,end_date,is_sql):
        self.is_sql=is_sql
        self.start_date=start_date
        self.end_date=end_date
        self.logger = setup_logger('Mktdata_update')
        self.logger.info('\nProcessing option data update...')

    def source_priority_withdraw(self):
        inputpath_config = glv.get('data_source_priority')
        df_config = pd.read_excel(inputpath_config, sheet_name='option')
        return df_config

    def standardize_column_names(self,df):
        # 创建列名映射字典
        column_mapping = {
            # 代码相关
            'code': 'code',
            'ts_code': 'code',
            'qtid': 'code',
            'CODE': 'code',
            # 结算价相关
            'settle': 'settle',
            'settleprice': 'settle',
            'settle_price': 'settle',
            'SETTLE': 'settle',
            'settlement': 'settle',
            'settlementprice': 'settle',
            'settlement_price': 'settle',
            # 前结算价相关
            'pre_settle': 'pre_settle',
            'presettle': 'pre_settle',
            'prev_settle': 'pre_settle',
            'prevsettleprice': 'pre_settle',
            'PRE_SETTLE': 'pre_settle',
            'PrevSettlePrice': 'pre_settle',
            # 收盘价相关
            'close': 'close',
            'closeprice': 'close',
            'close_price': 'close',
            'CLOSE': 'close',
            'ClosePrice': 'close',
            # 前收盘价相关
            'pre_close': 'pre_close',
            'preclose': 'pre_close',
            'prev_close': 'pre_close',
            'prevcloseprice': 'pre_close',
            'PRE_CLOSE': 'pre_close',
            'PrevClosePrice': 'pre_close',
            # 合约乘数相关
            'multiplier': 'multiplier',
            'contractmultiplier': 'multiplier',
            'contract_multiplier': 'multiplier',
            'CONTRACTMULTIPLIER': 'multiplier',
            #开盘价相关
            'OPEN' : 'open',
            'open' : 'open',
            #最高价相关
            'HIGH' : 'high',
            'high' : 'high',
            #最低价相关
            'LOW' : 'low',
            'low' : 'low',
            #成交量相关：
            'vol' : 'volume',
            'VOLUME' : 'volume',
            #成交金额相关：
            'amount' : 'amt',
            'AMT' : 'amt',
            #持仓量：
            'oi' : 'oi',
            'OI' : 'oi',
            #持仓量变化
            'oi_chg' : 'oi_chg',
            'oi_CHG': 'oi_chg',
            'DELTA' : 'delta_wind',
            'US_IMPLIEDVOL':'implied_vol_wind'
        }
        # 处理列名：先转小写
        df.columns = df.columns.str.lower()
        # 处理列名：替换空格为下划线
        df.columns = df.columns.str.replace(' ', '_')
        # 处理列名：移除特殊字符
        df.columns = df.columns.str.replace('[^\w\s]', '')
        # 创建小写的映射字典
        lower_mapping = {k.lower(): v for k, v in column_mapping.items()}
        # 应用标准化映射
        renamed_columns = {col: lower_mapping.get(col, col) for col in df.columns}
        df = df.rename(columns=renamed_columns)
        # 获取所有标准化后的列名
        standardized_columns = set(column_mapping.values())
        # 只保留在映射字典中定义的列
        columns_to_keep = [col for col in df.columns if col in standardized_columns]
        df = df[columns_to_keep]
        # 定义固定的列顺序
        fixed_columns = ['code', 'close', 'settle', 'pre_close', 'pre_settle','open','high','low','volume','amt','oi','oi_chg', 'multiplier','delta_wind','implied_vol_wind']
        # 只选择实际存在的列，并按固定顺序排列
        existing_columns = [col for col in fixed_columns if col in df.columns]
        df = df[existing_columns]
        if 'delta_wind' not in df.columns:
            df['delta_wind']=None
        if 'implied_vol_wind' not in df.columns:
            df['implied_vol_wind']=None
        return df
    def future_option_code_processing(self, df):
        def extract_dot_field(code):
            code_str = str(code)
            if '.' in code_str:
                return code_str.split('.')[0]
            else:
                return code_str
        df['code'] = df['code'].apply(extract_dot_field)
        return df
    def get_code_multiplier(self,df):
        inputpath_option_uni = glv.get('input_optiondata_info_tushare')
        df_uni = gt.readcsv(inputpath_option_uni)
        df_uni = df_uni[['ts_code', 'multiplier', 'per_unit']]
        df_uni.loc[df_uni['multiplier'].isna(), ['multiplier']] = df_uni[df_uni['multiplier'].isna()]['per_unit']
        df_uni = df_uni[['ts_code', 'multiplier']]
        df_uni.columns=['code','multiplier']
        df=df.merge(df_uni,on='code',how='left')
        return df
    def option_data_update_main(self):
        df_config = self.source_priority_withdraw()
        inputpath_future = glv.get('output_futuredata')
        outputpath_option_base = glv.get('output_optiondata')
        gt.folder_creator2(outputpath_option_base)
        input_list1=os.listdir(outputpath_option_base)
        if len(input_list1)==0:
            if self.start_date > '2024-01-01':
                start_date = '2024-01-01'
            else:
                start_date = self.start_date
        else:
            start_date=self.start_date
        working_days_list=gt.working_days_list(start_date,self.end_date)
        if self.is_sql == True:
            inputpath_configsql = glv.get('config_sql')
            sm=gt.sqlSaving_main(inputpath_configsql,'optionData',delete=True)
        for available_date in working_days_list:
            self.logger.info(f'Processing date: {available_date}')
            available_date=gt.intdate_transfer(available_date)
            ot = optionData_preparing(available_date)
            outputpath_option = os.path.join( outputpath_option_base, 'optionData_' + available_date + '.csv')
            df_config.sort_values(by='rank', inplace=True)
            source_name_list = df_config['source_name'].tolist()
            df_option = pd.DataFrame()
            inputpath_future_daily = gt.file_withdraw(inputpath_future, available_date)
            df_future = gt.readcsv(inputpath_future_daily)
            df_option_wind=ot.raw_wind_optiondata_withdraw()
            df_option_tushare=ot.raw_tushare_optiondata_withdraw()
            try:
                df_option_wind = self.standardize_column_names(df_option_wind)
            except:
                df_option_wind = pd.DataFrame()
            try:
                df_option_tushare = self.standardize_column_names(df_option_tushare)
            except:
                df_option_tushare = pd.DataFrame()
            if len(df_option_wind) > 0 and len(df_option_tushare) == 0:
                df_option = df_option_wind
                self.logger.info('option_data使用的数据源是: wind')
            elif len(df_option_wind) == 0 and len(df_option_tushare) > 0:
                df_option = df_option_tushare
                self.logger.info('option_data使用的数据源是: jy')
            elif len(df_option_wind) > 0 and len(df_option_tushare) > 0:
                df_option_wind=self.future_option_code_processing(df_option_wind)
                df_option_tushare = self.future_option_code_processing(df_option_tushare)
                all_codes = list(set(df_option_wind['code']) | set(df_option_tushare['code']))
                all_codes.sort()
                # 保持Wind的列顺序，并添加Tushare独有的列
                wind_columns = df_option_tushare.columns.tolist()
                jy_only_columns = [col for col in df_option_wind.columns if col not in wind_columns]
                all_columns = wind_columns + jy_only_columns
                # 创建新的DataFrame，包含所有列
                df_option = pd.DataFrame(columns=all_columns)
                # 设置code列
                df_option['code'] = all_codes
                if source_name_list[0] == 'wind':
                    df_priority = df_option_wind
                    df_bu = df_option_tushare
                else:
                    df_priority = df_option_tushare
                    df_bu = df_option_wind
                # 对于每一列（除了code列），优先使用Wind的数据，如果Wind的数据有缺失则使用Tushare的数据补充
                for col in all_columns:
                    if col == 'code':
                        continue
                    # 先使用Wind的数据
                    if col in df_priority.columns:
                        # 找出df_option中在df_priority中存在的代码
                        valid_codes = df_option['code'].isin(df_priority['code'])
                        # 只对存在的代码进行赋值
                        if valid_codes.any():
                            df_option.loc[valid_codes, col] = df_priority.set_index('code').loc[
                                df_option.loc[valid_codes, 'code'], col].values
                        # 对不存在的代码设置为None
                        df_option.loc[~valid_codes, col] = None
                    else:
                        df_option[col] = None
                    # 如果Wind的数据有缺失，用Tushare的数据补充
                    if col in df_bu.columns:
                        # 找出当前列中为NaN的行
                        mask = df_option[col].isna()
                        if mask.any():
                            # 只更新那些为NaN且code在df_bu中存在的行
                            codes_to_update = df_option.loc[mask, 'code']
                            codes_in_bu = df_bu['code'].unique()
                            valid_mask = codes_to_update.isin(codes_in_bu)
                            if valid_mask.any():
                                idx_to_update = df_option.loc[mask].index[valid_mask]
                                codes_valid = codes_to_update[valid_mask]
                                df_option.loc[idx_to_update, col] = df_bu.set_index('code').loc[codes_valid, col].values
            if len(df_option) != 0 and len(df_future) != 0:
                    available_date2 = gt.strdate_transfer(available_date)
                    df_option['valuation_date'] = gt.strdate_transfer(available_date)
                    df_option = df_option[['valuation_date'] + df_option.columns.tolist()[:-1]]
                    df_option.to_csv(outputpath_option, index=False)
                    dc = delta_calculator(available_date2, df_option, df_future)
                    df_option = dc.delta_calculator_main()
                    df_option.rename(columns={'Delta': 'delta', 'sigma': 'impliedvol'}, inplace=True)
                    df_option.to_csv(outputpath_option, index=False)
                    if self.is_sql == True:
                        now = datetime.now()
                        df_option['update_time'] = now
                        capture_file_withdraw_output(sm.df_to_sql, df_option)
            else:
                self.logger.info('option_data在'+str(available_date)+'更新有问题')
                self.logger.info('option_data使用的数据源是: wind和tushare的合并数据')


class etfData_update:
    def __init__(self,start_date,end_date,is_sql):
        self.is_sql=is_sql
        self.start_date=start_date
        self.end_date=end_date
        self.logger = setup_logger('Mktdata_update')
        self.logger.info('\nProcessing ETF data update...')

    def source_priority_withdraw(self):
        inputpath_config = glv.get('data_source_priority')
        df_config = pd.read_excel(inputpath_config, sheet_name='etf')
        return df_config

    def standardize_column_names(self,df):
        # 创建列名映射字典
        column_mapping = {
            # 代码相关
            'code': 'code',
            'ts_code': 'code',
            'qtid': 'code',
            'CODE': 'code',
            # 收盘价相关
            'close': 'close',
            'closeprice': 'close',
            'close_price': 'close',
            'CLOSE': 'close',
            'ClosePrice': 'close',
            # 前收盘价相关
            'pre_close': 'pre_close',
            'preclose': 'pre_close',
            'prev_close': 'pre_close',
            'prevcloseprice': 'pre_close',
            'PRE_CLOSE': 'pre_close',
            'PrevClosePrice': 'pre_close',
            # 开盘价相关
            'OPEN' : 'open',
            'open' : 'open',
            # 最高价相关
            'HIGH' : 'high',
            'high' : 'high',
            # 最低价相关
            'LOW' : 'low',
            'low' : 'low',
            # 成交量相关：
            'vol' : 'volume',
            'VOLUME' : 'volume',
            # 成交金额相关：
            'amount' : 'amt',
            'AMT' : 'amt',
            # 持仓量：
            'oi' : 'oi',
            'OI' : 'oi',
            # 持仓量变化
            'oi_chg' : 'oi_chg',
            'oi_CHG': 'oi_chg',
            'adjfactor': 'adjfactor',
            'adj_factor': 'adjfactor'
        }
        # 处理列名：先转小写
        df.columns = df.columns.str.lower()
        # 处理列名：替换空格为下划线
        df.columns = df.columns.str.replace(' ', '_')
        # 处理列名：移除特殊字符
        df.columns = df.columns.str.replace('[^\w\s]', '')
        # 创建小写的映射字典
        lower_mapping = {k.lower(): v for k, v in column_mapping.items()}
        # 应用标准化映射
        renamed_columns = {col: lower_mapping.get(col, col) for col in df.columns}
        df = df.rename(columns=renamed_columns)
        # 获取所有标准化后的列名
        standardized_columns = set(column_mapping.values())
        # 只保留在映射字典中定义的列
        columns_to_keep = [col for col in df.columns if col in standardized_columns]
        df = df[columns_to_keep]
        # 定义固定的列顺序
        fixed_columns = ['code', 'close', 'pre_close', 'open', 'high', 'low', 'volume', 'amt', 'oi', 'oi_chg','adjfactor']
        # 只选择实际存在的列，并按固定顺序排列
        existing_columns = [col for col in fixed_columns if col in df.columns]
        df = df[existing_columns]
        return df

    def etf_data_update_main(self):
        df_config = self.source_priority_withdraw()
        outputpath_etf_base = glv.get('output_etfdata')
        gt.folder_creator2(outputpath_etf_base)
        input_list1=os.listdir(outputpath_etf_base)
        if len(input_list1)==0:
            if self.start_date > '2024-01-01':
                start_date = '2024-01-01'
            else:
                start_date = self.start_date
        else:
            start_date=self.start_date
        working_days_list=gt.working_days_list(start_date,self.end_date)
        if self.is_sql == True:
            inputpath_configsql = glv.get('config_sql')
            sm=gt.sqlSaving_main(inputpath_configsql,'etfData',delete=True)
        for available_date in working_days_list:
            self.logger.info(f'Processing date: {available_date}')
            available_date=gt.intdate_transfer(available_date)
            et = etfData_preparing(available_date)
            outputpath_etf = os.path.join( outputpath_etf_base, 'etfData_' + available_date + '.csv')
            df_config.sort_values(by='rank', inplace=True)
            source_name_list = df_config['source_name'].tolist()
            df_etf = pd.DataFrame()
            for source_name in source_name_list:
                if source_name == 'wind':
                    df_etf = et.raw_wind_etfdata_withdraw()
                elif source_name == 'tushare':
                    df_etf = et.raw_tushare_etfdata_withdraw()
                else:
                    df_etf = et.raw_tushare_etfdata_withdraw()
                try:
                    df_etf = self.standardize_column_names(df_etf)
                except:
                    pass
                if len(df_etf) != 0:
                    self.logger.info(f'etf_data使用的数据源是: {source_name}')
                    break
            if len(df_etf) != 0:
                df_etf['valuation_date']=gt.strdate_transfer(available_date)
                df_etf=df_etf[['valuation_date']+df_etf.columns.tolist()[:-1]]
                df_etf.to_csv(outputpath_etf, index=False)
                self.logger.info(f'Successfully saved ETF data for date: {available_date}')
                if self.is_sql == True:
                    now = datetime.now()
                    df_etf['update_time'] = now
                    capture_file_withdraw_output(sm.df_to_sql, df_etf)
            else:
                self.logger.warning(f'etf_data {available_date} 三个数据源更新有问题')

class cbData_update:
    def __init__(self,start_date,end_date,is_sql):
        self.is_sql=is_sql
        self.start_date=start_date
        self.end_date=end_date
        self.logger = setup_logger('Mktdata_update')
        self.logger.info('\nProcessing convertible bond data update...')

    def source_priority_withdraw(self):
        inputpath_config = glv.get('data_source_priority')
        df_config = pd.read_excel(inputpath_config, sheet_name='c_bond')
        return df_config

    def standardize_column_names(self,df):
        # 创建列名映射字典
        column_mapping = {
            # 代码相关
            'code': 'code',
            'ts_code': 'code',
            'BONDCODE': 'code',
            # 代码相关
            'UNDERLYINGCODE': 'stock_code',
            'stk_code': 'stock_code',
            # 收盘价相关
            'close': 'close',
            'closeprice': 'close',
            'close_price': 'close',
            'CLOSE': 'close',
            'ClosePrice': 'close',
            # 前收盘价相关
            'pre_close': 'pre_close',
            'preclose': 'pre_close',
            'prev_close': 'pre_close',
            'prevcloseprice': 'pre_close',
            'PRE_CLOSE': 'pre_close',
            'PrevClosePrice': 'pre_close',
            # 到期日
            'PTMYEAR': 'maturity',
            # 开盘价相关
            'OPEN': 'open',
            'open': 'open',
            # 最高价相关
            'HIGH': 'high',
            'high': 'high',
            # 最低价相关
            'LOW': 'low',
            'low': 'low',
            # 转股价格:
            'CONVPRICE': 'conv_price',
            'conv_price': 'conv_price',

        }
        # 处理列名：先转小写
        df.columns = df.columns.str.lower()
        # 处理列名：替换空格为下划线
        df.columns = df.columns.str.replace(' ', '_')
        # 处理列名：移除特殊字符
        df.columns = df.columns.str.replace('[^\w\s]', '')
        # 创建小写的映射字典
        lower_mapping = {k.lower(): v for k, v in column_mapping.items()}
        # 应用标准化映射
        renamed_columns = {col: lower_mapping.get(col, col) for col in df.columns}
        df = df.rename(columns=renamed_columns)
        # 获取所有标准化后的列名
        standardized_columns = set(column_mapping.values())
        # 只保留在映射字典中定义的列
        columns_to_keep = [col for col in df.columns if col in standardized_columns]
        df = df[columns_to_keep]
        # 定义固定的列顺序
        fixed_columns = ['code', 'stock_code','close','open','high','low','pre_close','maturity','conv_price']
        # 只选择实际存在的列，并按固定顺序排列
        existing_columns = [col for col in fixed_columns if col in df.columns]
        df = df[existing_columns]
        return df

    def cb_data_update_main(self):
        df_config = self.source_priority_withdraw()
        outputpath_cb_base = glv.get('output_cbond')
        gt.folder_creator2(outputpath_cb_base)
        input_list1=os.listdir(outputpath_cb_base)
        if len(input_list1)==0:
            if self.start_date > '2024-01-01':
                start_date = '2024-01-01'
            else:
                start_date = self.start_date
        else:
            start_date=self.start_date
        working_days_list=gt.working_days_list(start_date,self.end_date)
        inputpath_timeseires=glv.get('output_timeseries')
        inputpath_timeseries=os.path.join(inputpath_timeseires,'stock_data')
        inputpath_timeseries=os.path.join(inputpath_timeseries,'stockReturn.csv')
        df_stock_return=gt.readcsv(inputpath_timeseries)
        if self.is_sql == True:
            inputpath_configsql = glv.get('config_sql')
            sm=gt.sqlSaving_main(inputpath_configsql,'convertibleBondData',delete=True)
        for available_date in working_days_list:
            self.logger.info(f'Processing date: {available_date}')
            available_date=gt.intdate_transfer(available_date)
            cb = CBData_preparing(available_date)
            outputpath_cb = os.path.join( outputpath_cb_base, 'cbData_' + available_date + '.csv')
            df_config.sort_values(by='rank', inplace=True)
            source_name_list = df_config['source_name'].tolist()
            df_cb = pd.DataFrame()
            for source_name in source_name_list:
                if source_name == 'wind':
                    df_cb = cb.raw_wind_cbdata_withdraw()
                elif source_name == 'tushare':
                    df_cb = cb.raw_tushare_cbdata_withdraw()
                else:
                    df_cb = cb.raw_tushare_cbdata_withdraw()
                try:
                    df_cb = self.standardize_column_names(df_cb)
                except:
                    pass
                if len(df_cb) != 0:
                    self.logger.info(f'cb_data使用的数据源是: {source_name}')
                    break
            if len(df_cb) != 0:
                outputpath_stock_close_base = glv.get('output_stock')
                outputpath_stock = gt.file_withdraw(outputpath_stock_close_base, available_date)
                df_stock = gt.readcsv(outputpath_stock)
                df_stock=df_stock[['code','close']]
                cbdelta = cb_delta_calculator(available_date, df_cb, df_stock, df_stock_return)
                df_cb = cbdelta.CB_delta_calculate()
                df_cb['valuation_date'] = gt.strdate_transfer(available_date)
                df_cb = df_cb[['valuation_date'] + df_cb.columns.tolist()[:-1]]
                df_cb.to_csv(outputpath_cb, index=False)
                self.logger.info(f'Successfully saved convertible bond data for date: {available_date}')
                if self.is_sql == True:
                    now = datetime.now()
                    df_cb['update_time'] = now
                    capture_file_withdraw_output(sm.df_to_sql, df_cb)
            else:
                self.logger.warning(f'cb_data {available_date} 三个数据源更新有问题')

class lhb_amt_update_main:
    def __init__(self, start_date, end_date,is_sql):
        self.is_sql=is_sql
        self.logger = setup_logger('Mktdata_update')
        self.logger.info('\n' + '*'*50 + '\nLHB AMOUNT UPDATE PROCESS\n' + '*'*50)
        self.start_date = start_date
        self.end_date = end_date
    def lhb_amt_update_main(self):
        outputpath=glv.get('output_lhb')
        gt.folder_creator2(outputpath)
        inputlist=os.listdir(outputpath)
        if len(inputlist)==0:
            if self.start_date > '2024-07-01':
                start_date = '2024-07-01'
            else:
                start_date = self.start_date
        else:
            start_date=self.start_date
        if self.is_sql == True:
            inputpath_configsql = glv.get('config_sql')
            sm=gt.sqlSaving_main(inputpath_configsql,'lhbData',delete=True)
        working_days_list=gt.working_days_list(start_date,self.end_date)
        for available_date in working_days_list:
            df_final=pd.DataFrame()
            available_date2=gt.intdate_transfer(available_date)
            outputpath_daily=os.path.join(outputpath,'lhb_amt_'+str(available_date2)+'.csv')
            lap = LHB_amt_prepare(available_date)
            df_lhb,df_index=lap.raw_LHB_tushare_withdraw()
            skipped_dates=0
            if df_index.empty or df_lhb.empty:
                skipped_dates += 1
                continue
            try:
                # 只保留.SH和.SZ后缀的股票
                df_lhb = df_lhb[df_lhb['ts_code'].str.endswith(('.SH', '.SZ'))]
                # 排除特定指数并计算总成交额
                exclude_codes = ['932000.CSI', '999004.SSI', '000510.CSI']
                df_index_filtered = df_index[~df_index['code'].isin(exclude_codes)]
                amt_sum = df_index_filtered['amt'].sum()

                # 计算龙虎榜成交额
                df_lhb_unique = df_lhb.loc[df_lhb.groupby('ts_code')['amount'].idxmin()]
                lhb_sum = df_lhb_unique['amount'].sum()

                # 计算占比
                if amt_sum != 0:
                    proportion = lhb_sum / amt_sum
                else:
                    proportion=None
                if proportion != None:
                    df_final['valuation_date'] = [available_date]
                    df_final['LHBProportion'] = [proportion]
                    df_final.to_csv(outputpath_daily,index=False)
                    self.logger.info(f'Successfully saved lhb amt for date: {available_date}')
                    if self.is_sql == True:
                        now = datetime.now()
                        df_final['update_time'] = now
                        capture_file_withdraw_output(sm.df_to_sql, df_final)
                else:
                    self.logger.error(f'处理龙虎榜数据时: {available_date2} 发生错误')
            except:
                self.logger.error(f'处理龙虎榜数据时: {available_date2} 发生错误')

class nlb_update_main:
    def __init__(self, start_date, end_date,is_sql):
        self.is_sql=is_sql
        self.logger = setup_logger('Mktdata_update')
        self.logger.info('\n' + '*'*50 + '\nNLB UPDATE PROCESS\n' + '*'*50)
        self.start_date = start_date
        self.end_date = end_date
    def nlb_update_main(self):
        outputpath=glv.get('output_nlb')
        gt.folder_creator2(outputpath)
        inputlist=os.listdir(outputpath)
        if len(inputlist)==0:
            if self.start_date > '2024-07-01':
                start_date = '2024-07-01'
            else:
                start_date = self.start_date
        else:
            start_date=self.start_date
        working_days_list=gt.working_days_list(start_date,self.end_date)
        if self.is_sql == True:
            inputpath_configsql = glv.get('config_sql')
            sm=gt.sqlSaving_main(inputpath_configsql,'NetLeverageBuying')
        for available_date in working_days_list:
            df_final=pd.DataFrame()
            available_date2=gt.intdate_transfer(available_date)
            outputpath_daily=os.path.join(outputpath,'NetLeverageBuying_'+str(available_date2)+'.csv')
            nlp = NLB_amt_prepare(available_date)
            df_leverage,df_index=nlp.raw_NLB_wind_withdraw()
            skipped_dates=0
            if df_index.empty or df_leverage.empty:
                skipped_dates += 1
                continue
            try:
                df_leverage['NetLeverage_buying'] = df_leverage['MRG_LONG_AMT']
                # 获取沪深300和中证1000的成分股
                hs300_stocks = gt.index_weight_withdraw('沪深300', available_date)
                zz1000_stocks = gt.index_weight_withdraw('中证1000', available_date)
                gz2000_stocks = gt.index_weight_withdraw('国证2000', available_date)
                hs300_stocks = hs300_stocks['code'].tolist()
                zz1000_stocks = zz1000_stocks['code'].tolist()
                gz2000_stocks = gz2000_stocks['code'].tolist()
                # 计算指数的NetLeverage_buying
                nlb_hs300 = df_leverage[df_leverage['CODE'].isin(hs300_stocks)]['NetLeverage_buying'].sum()
                nlb_zz1000 = df_leverage[df_leverage['CODE'].isin(zz1000_stocks)]['NetLeverage_buying'].sum()
                nlb_gz2000 = df_leverage[df_leverage['CODE'].isin(gz2000_stocks)]['NetLeverage_buying'].sum()
                # 获取指数成交额
                amt_hs300 = df_index[df_index['code'] == '000300.SH']['amt'].iloc[0]
                amt_zz1000 = df_index[df_index['code'] == '000852.SH']['amt'].iloc[0]
                amt_gz2000 = df_index[df_index['code'] == '932000.CSI']['amt'].iloc[0]
                # 计算相对于成交额的比例
                nlb_ratio_hs300 = nlb_hs300 / amt_hs300 if amt_hs300 != 0 else 0
                nlb_ratio_zz1000 = nlb_zz1000 / amt_zz1000 if amt_zz1000 != 0 else 0
                nlb_ratio_gz2000 = nlb_gz2000 / amt_gz2000 if amt_gz2000 != 0 else 0
                if nlb_ratio_gz2000 != 0 and nlb_ratio_zz1000 != 0 and nlb_ratio_hs300 != 0:
                    NetLeverageAMTProportion_difference = nlb_ratio_hs300 - nlb_ratio_zz1000 - nlb_ratio_gz2000
                    df_final['organization']=['hs300','zz1000','gz2000','NetLeverageAMTProportion_difference']
                    df_final['type']='NetLeverageBuying'
                    df_final['valuation_date'] = available_date
                    df_final['value'] = [nlb_ratio_hs300,nlb_ratio_zz1000,nlb_ratio_gz2000,NetLeverageAMTProportion_difference]
                    self.logger.info(f'Successfully saved nlb data for date: {available_date}')
                    df_final.to_csv(outputpath_daily, index=False)
                    if self.is_sql == True:
                        capture_file_withdraw_output(sm.df_to_sql, df_final)
                else:
                    self.logger.error(f'处理融资融券时: {available_date2} 发生错误')
            except:
                self.logger.error(f'处理融资融券时: {available_date2} 发生错误')
class futureDifference_update_main:
    def __init__(self, start_date, end_date,is_sql):
        self.is_sql=is_sql
        self.logger = setup_logger('Mktdata_update')
        self.logger.info('\n' + '*'*50 + '\nFuture Difference UPDATE PROCESS\n' + '*'*50)
        self.start_date = start_date
        self.end_date = end_date

    def index_future_withdraw(self,x):
        if 'IF' in x:
            return 'hs300'
        elif 'IC' in x:
            return 'zz500'
        elif 'IM' in x:
            return 'zz1000'
        else:
            return 'other'
    def FutureDifference_update_main(self):
        outputpath=glv.get('output_futureDifference')
        gt.folder_creator2(outputpath)
        inputlist=os.listdir(outputpath)
        if len(inputlist)==0:
            if self.start_date > '2024-07-01':
                start_date = '2024-07-01'
            else:
                start_date = self.start_date
        else:
            start_date=self.start_date
        working_days_list=gt.working_days_list(start_date,self.end_date)
        if self.is_sql == True:
            inputpath_configsql = glv.get('config_sql')
            sm=gt.sqlSaving_main(inputpath_configsql,'IndexFutureDifference')
        for available_date in working_days_list:
            available_date2 = gt.intdate_transfer(available_date)
            outputpath_daily = os.path.join(outputpath, 'FutureDifference_' + str(available_date2) + '.csv')
            try:
                lap = FutureDifference_prepare(available_date)
                df_future, df_index = lap.raw_futureDifference_withdraw()
                skipped_dates = 0
                if df_index.empty or df_future.empty:
                    skipped_dates += 1
                    continue
                df_future['is_index'] = df_future['code'].apply(lambda x: self.index_future_withdraw(x))
                df_future = df_future[~(df_future['is_index'] == 'other')]
                df_future['len'] = df_future['code'].apply(lambda x: len(x))
                df_future = df_future[df_future['len'] == 10]
                index_type_list = df_future['is_index'].unique().tolist()
                df_hs300 = df_future[df_future['is_index'] == 'hs300']
                if 'zz1000' not in index_type_list:
                    use_index = 'zz500'
                    use_index2='000905.SH'
                else:
                    use_index = 'zz1000'
                    use_index2 = '000852.SH'
                df_zz = df_future[df_future['is_index'] == use_index]
                df_hs300.sort_values('code', inplace=True)
                df_zz.sort_values('code', inplace=True)
                df_hs300.reset_index(inplace=True, drop=True)
                df_zz.reset_index(inplace=True, drop=True)
                df_hs300 = df_hs300.loc[1:]
                df_zz = df_zz.loc[1:]
                future_close_hs300 = df_hs300['close'].mean()
                future_close_zz = df_zz['close'].mean()
                index_close_hs300 = df_index[df_index['code']=='000300.SH']['close'].tolist()[0]
                index_close_zz = df_index[df_index['code']==use_index2]['close'].tolist()[0]
                difference_hs300 = index_close_hs300 - future_close_hs300
                difference_zz = index_close_zz - future_close_zz
                difference_future = difference_hs300 - difference_zz
                df_add=pd.DataFrame()
                df_add['organization']=['hs300','zz1000','indexFuture_difference']
                df_add['type']='FutureDifference'
                df_add['valuation_date']=available_date
                df_add['value']=[difference_hs300,difference_zz,difference_future]
                if len(df_add) > 0:
                    df_add.to_csv(outputpath_daily,index=False)
                    self.logger.info(f'Successfully saved future difference data for date: {available_date}')
                    if self.is_sql == True:
                        capture_file_withdraw_output(sm.df_to_sql, df_add)
                else:
                    self.logger.error(f"处理基差日期 {available_date} 时出错: {str(e)}")
            except Exception as e:
                self.logger.error(f"处理基差日期 {available_date} 时出错: {str(e)}")
