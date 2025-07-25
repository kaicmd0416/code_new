import pandas as pd
import os
import sys
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import datetime as datetime
import global_tools as gt
import global_setting.global_dic as glv
import yaml
import numpy as np
global source,config_path
config_path=glv.get('path_config')
source=gt.source_getting2(config_path)
class futureoption_position:
    def __init__(self,product_code):
        self.product_code=product_code
    def standardize_column_names_future(self,df):
        # 创建列名映射字典
        column_mapping = {
            # 代码相关
            '合约代码': 'code',
            'InstrumentID': 'code',
            '合约': 'code',
            '市场代码': 'mkt_code',
            '合约名称': 'chi_name',
            '多空': 'direction',
            '买卖': 'direction',
            'PosiDirection': 'direction',
            '总持仓': 'quantity',
            'Position': 'quantity',
            '昨仓': 'pre_quantity',
            'YdPosition': 'pre_quantity',
            '今仓': 'today_quantity',
            '持仓均价': 'unit_cost',
            '持仓成本': 'cost',
            '合约价值': 'mkt_value',
            '持仓盈亏': 'profit',
            'PositionProfit': 'profit',
            '最新价': 'price',
            'SettlementPrice': 'price',
            '当日涨幅': 'pct_chg'
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
        fixed_columns = ['code', 'price', 'pct_chg', 'mkt_value', 'mkt_code', 'direction', 'chi_name', 'quantity',
                         'pre_quantity', 'today_quantity', 'unit_cost', 'cost', 'profit']
        # 只选择实际存在的列，并按固定顺序排列
        existing_columns = [col for col in fixed_columns if col in df.columns]
        df = df[existing_columns]
        return df
    def read_csv_file(self,file_name):
        df = pd.read_csv(file_name, header=None)
        df.columns = df.iloc[0, :].map(lambda x: x.split('=')[0])
        df = df.map(lambda x: x.split('=')[1])
        return df
    def df_classification(self, df):
        def get_asset_type(code):
            if '-' in str(code):
                return 'option'
            else:
                return 'future'
        df['asset_type'] = df['code'].apply(get_asset_type)
        return df
    def direction_transfer(self,x):
        if x == '2':
            return '多'
        else:
            return '空'
    def position_withdraw_hy(self):
        inputpath_holding = glv.get('future_info_xy')
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        today2=gt.intdate_transfer(today)
        if source=='local':
            inputpath_holding_today=os.path.join(inputpath_holding,today2)
            inputpath_holding_today = os.path.join(inputpath_holding_today, 'GLDH5000602925.position.dat')
            df_holding=self.read_csv_file(inputpath_holding_today)
            df_holding= self.standardize_column_names_future(df_holding)
            df_holding['direction'] = df_holding['direction'].apply(lambda x: self.direction_transfer(x))
            df_holding['valuation_date'] = gt.strdate_transfer(today)
        else:
            inputpath_holding_today=str(inputpath_holding)+f" Where product_code='SGS958' And valuation_date='{today}'"
            df_holding=gt.data_getting(inputpath_holding_today,config_path)
        df_holding=df_holding[['valuation_date','code','direction','quantity','pre_quantity']]
        df_holding=self.df_classification(df_holding)
        df_future=df_holding[df_holding['asset_type']=='future']
        df_option=df_holding[df_holding['asset_type']=='option']
        df_future.drop(columns='asset_type',inplace=True)
        df_option.drop(columns='asset_type',inplace=True)
        df_future=self.fill_quantity_with_pre_quantity(df_future)
        df_option=self.fill_quantity_with_pre_quantity(df_option)
        return df_future,df_option
    def position_withdraw_renr(self):
        inputpath_holding = glv.get('future_info_renr')
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        today2=gt.intdate_transfer(today)
        if source=='local':
            inputpath_holding_today=os.path.join(inputpath_holding,today2)
            inputpath_holding_today = os.path.join(inputpath_holding_today, 'GLDH5000603056.position.dat')
            df_holding=self.read_csv_file(inputpath_holding_today)
            df_holding= self.standardize_column_names_future(df_holding)
            df_holding['direction'] = df_holding['direction'].apply(lambda x: self.direction_transfer(x))
            df_holding['valuation_date'] = gt.strdate_transfer(today)
        else:
            inputpath_holding_today=str(inputpath_holding)+f" Where product_code='SLA626' And valuation_date='{today}'"
            df_holding=gt.data_getting(inputpath_holding_today,config_path)
        df_holding=df_holding[['valuation_date','code','direction','quantity','pre_quantity']]
        df_holding = self.df_classification(df_holding)
        df_future = df_holding[df_holding['asset_type'] == 'future']
        df_option = df_holding[df_holding['asset_type'] == 'option']
        df_future.drop(columns='asset_type', inplace=True)
        df_option.drop(columns='asset_type', inplace=True)
        df_future=self.fill_quantity_with_pre_quantity(df_future)
        df_option=self.fill_quantity_with_pre_quantity(df_option)
        return df_future, df_option
    def position_withdraw_rrjx(self):
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        today2 = gt.intdate_transfer(today)
        inputpath_holding = glv.get('future_info_rr')
        if source=='local':
            inputpath_holding=os.path.join(inputpath_holding,'PositionDetail(金砖1号)'+f"({today2}).csv")
            df_holding=gt.data_getting(inputpath_holding,config_path)
            df_holding=self.standardize_column_names_future(df_holding)
            df_holding['valuation_date']=today
        else:
            inputpath_holding_today = str(
                inputpath_holding) + f" Where product_code='SNY426' And valuation_date='{today}'"
            df_holding = gt.data_getting(inputpath_holding_today, config_path)
        df_holding = df_holding[['valuation_date', 'code', 'direction', 'quantity', 'pre_quantity']]
        df_holding = self.df_classification(df_holding)
        df_future = df_holding[df_holding['asset_type'] == 'future']
        df_option = df_holding[df_holding['asset_type'] == 'option']
        df_future.drop(columns='asset_type', inplace=True)
        df_option.drop(columns='asset_type', inplace=True)
        return df_future, df_option
    def position_withdraw_rr500(self):
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        today2 = gt.intdate_transfer(today)
        inputpath_holding = glv.get('future_info_rr')
        if source == 'local':
            inputpath_holding = os.path.join(inputpath_holding, 'PositionDetail(量化500增强2)' + f"({today2}).csv")
            df_holding = gt.data_getting(inputpath_holding, config_path)
            df_holding = self.standardize_column_names_future(df_holding)
            df_holding['valuation_date'] = today
        else:
            inputpath_holding_today = str(
                inputpath_holding) + f" Where product_code='SSS044' And valuation_date='{today}'"
            df_holding = gt.data_getting(inputpath_holding_today, config_path)
        df_holding = df_holding[['valuation_date', 'code', 'direction', 'quantity', 'pre_quantity']]
        df_holding = self.df_classification(df_holding)
        df_future = df_holding[df_holding['asset_type'] == 'future']
        df_option = df_holding[df_holding['asset_type'] == 'option']
        df_future.drop(columns='asset_type', inplace=True)
        df_option.drop(columns='asset_type', inplace=True)
        return df_future, df_option
    def position_withdraw_other(self):
        if self.product_code=='STH580':
            inputpath=glv.get('future_info_rj')
        elif self.product_code=='SST132':
            inputpath=glv.get('future_info_zx')
        elif self.product_code=='SVU353':
            inputpath=glv.get('future_info_gy')
        else:
            raise ValueError
        target_date=datetime.date.today()
        for i in range(2):
            target_date=gt.last_workday_calculate(target_date)
            last_day=gt.last_workday_calculate(target_date)
            target_date2 = gt.intdate_transfer(target_date)
            last_day2=gt.intdate_transfer(last_day)
            if source == 'local':
                inputpath_holding = gt.file_withdraw(inputpath, target_date2)
                inputpath_holding_yes=gt.file_wtidhraw(inputpath,last_day2)
                df_holding = gt.data_getting(inputpath_holding, config_path)
                df_holding_yes=gt.data_getting(inputpath_holding_yes,config_path)
            else:
                inputpath_holding = str(
                    inputpath) + f" Where product_code='{self.product_code}' And valuation_date='{target_date}'"
                inputpath_holding_yes = str(
                    inputpath) + f" Where product_code='{self.product_code}' And valuation_date='{last_day}'"
                df_holding = gt.data_getting(inputpath_holding, config_path)
                df_holding_yes = gt.data_getting(inputpath_holding_yes, config_path)
            if len(df_holding)!=0:
                break
        df_holding=df_holding[(df_holding['asset_type']=='future')|(df_holding['asset_type']=='option')]
        df_holding_yes = df_holding_yes[(df_holding_yes['asset_type'] == 'future') | (df_holding_yes['asset_type'] == 'option')]
        df_holding=df_holding[['valuation_date','code','quantity','mkt_value']]
        df_holding_yes=df_holding_yes[['code','quantity']]
        df_holding_yes.columns=['code','pre_quantity']
        def direction_transfer(x):
            if x>0:
                return '多'
            else:
                return '空'
        df_holding['direction']=df_holding['mkt_value'].apply(lambda x: direction_transfer(x))
        df_holding=df_holding[['valuation_date', 'code', 'direction', 'quantity']]
        df_holding=df_holding.merge(df_holding_yes,on='code',how='outer')
        df_holding.fillna(0,inplace=True)
        df_holding = self.df_classification(df_holding)
        df_future = df_holding[df_holding['asset_type'] == 'future']
        df_option = df_holding[df_holding['asset_type'] == 'option']
        df_future.drop(columns='asset_type', inplace=True)
        df_option.drop(columns='asset_type', inplace=True)
        return df_future, df_option
    def fill_quantity_with_pre_quantity(self, df):
        if 'quantity' in df.columns and 'pre_quantity' in df.columns:
            df['quantity'] = df['quantity'].replace([None, 'None', 'nan', '', np.nan], np.nan)
            df['quantity'] = df['quantity'].astype(float)
            df['quantity'] = df['quantity'].fillna(df['pre_quantity'])
        return df
    def futureoption_withdraw(self):
        if self.product_code=='SGS958':
            df_future,df_option=self.position_withdraw_hy()
        elif self.product_code=='SLA626':
            df_future,df_option=self.position_withdraw_renr()
        elif self.product_code=='SNY426':
            df_future,df_option=self.position_withdraw_rrjx()
        elif self.product_code=='SSS044':
            df_future,df_option=self.position_withdraw_rr500()
        else:
            df_future,df_option=self.position_withdraw_other()
        return df_future,df_option
class security_position:
    def __init__(self,product_code):
        self.product_code=product_code
    def pool_withdraw(self):
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        df_etf=gt.etfdata_withdraw(today,True)
        df_stock=gt.stockdata_withdraw(today,True)
        df_cb=gt.cbdata_withdraw(today,True)
        etf_pool=df_etf['code'].unique().tolist()
        stock_pool=df_stock['code'].unique().tolist()
        cb_pool=df_cb['code'].unique().tolist()
        return stock_pool,etf_pool,cb_pool
    def df_classification(self,df):
        stock_pool,etf_pool,cb_pool=self.pool_withdraw()
        def get_asset_type(code):
            if code in stock_pool:
                return 'stock'
            elif code in etf_pool:
                return 'etf'
            elif code in cb_pool:
                return 'convertible_bond'
            else:
                return None
        df['asset_type'] = df['code'].apply(get_asset_type)
        return df

    def standardize_column_names_stock(self,df):
        # 创建列名映射字典
        column_mapping = {
            # 代码相关
            '证券代码': 'code',
            '代码':'code',
            '市场名称': 'mkt_name',
            '证券名称': 'chi_name',
            '当前拥股': 'quantity',
            '数量':'quantity',
            '成本价': 'unit_cost',
            '当前成本': 'cost',
            '市值': 'mkt_value',
            '盈亏': 'profit',
            '可用余额': 'valid_quantity',
            '昨夜拥股': 'pre_quantity',
            '最新价': 'price',
            '当日涨幅': 'pct_chg'
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
        fixed_columns = ['code', 'price', 'pct_chg', 'mkt_value', 'mkt_name', 'chi_name', 'quantity', 'unit_cost',
                         'cost', 'profit', 'valid_quantity', 'pre_quantity']
        # 只选择实际存在的列，并按固定顺序排列
        existing_columns = [col for col in fixed_columns if col in df.columns]
        df = df[existing_columns]
        return df
    def position_withdraw_hy(self):
        inputpath_holding = glv.get('stock_info_xy')
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        today2=gt.intdate_transfer(today)
        if source=='local':
            inputpath_holding_today = os.path.join(inputpath_holding, f"PositionDetail({today2}).csv")
            df_holding=gt.readcsv(inputpath_holding_today)
            df_holding= self.standardize_column_names_stock(df_holding)
        else:
            inputpath_holding_today=str(inputpath_holding)+f" Where product_code='SGS958' And valuation_date='{today}'"
            df_holding=gt.data_getting(inputpath_holding_today,config_path)
        df_holding=df_holding[['code','quantity','pre_quantity']]
        df_holding=gt.code_transfer(df_holding)
        df_holding = self.df_classification(df_holding)
        df_stock=df_holding[df_holding['asset_type']=='stock']
        df_etf=df_holding[df_holding['asset_type']=='etf']
        df_cb=df_holding[df_holding['asset_type']=='convertible_bond']
        df_stock.drop(columns='asset_type',inplace=True)
        df_etf.drop(columns='asset_type', inplace=True)
        df_cb.drop(columns='asset_type', inplace=True)
        return df_stock,df_etf,df_cb

    def position_withdraw_renr(self):
        inputpath_holding = glv.get('stock_info_renr')
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        today2 = gt.intdate_transfer(today)
        if source == 'local':
            inputpath_holding_today = os.path.join(inputpath_holding, f"PositionStatics-{today2}.csv")
            df_holding = gt.readcsv(inputpath_holding_today)
            df_holding = self.standardize_column_names_stock(df_holding)
        else:
            inputpath_holding_today = str(
                inputpath_holding) + f" Where product_code='SLA626' And valuation_date='{today}'"
            df_holding = gt.data_getting(inputpath_holding_today, config_path)
        df_holding = df_holding[['code', 'quantity','pre_quantity']]
        df_holding = gt.code_transfer(df_holding)
        df_holding = self.df_classification(df_holding)
        df_stock = df_holding[df_holding['asset_type'] == 'stock']
        df_etf = df_holding[df_holding['asset_type'] == 'etf']
        df_cb = df_holding[df_holding['asset_type'] == 'convertible_bond']
        df_stock.drop(columns='asset_type', inplace=True)
        df_etf.drop(columns='asset_type', inplace=True)
        df_cb.drop(columns='asset_type', inplace=True)
        return df_stock, df_etf, df_cb

    def position_withdraw_rrjx(self):
        inputpath_holding = glv.get('stock_info_rr')
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        today2 = gt.intdate_transfer(today)
        if source == 'local':
            inputpath_holding_today = os.path.join(inputpath_holding, f"PositionDetail(金砖1号)({today2}).csv")
            df_holding = gt.readcsv(inputpath_holding_today)
            df_holding = self.standardize_column_names_stock(df_holding)
        else:
            inputpath_holding_today = str(
                inputpath_holding) + f" Where product_code='SNY426' And valuation_date='{today}'"
            df_holding = gt.data_getting(inputpath_holding_today, config_path)
        df_holding = df_holding[['code', 'quantity','pre_quantity']]
        df_holding = gt.code_transfer(df_holding)
        df_holding = self.df_classification(df_holding)
        df_stock = df_holding[df_holding['asset_type'] == 'stock']
        df_etf = df_holding[df_holding['asset_type'] == 'etf']
        df_cb = df_holding[df_holding['asset_type'] == 'convertible_bond']
        df_stock.drop(columns='asset_type', inplace=True)
        df_etf.drop(columns='asset_type', inplace=True)
        df_cb.drop(columns='asset_type', inplace=True)
        return df_stock, df_etf, df_cb

    def position_withdraw_rr500(self):
        inputpath_holding = glv.get('stock_info_rr')
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        today2 = gt.intdate_transfer(today)
        if source == 'local':
            inputpath_holding_today = os.path.join(inputpath_holding, f"PositionDetail(量化中证500)({today2}).csv")
            df_holding = gt.readcsv(inputpath_holding_today)
            df_holding = self.standardize_column_names_stock(df_holding)
        else:
            inputpath_holding_today = str(
                inputpath_holding) + f" Where product_code='SSS044' And valuation_date='{today}'"
            df_holding = gt.data_getting(inputpath_holding_today, config_path)
        df_holding = df_holding[['code', 'quantity','pre_quantity']]
        df_holding = gt.code_transfer(df_holding)
        df_holding = self.df_classification(df_holding)
        df_stock = df_holding[df_holding['asset_type'] == 'stock']
        df_etf = df_holding[df_holding['asset_type'] == 'etf']
        df_cb = df_holding[df_holding['asset_type'] == 'convertible_bond']
        df_stock.drop(columns='asset_type', inplace=True)
        df_etf.drop(columns='asset_type', inplace=True)
        df_cb.drop(columns='asset_type', inplace=True)
        return df_stock, df_etf, df_cb
    def position_withdraw_other(self):
        if self.product_code=='STH580':
            inputpath=glv.get('future_info_rj')
        elif self.product_code=='SST132':
            inputpath=glv.get('future_info_zx')
        elif self.product_code=='SVU353':
            inputpath=glv.get('future_info_gy')
        else:
            raise ValueError
        target_date=datetime.date.today()
        for i in range(2):
            target_date=gt.last_workday_calculate(target_date)
            target_date2 = gt.intdate_transfer(target_date)
            last_date=gt.last_workday_calculate(target_date)
            last_date2=gt.intdate_transfer(last_date)
            if source == 'local':
                inputpath_holding = gt.file_withdraw(inputpath, target_date2)
                inputpath_holding_yes=gt.file_withdraw(inputpath,last_date2)
                df_holding = gt.data_getting(inputpath_holding, config_path)
                df_holding_yes=gt.data_getting(inputpath_holding_yes,config_path)
            else:
                inputpath_holding = str(
                    inputpath) + f" Where product_code='{self.product_code}' And valuation_date='{target_date}'"
                inputpath_holding_yes = str(
                    inputpath) + f" Where product_code='{self.product_code}' And valuation_date='{last_date}'"
                df_holding = gt.data_getting(inputpath_holding, config_path)
                df_holding_yes=gt.data_getting(inputpath_holding_yes,config_path)
            if len(df_holding)!=0:
                break
        df_holding=df_holding[['valuation_date', 'code', 'quantity','asset_type']]
        df_holding_yes=df_holding_yes[['code','quantity']]
        df_holding_yes.columns=['code','pre_quantity']
        df_holding=df_holding.merge(df_holding_yes,on='code',how='outer')
        df_holding.fillna(0,inplace=True)
        df_stock = df_holding[df_holding['asset_type'] == 'stock']
        df_etf = df_holding[df_holding['asset_type'] == 'etf']
        df_cb = df_holding[df_holding['asset_type'] == 'cbond']
        df_stock.drop(columns='asset_type', inplace=True)
        df_etf.drop(columns='asset_type', inplace=True)
        df_cb.drop(columns='asset_type', inplace=True)
        return df_stock, df_etf, df_cb
    def security_withdraw(self):
        if self.product_code=='SGS958':
            df_stock, df_etf, df_cb=self.position_withdraw_hy()
        elif self.product_code=='SLA626':
            df_stock, df_etf, df_cb=self.position_withdraw_renr()
        elif self.product_code=='SNY426':
            df_stock, df_etf, df_cb=self.position_withdraw_rrjx()
        elif self.product_code=='SSS044':
            df_stock, df_etf, df_cb=self.position_withdraw_rr500()
        else:
            df_stock, df_etf, df_cb=self.position_withdraw_other()
        return df_stock, df_etf, df_cb
class prod_info:
    def __init__(self,product_code):
        self.product_code=product_code
    def get_product_detail(self, field):
        yaml_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'project_config', 'product_detail.yaml')
        with open(yaml_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        if self.product_code not in data:
            raise ValueError(f"Product code {self.product_code} not found in product_detail.yaml")
        product_info = data[self.product_code]
        if field not in product_info:
            raise ValueError(f"Field '{field}' not found for product code {self.product_code}")
        return product_info[field]
    def availableDate_withdraw(self):
        if source=='local':
            inputpath = glv.get('prod_info')
            product_name = self.get_product_detail('name')
            inputpath = os.path.join(inputpath, product_name)
            inputlist = os.listdir(inputpath)
            inputlist = [str(i)[:8] for i in inputlist]
            inputlist.sort()
            available_date = inputlist[-1]
        else:
            inputpath = glv.get('prod_info')
            inputpath = str(inputpath) + f" Where product_code='{self.product_code}'"
            df = gt.data_getting(inputpath, config_path)
            df.sort_values(by='valuation_date', inplace=True)
            available_date = df['valuation_date'].unique().tolist()[-1]
        available_date = gt.strdate_transfer(available_date)
        return available_date
    def assetvalue_processing(self,asset_value,available_date):
        if asset_value is not None and asset_value != '':
            if isinstance(asset_value, str):
                asset_value = asset_value.replace(',', '')
                try:
                    asset_value = float(asset_value)
                except ValueError:
                    pass  # 如果不能转换为float则保持原值
        else:
            asset_value = float(asset_value)
        asset_type=self.get_product_detail('type')
        index_type=self.get_product_detail('index')
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        if asset_type=='中性':
            asset_value=asset_value
        else:
            working_days_list=gt.working_days_list(available_date,today)
            working_days_list=working_days_list[1:]
            for days in working_days_list:
                if days!=today:
                    index_return=gt.crossSection_index_return_withdraw(index_type,days)
                else:
                    index_return=gt.crossSection_index_return_withdraw(index_type,days,True)
                index_return=float(index_return)
                asset_value = (1 + index_return) * asset_value
        return asset_value

    def assetvalue_withdraw(self):
        available_date = self.availableDate_withdraw()
        available_date2 = gt.intdate_transfer(available_date)
        if source == 'local':
            inputpath = glv.get('prod_info')
            product_name = self.productName_transfer()
            inputpath = os.path.join(inputpath, product_name)
            inputpath = gt.file_withdraw(inputpath, available_date2)
        else:
            inputpath = glv.get('prod_info')
            inputpath = str(
                inputpath) + f" Where product_code='{self.product_code}' And valuation_date='{available_date}'"
        df = gt.data_getting(inputpath, config_path)
        asset_value = df['NetAssetValue'].unique().tolist()[0]
        asset_value = self.assetvalue_processing(asset_value, available_date)

        return asset_value
class weight_withdraw:
    def __init__(self):
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        self.available_date = today
        self.yes=gt.last_workday_calculate(self.available_date)
        self.realtime = True
    def portfolio_list_getting(self,yes=False):
        inputpath=glv.get('portfolio_weight')
        if yes==True:
            available_date=self.yes
        else:
            available_date=self.available_date
        if source=='local':
            portfolio_list=os.listdir(inputpath)
        else:
            inputpath=str(inputpath)+f" Where valuation_date='{available_date}'"
            df=gt.data_getting(inputpath,config_path,update_time=False)
            portfolio_list=df['portfolio_name'].unique().tolist()
        return portfolio_list
    def product_list_getting(self):
        product_list=['SGS958','SVU353','SNY426','SSS044','STH580','SST132','SLA626']
        return product_list
    def portfolio_withdraw(self,portfolio_name,yes=False):
        inputpath = glv.get('portfolio_weight')
        if yes==True:
            available_date=self.yes
        else:
            available_date=self.available_date
        if source == 'local':
            inputpath=os.path.join(inputpath,portfolio_name)
            inputpath=gt.file_withdraw(inputpath,gt.intdate_transfer(available_date))
        else:
            inputpath = str(inputpath) + f" Where valuation_date='{available_date}' And portfolio_name='{portfolio_name}'"
        df = gt.data_getting(inputpath, config_path)
        df=df[['code','weight']]
        return df
    def product_withdraw(self,product_code,yes=False):
        inputpath = glv.get('product_weight')
        if yes==True:
            available_date=self.yes
        else:
            available_date=self.available_date
        if source == 'local':
            inputpath=os.path.join(inputpath,product_code)
            inputpath=gt.file_withdraw(inputpath,gt.intdate_transfer(available_date))
        else:
            inputpath = str(inputpath) + f" Where valuation_date='{available_date}' And product_code='{product_code}'"
        df = gt.data_getting(inputpath, config_path)
        df=df[['code','weight']]
        return df
class mkt_data:
    def __init__(self):
        today = datetime.date.today()
        today = gt.strdate_transfer(today)
        self.available_date=today
        self.realtime=True
    def realtimeData_withdraw(self):
        df_stock = gt.stockdata_withdraw(self.available_date, self.realtime)
        df_future =  gt.futuredata_withdraw(self.available_date, self.realtime)
        df_etf =  gt.etfdata_withdraw(self.available_date, self.realtime)
        df_option =  gt.optiondata_withdraw(self.available_date, self.realtime)
        df_convertible_bond =  gt.cbdata_withdraw(self.available_date, self.realtime)
        df_adj_factor =  gt.stockdata_adj_withdraw(self.available_date, self.realtime, adj_source='wind')
        return df_stock,df_future,df_etf,df_option,df_convertible_bond,df_adj_factor
    def factorData_withdraw(self):
        available_date = gt.last_workday_calculate(datetime.date.today())
        df_sz50 = gt.crossSection_index_factorexposure_withdraw_new(index_type='上证50',
                                                                    available_date=available_date)
        df_hs300 = gt.crossSection_index_factorexposure_withdraw_new(index_type='沪深300',
                                                                     available_date=available_date)
        df_zz500 = gt.crossSection_index_factorexposure_withdraw_new(index_type='中证500',
                                                                     available_date=available_date)
        df_zz1000 = gt.crossSection_index_factorexposure_withdraw_new(index_type='中证1000',
                                                                      available_date=available_date)
        df_zz2000 = gt.crossSection_index_factorexposure_withdraw_new(index_type='中证2000',
                                                                      available_date=available_date)
        df_zzA500 = gt.crossSection_index_factorexposure_withdraw_new(index_type='中证A500',
                                                                      available_date=available_date)
        df_sz50.drop(columns='valuation_date', inplace=True)
        df_hs300.drop(columns='valuation_date', inplace=True)
        df_zz500.drop(columns='valuation_date', inplace=True)
        df_zz1000.drop(columns='valuation_date', inplace=True)
        df_zz2000.drop(columns='valuation_date', inplace=True)
        df_zzA500.drop(columns='valuation_date', inplace=True)
        return df_sz50, df_hs300, df_zz500, df_zz1000, df_zz2000, df_zzA500

if __name__ == '__main__':
    ww=weight_withdraw()
    print(ww.product_withdraw('SLA626'))
