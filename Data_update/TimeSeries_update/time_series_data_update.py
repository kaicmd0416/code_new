import os
import pandas as pd
import global_setting.global_dic as glv
import sys
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import global_tools as gt
import numpy as np
import yaml
import pymysql
from setup_logger.logger_setup import setup_logger

class timeSeries_data_update:
    def __init__(self,start_date,end_date):
        self.logger = setup_logger('TimeseriesData_update')
        self.db_config = self._load_db_config()
        self.output_path = glv.get('output_timeseries')
        self.conn = None
        self.cursor = None
        self.start_date = start_date
        self.end_date = end_date
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
        
    def __enter__(self):
        """支持with语句的上下文管理"""
        self.connect_db()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """支持with语句的上下文管理"""
        self.close_db()

    def execute_sql_to_df(self, sql, params=None):
        """
        执行SQL查询并返回DataFrame
        
        Args:
            sql (str): SQL查询语句
            params (tuple/dict, optional): SQL参数，用于参数化查询
            
        Returns:
            pandas.DataFrame: 查询结果
            
        Examples:
            # 简单查询
            df = ts.execute_sql_to_df("SELECT * FROM table_name")
            
            # 带参数的查询
            df = ts.execute_sql_to_df("SELECT * FROM table_name WHERE date = %s", ('2024-01-01',))
            
            # 带多个参数的查询
            df = ts.execute_sql_to_df(
                "SELECT * FROM table_name WHERE date BETWEEN %s AND %s",
                ('2024-01-01', '2024-01-31')
            )
        """
        try:
            if not self.conn or not self.conn.open:
                self.connect_db()
                
            # 使用pandas的read_sql直接读取为DataFrame
            df = pd.read_sql(sql, self.conn, params=params)
            self.logger.info(f"SQL查询执行成功，返回 {len(df)} 行数据")
            return df
            
        except Exception as e:
            self.logger.error(f"SQL查询执行失败: {str(e)}")
            raise
    def df_transformer(self,df,type):
        # 获取唯一的估值日期
        valuation_date_list = df['valuation_date'].unique().tolist()
        
        # 将数据透视化：organization成为列，value作为值
        if type=='indexOther':
            df_pivot = df.pivot(index='valuation_date', columns='organization', values='value')
        elif type=='macroData':
            df_pivot = df.pivot(index='valuation_date', columns='name', values='value')
        else:
            raise ValueError
        
        # 重置索引，使valuation_date成为列
        df_pivot = df_pivot.reset_index()
        
        # 确保valuation_date是第一列
        cols = df_pivot.columns.tolist()
        cols.remove('valuation_date')
        df_pivot = df_pivot[['valuation_date'] + cols]
        
        return df_pivot
    def indexMktData_update(self):
        inputpath=os.path.join(self.output_path,'index_data')
        gt.folder_creator2(inputpath)
        for type in ['Amt','Close','PCT_chg','High','Low','Open','Turn_over','Volume']:
            type2=type.lower()
            if type=='PCT_chg':
                type='Return'
            name = 'Index' + str(type)
            inputpath_file=os.path.join(inputpath,name+'.csv')
            if os.path.exists(inputpath_file):
                df=pd.read_csv(inputpath_file)
                start_date=gt.strdate_transfer(self.start_date)
                end_date=gt.strdate_transfer(self.end_date)
                sql=f"SELECT valuation_date, code, {type2} as value FROM data_index WHERE valuation_date BETWEEN '{start_date}' AND '{end_date}'"
            else:
                df=pd.DataFrame()
                sql=f"SELECT valuation_date, code, {type2} as value FROM data_index"
            df_add=self.execute_sql_to_df(sql)
            if df_add.empty:
                self.logger.info(f"index_data没有找到{name}数据")
                continue
            
            # 处理数据转置
            try:
                # 确保列名正确
                df_add.columns = ['valuation_date', 'code', 'value']
                
                # 处理NULL值
                df_add['value'] = pd.to_numeric(df_add['value'], errors='coerce')
                
                # 按valuation_date和code分组，将value列的值作为新的列
                df_pivot = df_add.pivot(index='valuation_date', columns='code', values='value')
                
                # 重置索引，使valuation_date成为列
                df_pivot = df_pivot.reset_index()
                
                # 重命名列
                df_pivot.columns.name = None  # 移除columns的name
                
                # 去掉999004.SSI列
                if '999004.SSI' in df_pivot.columns:
                    df_pivot = df_pivot.drop(columns=['999004.SSI'])
                
                df_pivot['valuation_date']=df_pivot['valuation_date'].astype(str).apply(lambda x: gt.strdate_transfer(x))
                      
            except Exception as e:
                self.logger.error(f"index_data数据转置处理失败: {str(e)}")
                df_pivot=pd.DataFrame()
            if not df_pivot.empty:
            # 竖向拼接df和df_pivot
                df = pd.concat([df, df_pivot], axis=0, ignore_index=True)
                
                # 处理重复的valuation_date，保留最后一个
                df = df.drop_duplicates(subset=['valuation_date'], keep='last')
                
                # 按valuation_date排序
                df = df.sort_values('valuation_date')   
                df.to_csv(inputpath_file,index=False)  
            else:
                self.logger.info(f"index_data没有找到{name}数据在"+str(self.start_date)+"至"+str(self.end_date))

    def stockMktData_update(self):
        inputpath = os.path.join(self.output_path, 'stock_data')
        gt.folder_creator2(inputpath)
        for type in ['Close', 'PCT_chg']:
            type2 = type.lower()
            if type == 'PCT_chg':
                type = 'Return'
            name = 'stock' + str(type)
            inputpath_file = os.path.join(inputpath, name + '.csv')
            if os.path.exists(inputpath_file):
                df = pd.read_csv(inputpath_file)
                start_date = gt.strdate_transfer(self.start_date)
                end_date = gt.strdate_transfer(self.end_date)
                sql = f"SELECT valuation_date, code, {type2} as value FROM data_stock WHERE valuation_date BETWEEN '{start_date}' AND '{end_date}'"
            else:
                df = pd.DataFrame()
                sql = f"SELECT valuation_date, code, {type2} as value FROM data_stock"
            df_add = self.execute_sql_to_df(sql)
            if df_add.empty:
                self.logger.info(f"stock_data没有找到{name}数据")
                continue

            # 处理数据转置
            try:
                # 确保列名正确
                df_add.columns = ['valuation_date', 'code', 'value']

                # 处理NULL值
                df_add['value'] = pd.to_numeric(df_add['value'], errors='coerce')

                # 按valuation_date和code分组，将value列的值作为新的列
                df_pivot = df_add.pivot(index='valuation_date', columns='code', values='value')

                # 重置索引，使valuation_date成为列
                df_pivot = df_pivot.reset_index()

                # 重命名列
                df_pivot.columns.name = None  # 移除columns的name
                df_pivot['valuation_date'] = df_pivot['valuation_date'].astype(str).apply(
                    lambda x: gt.strdate_transfer(x))

            except Exception as e:
                self.logger.error(f"stock_data数据转置处理失败: {str(e)}")
                df_pivot = pd.DataFrame()
            if not df_pivot.empty:
                # 竖向拼接df和df_pivot
                df = pd.concat([df, df_pivot], axis=0, ignore_index=True)

                # 处理重复的valuation_date，保留最后一个
                df = df.drop_duplicates(subset=['valuation_date'], keep='last')

                # 按valuation_date排序
                df = df.sort_values('valuation_date')
                df.to_csv(inputpath_file, index=False)
            else:
                self.logger.info(f"stock_data没有找到{name}数据在" + str(self.start_date) + "至" + str(self.end_date))

    def indexOtherData_update(self):
        inputpath = os.path.join(self.output_path, 'index_data')
        inputpath2=os.path.join(self.output_path,'mkt_data')
        gt.folder_creator2(inputpath)
        gt.folder_creator2(inputpath2)
        for type in ['FutureDifference','rrIndexScore','eg','LargeOrderInflow','NetLeverageBuying']:
            if type in ['FutureDifference','rrIndexScore']:
                 name = 'Index' + str(type)
                 inputpath_file=os.path.join(inputpath,str(name)+'.csv')
            elif type=='eg':
                name='IndexygData'
                inputpath_file = os.path.join(inputpath, str(name) + '.csv')
            else:
                name=type
                inputpath_file=os.path.join(inputpath2,str(name)+'.csv')
            if os.path.exists(inputpath_file):
                df = pd.read_csv(inputpath_file)
                start_date = gt.strdate_transfer(self.start_date)
                end_date = gt.strdate_transfer(self.end_date)
                sql = f"SELECT * FROM data_indexother WHERE valuation_date BETWEEN '{start_date}' AND '{end_date}' AND type = '{type}'"
            else:
                df = pd.DataFrame()
                sql = f"SELECT * FROM data_indexother WHERE type = '{type}'"
            df_add = self.execute_sql_to_df(sql)
            df_add=self.df_transformer(df_add,'indexOther')
            df_add['valuation_date'] = df_add['valuation_date'].astype(str).apply(
                lambda x: gt.strdate_transfer(x))
            if df_add.empty:
                self.logger.info(f"{type}没有找到{name}数据")
                continue
            if not df_add.empty:
                # 竖向拼接df和df_add
                df = pd.concat([df, df_add], axis=0, ignore_index=True)

                # 处理重复的valuation_date，保留最后一个
                df = df.drop_duplicates(subset=['valuation_date'], keep='last')

                # 按valuation_date排序
                df = df.sort_values('valuation_date')
                df.to_csv(inputpath_file, index=False)
            else:
                self.logger.info(f"{type}没有找到{name}数据在" + str(self.start_date) + "至" + str(self.end_date))

    def indexFactorData_update(self):
        inputpath = os.path.join(self.output_path, 'index_factordata')
        gt.folder_creator2(inputpath)
        for type in ['sz50', 'hs300', 'zz500', 'zz1000', 'zzA500', 'zz2000', 'gz2000']:
            name = str(type) + '因子风险暴露'
            inputpath_file = os.path.join(inputpath, name + '.csv')
            if os.path.exists(inputpath_file):
                df = gt.readcsv(inputpath_file)
                start_date = gt.strdate_transfer(self.start_date)
                end_date = gt.strdate_transfer(self.end_date)
                sql = f"SELECT * FROM data_factorindexexposure WHERE valuation_date BETWEEN '{start_date}' AND '{end_date}' AND organization = '{type}' "
            else:
                df = pd.DataFrame()
                sql = f"SELECT * FROM data_factorindexexposure WHERE organization = '{type}'"
            df_add = self.execute_sql_to_df(sql)
            df_add['valuation_date'] = df_add['valuation_date'].astype(str).apply(
                lambda x: gt.strdate_transfer(x))
            df_add.drop(columns=['organization'], inplace=True)
            if df_add.empty:
                self.logger.info(f"{type}因子暴露没有找到{name}数据")
                continue
            if not df_add.empty:
                # 竖向拼接df和df_add
                df = pd.concat([df, df_add], axis=0, ignore_index=True)

                # 处理重复的valuation_date，保留最后一个
                df = df.drop_duplicates(subset=['valuation_date'], keep='last')

                # 按valuation_date排序
                df = df.sort_values('valuation_date')
                df.to_csv(inputpath_file,encoding='gbk', index=False)
            else:
                self.logger.info(
                    f"{type}因子暴露没有找到{name}数据在" + str(self.start_date) + "至" + str(self.end_date))
    def MacroData_update(self):
        inputpath = os.path.join(self.output_path, 'macro_data')
        gt.folder_creator2(inputpath)
        for type in ['ChinaDevelopmentBankBonds','ChinaGovernmentBonds','ChinaMediumTermNotes','CPI','PPI','PMI','M1M2','Shibor','SocialFinance']:
            type2 = type.lower()
            name=type
            inputpath_file = os.path.join(inputpath, type + '.csv')
            if os.path.exists(inputpath_file):
                df = pd.read_csv(inputpath_file)
                start_date = gt.strdate_transfer(self.start_date)
                end_date = gt.strdate_transfer(self.end_date)
                sql = f"SELECT * FROM data_macro WHERE valuation_date BETWEEN '{start_date}' AND '{end_date}' AND organization = '{type}' AND type = '{'close'}' "
            else:
                df = pd.DataFrame()
                sql = f"SELECT * FROM data_macro WHERE organization = '{type}' AND type = '{'close'}' "
            df_add = self.execute_sql_to_df(sql)
            df_add = self.df_transformer(df_add, 'macroData')
            df_add['valuation_date'] = df_add['valuation_date'].astype(str).apply(
                lambda x: gt.strdate_transfer(x))
            if df_add.empty:
                self.logger.info(f"{type}_data没有找到{name}数据")
                continue
            if not df_add.empty:
                # 竖向拼接df和df_add
                df = pd.concat([df, df_add], axis=0, ignore_index=True)

                # 处理重复的valuation_date，保留最后一个
                df = df.drop_duplicates(subset=['valuation_date'], keep='last')

                # 按valuation_date排序
                df = df.sort_values('valuation_date')
                df.to_csv(inputpath_file,encoding='gbk', index=False)
            else:
                self.logger.info(
                    f"{type}_data没有找到{name}数据在" + str(self.start_date) + "至" + str(self.end_date))
    def MktOther_update(self):
        inputpath = os.path.join(self.output_path, 'mkt_data')
        gt.folder_creator2(inputpath)
        for type in ['LHB_AMT_proportion']:
            type2 = type.lower()
            if type=='LHB_AMT_proportion':
                type2='data_lhb'
            name=type
            inputpath_file = os.path.join(inputpath, type + '.csv')
            if os.path.exists(inputpath_file):
                df = pd.read_csv(inputpath_file)
                start_date = gt.strdate_transfer(self.start_date)
                end_date = gt.strdate_transfer(self.end_date)
                sql = f"SELECT * FROM {type2} WHERE valuation_date BETWEEN '{start_date}' AND '{end_date}'"
            else:
                df = pd.DataFrame()
                sql = f"SELECT * FROM {type2}"
            df_add = self.execute_sql_to_df(sql)
            df_add['valuation_date'] = df_add['valuation_date'].astype(str).apply(
                lambda x: gt.strdate_transfer(x))
            if df_add.empty:
                self.logger.info(f"{type}_data没有找到{name}数据")
                continue
            if not df_add.empty:
                # 竖向拼接df和df_add
                df = pd.concat([df, df_add], axis=0, ignore_index=True)

                # 处理重复的valuation_date，保留最后一个
                df = df.drop_duplicates(subset=['valuation_date'], keep='last')

                # 按valuation_date排序
                df = df.sort_values('valuation_date')
                df.to_csv(inputpath_file,encoding='gbk', index=False)
            else:
                self.logger.info(
                    f"{type}_data没有找到{name}数据在" + str(self.start_date) + "至" + str(self.end_date))

    def USData_update(self):
        inputpath = os.path.join(self.output_path, 'us_data')
        gt.folder_creator2(inputpath)
        for type in ['USDollar','USIndex']:
            name = type
            inputpath_file = os.path.join(inputpath, type + '.csv')
            if os.path.exists(inputpath_file):
                df = pd.read_csv(inputpath_file)
                start_date = gt.strdate_transfer(self.start_date)
                end_date = gt.strdate_transfer(self.end_date)
                sql = f"SELECT * FROM data_us WHERE valuation_date BETWEEN '{start_date}' AND '{end_date}' AND type = '{'CLOSE'}' AND organization = '{type}'"
            else:
                df = pd.DataFrame()
                sql = f"SELECT * FROM data_us WHERE type = '{'CLOSE'}' AND organization = '{type}'"
            df_add = self.execute_sql_to_df(sql)
            df_add=self.df_transformer(df_add,'macroData')
            df_add['valuation_date'] = df_add['valuation_date'].astype(str).apply(
                lambda x: gt.strdate_transfer(x))
            if df_add.empty:
                self.logger.info(f"{type}_data没有找到{name}数据")
                continue
            if not df_add.empty:
                # 竖向拼接df和df_add
                df = pd.concat([df, df_add], axis=0, ignore_index=True)

                # 处理重复的valuation_date，保留最后一个
                df = df.drop_duplicates(subset=['valuation_date'], keep='last')

                # 按valuation_date排序
                df = df.sort_values('valuation_date')
                df.to_csv(inputpath_file, encoding='gbk', index=False)
            else:
                self.logger.info(
                    f"{type}_data没有找到{name}数据在" + str(self.start_date) + "至" + str(self.end_date))
    def VIXData_update(self):
        inputpath = os.path.join(self.output_path, 'vix_data')
        gt.folder_creator2(inputpath)
        for type in ['TimeWeighted','VolumeWeighted']:
            name=type
            inputpath_file = os.path.join(inputpath, 'vix_'+name + '.csv')
            if os.path.exists(inputpath_file):
                df = pd.read_csv(inputpath_file)
                start_date = gt.strdate_transfer(self.start_date)
                end_date = gt.strdate_transfer(self.end_date)
                sql = f"SELECT valuation_date,organization,ch_vix as value FROM data_vix WHERE valuation_date BETWEEN '{start_date}' AND '{end_date}' AND vix_type = '{type}'"
            else:
                df = pd.DataFrame()
                sql = f"SELECT valuation_date,organization,ch_vix as value FROM data_vix WHERE vix_type = '{type}'"
            df_add = self.execute_sql_to_df(sql)
            if df_add.empty:
                self.logger.info(f"{type}_vixdata没有找到{name}数据")
                continue
            try:
                # 确保列名正确
                df_add.columns = ['valuation_date', 'code', 'value']

                # 处理NULL值
                df_add['value'] = pd.to_numeric(df_add['value'], errors='coerce')

                # 按valuation_date和code分组，将value列的值作为新的列
                df_pivot = df_add.pivot(index='valuation_date', columns='code', values='value')

                # 重置索引，使valuation_date成为列
                df_pivot = df_pivot.reset_index()

                # 重命名列
                df_pivot.columns.name = None  # 移除columns的name
                df_pivot['valuation_date'] = df_pivot['valuation_date'].astype(str).apply(
                    lambda x: gt.strdate_transfer(x))

            except Exception as e:
                self.logger.error(f"vix_data数据转置处理失败: {str(e)}")
                df_pivot = pd.DataFrame()
            if not df_pivot.empty:
                # 竖向拼接df和df_add
                df = pd.concat([df, df_pivot], axis=0, ignore_index=True)

                # 处理重复的valuation_date，保留最后一个
                df = df.drop_duplicates(subset=['valuation_date'], keep='last')

                # 按valuation_date排序
                df = df.sort_values('valuation_date')
                df.to_csv(inputpath_file, encoding='gbk', index=False)
            else:
                self.logger.info(
                    f"{type}_vixdata没有找到{name}数据在" + str(self.start_date) + "至" + str(self.end_date))
    def FactorData_update(self):
        inputpath = os.path.join(self.output_path, 'factor_data')
        gt.folder_creator2(inputpath)
        for type in ['data_factorreturn']:
            type2 = type.lower()
            name='lnmodel'
            inputpath_file = os.path.join(inputpath, type + '.csv')
            if os.path.exists(inputpath_file):
                df = gt.readcsv(inputpath_file)
                start_date = gt.strdate_transfer(self.start_date)
                end_date = gt.strdate_transfer(self.end_date)
                sql = f"SELECT * FROM {type2} WHERE valuation_date BETWEEN '{start_date}' AND '{end_date}'"
            else:
                df = pd.DataFrame()
                sql = f"SELECT * FROM {type2}"
            df_add = self.execute_sql_to_df(sql)
            df_add['valuation_date'] = df_add['valuation_date'].astype(str).apply(
                lambda x: gt.strdate_transfer(x))
            if df_add.empty:
                self.logger.info(f"{type}_data没有找到{name}数据")
                continue
            if not df_add.empty:
                # 竖向拼接df和df_add
                df = pd.concat([df, df_add], axis=0, ignore_index=True)

                # 处理重复的valuation_date，保留最后一个
                df = df.drop_duplicates(subset=['valuation_date'], keep='last')

                # 按valuation_date排序
                df = df.sort_values('valuation_date')
                df.to_csv(inputpath_file,encoding='gbk', index=False)
            else:
                self.logger.info(
                    f"{type}_data没有找到{name}数据在" + str(self.start_date) + "至" + str(self.end_date))
    def Mktdata_update_main(self):
        self.indexMktData_update()
        self.stockMktData_update()
        self.VIXData_update()
    def Factordata_update_main(self):
        self.indexFactorData_update()
        self.FactorData_update()
        self.indexOtherData_update()
    def macrodata_update_main(self):
        #self.MktOther_update()
        #self.MacroData_update()
        self.USData_update()

if __name__ == "__main__":
    ts=timeSeries_data_update('2025-05-01','2025-05-09')
    ts.macrodata_update_main()
    