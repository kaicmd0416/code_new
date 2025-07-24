import pandas as pd
import Portfolio_tracking.global_setting.global_dic as glv
from utils.db_processor import DatabaseProcessor
from datetime import datetime, time
import pytz
import logging
from sqlalchemy import text
from global_tools_func.global_tools import is_workday2
class realtimeStock_sqlSaving:
    def __init__(self,df1,df2):
        self.df1=df1
        self.df2=df2
        self.db_processor = DatabaseProcessor()
        self._setup_logging()

    def _setup_logging(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    def product_return_processing(self):
        df_product=self.df2.copy()
        inputpath=glv.get('config_product')
        df=pd.read_excel(inputpath,sheet_name='product_detail')
        df=df[['product_name','product_code']]
        df_product=df_product.merge(df,on='product_name',how='left')
        # 将列名从 portfolio_return(bp) 和 excess_return(bp) 修改为 portfolio_return_bp 和 excess_return_bp
        if 'portfolio_return(bp)' in df_product.columns:
            df_product.rename(columns={'portfolio_return(bp)': 'portfolio_return_bp'}, inplace=True)
        if 'excess_return(bp)' in df_product.columns:
            df_product.rename(columns={'excess_return(bp)': 'excess_return_bp'}, inplace=True)

        #在这里接入存sql的代码，然后primary key 为 product_code
        #其中portfolio_return(bp) excess_return(bp)为float
        #注意这里面入库需要额外加一列update_time，代表入库时间
        #应该创建一个tracking的库，这个对应的表名为 realtime_productStockReturn
        return df_product
    def portfolio_return_processing(self):
        df_portfolio=self.df1.copy()
        # 将列名从 portfolio_return(bp) 和 excess_return(bp) 修改为 portfolio_return_bp 和 excess_return_bp
        if 'portfolio_return(bp)' in df_portfolio.columns:
            df_portfolio.rename(columns={'portfolio_return(bp)': 'portfolio_return_bp'}, inplace=True)
        if 'excess_return(bp)' in df_portfolio.columns:
            df_portfolio.rename(columns={'excess_return(bp)': 'excess_return_bp'}, inplace=True)

        #在这里接入存sql的代码，然后primary key 为 score_name
        #其中portfolio_return(bp) excess_return(bp)为float
        #注意这里面入库需要额外加一列update_time，代表入库时间
        #应该创建一个tracking的库，这个对应的表名为 realtime_portfolioReturn
        return df_portfolio

    def is_after_16(self):
        """检查当前时间是否在16:00~16:30之间"""
        shanghai_tz = pytz.timezone('Asia/Shanghai')
        current_time = datetime.now(shanghai_tz)

        # 创建带有时区信息的16:00和16:30时间
        start_time = current_time.replace(hour=16, minute=0, second=0, microsecond=0)
        end_time = current_time.replace(hour=16, minute=30, second=0, microsecond=0)

        return start_time <= current_time <= end_time

    # def check_history_record(self):
    #     """检查今天是否已经记录过历史数据"""
    #     today = datetime.now(pytz.timezone('Asia/Shanghai')).date()
    #     with self.db_processor.engine.connect() as conn:
    #         # 检查所有相关的历史表
    #         tables_to_check = ['history_productStockReturn', 'history_portfolioReturn']
    #         for table in tables_to_check:
    #             result = conn.execute(text(
    #                 f"SELECT COUNT(*) FROM {table} WHERE DATE(date) = :today"
    #             ), {"today": today})
    #             count = result.scalar()
    #             if count >0:
    #                 return True  # 如果任何一个表有今天的数据，就返回True
    #     return False  # 所有表都没有今天的数据，才返回False
    # def check_history_record(self):
    #     """检查所有指定的表是否都记录了今天的数据"""
    #     # 获取当前日期（上海时区）
    #     today = datetime.now(pytz.timezone('Asia/Shanghai')).date()
    #     self.logger.info(f"Checking history records for today: {today}")
    #
    #     # 需要检查的表名列表
    #     tables_to_check = ['history_proinfo', 'history_futureoptionholding', 'history_holdingchanging']
    #
    #     try:
    #         with self.db_processor.engine.connect() as conn:
    #             for table in tables_to_check:
    #                 # 构造 SQL 查询语句
    #                 query = text(f"SELECT COUNT(*) FROM {table} WHERE CAST(date AS DATE) = :today")
    #                 result = conn.execute(query, {"today": today})
    #                 count = result.scalar()
    #
    #                 if count == 0:
    #                     self.logger.info(f"Table '{table}' does not have records for today.")
    #                     return False  # 如果任何一个表没有今天的数据，返回 False
    #     except Exception as e:
    #         self.logger.error(f"Error checking history records: {e}")
    #         return False  # 如果发生异常，返回 False
    #
    #     self.logger.info("All tables have history records for today.")
    #     return True  # 所有表都有今天的数据，返回 True
    def save_to_history(self, data_dict):
        """将数据保存到历史表"""
        try:
            today = datetime.now(pytz.timezone('Asia/Shanghai')).date()

            history_data_dict = {}
            table_mapping = {
                'realtime_productStockReturn': 'history_productStockReturn',
                'realtime_portfolioReturn': 'history_portfolioReturn'
            }
            # print(self.check_history_record())
            # # 先检查所有表是否都没有今天的数据
            # if self.check_history_record():
            #     print("今天的历史数据已经存在，跳过保存")
            #     return

            for table_name, df in data_dict.items():
                if table_name in table_mapping:
                    history_table_name = table_mapping[table_name]
                    df_with_date = df.copy()
                    df_with_date['date'] = today
                    df_with_date['update_time'] = datetime.now().replace(tzinfo=None)
                    history_data_dict[history_table_name] = df_with_date

            # 使用事务确保所有表的数据都写入成功
            self.db_processor.write_multiple_tables(history_data_dict)

        except Exception as e:
            print(f"保存历史数据时出错: {str(e)}")
            raise e
    def realtimeStock_savingmain(self):
        product_return = self.product_return_processing()
        portfolio_return = self.portfolio_return_processing()
        data_dict = {
            'realtime_productStockReturn': product_return,
            'realtime_portfolioReturn': portfolio_return
        }
        # db_processor = DatabaseProcessor()
        self.db_processor.write_multiple_tables(data_dict)
        # 检查是否需要保存历史数据（只在交易日16点后且今天没有记录过的情况下保存）
        if is_workday2() and self.is_after_16() :
            self.save_to_history(data_dict)

