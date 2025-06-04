import pandas as pd
import Portfolio_tracking.global_setting.global_dic as glv
from utils.db_processor import DatabaseProcessor
from datetime import datetime, time
import pytz
import logging
from sqlalchemy import text
from global_tools_func.global_tools import is_workday2
class sql_processing:
    def __init__(self,product_name,simulation,df_proinfo,df_future,df_commodity,df_fo_difference,df_fo_difference2,df_etf_difference,df_stock_difference):
        if product_name=='惠盈一号':
            product_name='宣夜惠盈一号'
        self.product_name=product_name
        self.simulation=simulation
        self.df_proinfo=df_proinfo
        self.df_future=df_future
        self.df_commodity=df_commodity
        self.df_fo_difference=df_fo_difference
        self.df_fo_difference2=df_fo_difference2
        self.df_etf_difference=df_etf_difference
        self.df_stock_difference=df_stock_difference
        self._setup_logging()
        # 创建一个数据库处理器实例
        self.db_processor = DatabaseProcessor()

    def _setup_logging(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    def product_code_withdraw(self):
        inputpath=glv.get('config_product')
        df=pd.read_excel(inputpath,sheet_name='product_detail')
        product_code=df[df['other_name']==self.product_name]['product_code'].tolist()[0]
        return product_code
    def df_proinfo_processing(self):
        product_code=self.product_code_withdraw()
        df=self.df_proinfo.copy()
        df1=df[['info_name','money']]
        df2=df[['profit_name','profit']]
        df1.columns=['type','value']
        df2.columns=['type','value']
        df_final=pd.concat([df1,df2])
        df_final['product_code']=product_code
        df_final['simulation']=self.simulation
        #在这里接入存sql的代码，然后primary key 为 product_code,type,simulation
        #其中只有value为float，其他的都是str格式
        #注意这里面入库需要额外加一列update_time，代表入库时间

        # 在您的sql_saving.py中使用
        # db_processor = DatabaseProcessor()
        # db_processor.process_with_interval(df_final, 'realtime_proinfo')
        return df_final
    def df_futureoption_processing(self):
        product_code = self.product_code_withdraw()
        df_futureoption=self.df_future.copy()
        df_commodity=self.df_commodity.copy()
        df_futureoption=df_futureoption[['合约','买卖','总持仓','Delta','market_value','daily_profit','proportion','len']]
        df_commodity= df_commodity[
            ['合约', '买卖', '总持仓', 'Delta', 'market_value', 'daily_profit', 'proportion']]
        df_futureoption.columns=['code','direction','HoldingQty','delta','mkt_value','daily_profit','proportion','len']
        df_commodity.columns=['code','direction','HoldingQty','delta','mkt_value','daily_profit','proportion']
        df_futureoption.drop(columns='len',inplace=True)
        df_futureoption['type']='stock_futureOption'
        df_commodity['type']='commodity_futureOption'
        df_final=pd.concat([df_futureoption,df_commodity])
        df_final['product_code'] = product_code
        df_final['simulation'] = self.simulation
        def direction_decision(x):
            if x=='买':
                return 'long'
            elif x=='卖':
                return'short'
            else:
                return None
        df_final['direction']= df_final['direction'].apply(lambda x: x.strip())
        df_final['direction']=df_final['direction'].apply(lambda x: direction_decision(x))
        #在这里接入存sql的代码，然后primary key 为 product_code,simulation,code,direction
        #其中HoldingQty,delta,mkt_value,daily_profit,proportion为float其他的为str
        #注意这里面入库需要额外加一列update_time，代表入库时间
        #应该创建一个tracking的库，这个对应的表名为 realtime_futureoptionHolding

        return df_final
    def df_changing(self):
        product_code = self.product_code_withdraw()
        df_stock=self.df_stock_difference.copy()
        df_etf=self.df_etf_difference.copy()
        df_future=self.df_fo_difference.copy()
        df_future2=self.df_fo_difference2.copy()
        df_stock=df_stock[['code','HoldingQty','HoldingQty_yes','difference','action']]
        df_etf = df_etf[['code', 'HoldingQty', 'HoldingQty_yes', 'difference', 'action']]
        df_stock['direction']='long'
        df_stock['type']='stock'
        df_etf['direction']='long'
        df_etf['type']='etf'
        df_future=df_future[['合约','买卖','总持仓','昨仓','difference','action']]
        df_future2=df_future2[['合约','买卖','总持仓','昨日持仓','difference','action']]
        df_future.columns=['code','direction','HoldingQty','HoldingQty_yes','difference','action']
        df_future2.columns = ['code', 'direction', 'HoldingQty', 'HoldingQty_yes', 'difference', 'action']
        def direction_decision(x):
            if x=='买':
                return 'long'
            elif x=='卖':
                return'short'
            else:
                return None
        df_future['direction'] = df_future['direction'].apply(lambda x: x.strip())
        df_future2['direction'] = df_future2['direction'].apply(lambda x: x.strip())
        df_future['direction']=df_future['direction'].apply(lambda x: direction_decision(x))
        df_future2['direction'] = df_future2['direction'].apply(lambda x: direction_decision(x))
        df_future['type']='daily_futureOption'
        df_future2['type']='overnight_futureOption'
        df_final=pd.concat([df_stock,df_etf,df_future,df_future2])
        df_final['simulation']=self.simulation
        df_final['product_code']=product_code
        #在这里接入存sql的代码，然后primary key 为 product_code,simulation,code,direction,type
        #其中HoldingQty,HoldingQty_yes,difference为float
        #注意这里面入库需要额外加一列update_time，代表入库时间
        #应该创建一个tracking的库，这个对应的表名为 realtime_holdingChanging
        return df_final

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
    #     # product_code = self.product_code_withdraw()
    #     with self.db_processor.engine.connect() as conn:
    #         # 检查所有相关的历史表
    #         tables_to_check = ['history_proinfo', 'history_futureoptionholding','history_holdingchanging']
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
            product_code = self.product_code_withdraw()

            history_data_dict = {}
            table_mapping = {
                'realtime_proinfo': 'history_proinfo',
                'realtime_futureoptionholding': 'history_futureoptionholding',
                'realtime_holdingchanging': 'history_holdingchanging'
            }
            # if self.check_history_record():
            #     print("今天的历史数据已经存在，跳过保存")
            #     return

            for table_name, df in data_dict.items():
                if table_name in table_mapping:
                    history_table_name = table_mapping[table_name]
                    df_with_date = df.copy()
                    df_with_date['date'] = today
                    df_with_date['update_time'] = datetime.now().replace(tzinfo=None)
                    # 确保数据包含正确的product_code
                    if 'product_code' not in df_with_date.columns:
                        df_with_date['product_code'] = product_code
                    history_data_dict[history_table_name] = df_with_date
            # print(history_data_dict)
            self.db_processor.write_multiple_tables(history_data_dict)

        except Exception as e:
            print(f"保存历史数据时出错: {str(e)}")
            raise e
    def productTracking_sqlmain(self):
        df_proinfo = self.df_proinfo_processing()
        df_futureoption = self.df_futureoption_processing()
        df_changing = self.df_changing()
        trading_detail_path = glv.get('product_detail')
        df_ori = pd.read_excel(trading_detail_path, sheet_name='product_detail')
        df_product_name = df_ori[['product_name', 'product_code']]

        # 创建数据字典
        data_dict = {
            'product_detail': df_product_name,
            'realtime_proinfo': df_proinfo,
            'realtime_futureoptionholding': df_futureoption,
            'realtime_holdingchanging': df_changing
        }

        # 一次性写入所有数据
        self.db_processor.write_multiple_tables(data_dict)
        # 检查是否需要保存历史数据（只在交易日16点后且今天没有记录过的情况下保存）


        if is_workday2() and self.is_after_16() :
            self.save_to_history(data_dict)

if __name__ == "__main__":
    sq = sql_processing()
    # print(sq.check_history_record())

    pass


