import sys
import os
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt
import pandas as pd
import global_setting.global_dic as glv
from setup_logger.logger_setup import setup_logger
import io
import contextlib
from datetime import datetime
def capture_file_withdraw_output(func, *args, **kwargs):
    """捕获file_withdraw的输出并记录到日志"""
    logger = setup_logger('DataOther_sql')
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        result = func(*args, **kwargs)
        output = buf.getvalue()
        if output.strip():
            logger.info(output.strip())
    return result
class File_moving:
    def data_other_moving(self):
        input = glv.get('input_destination')
        output = glv.get('output_destination')
        gt.folder_creator2(output)
        status=0
        if len(os.listdir(output))==0:
            status=1
        gt.move_specific_files(input, output)
        if status==1:
            DOS = DataOther_sql()
            DOS.Dataother_main()
    def data_product_moving(self):
        input=glv.get('input_prod')
        gt.folder_creator2(input)
        if len(os.listdir(input))!=0:
            output = glv.get('output_prod')
            gt.move_specific_files2(input, output)
            print('product_detail已经复制完成')
    def file_moving_update_main(self):
        self.data_other_moving()
        self.data_product_moving()
class DataOther_sql:
    def __init__(self):
        self.inputpath=glv.get('output_destination')
    def valuationData_sql(self):
        inputpath=os.path.join(self.inputpath,'chinese_valuation_date.xlsx')
        df=pd.read_excel(inputpath)
        inputpath_configsql = glv.get('config_sql')
        sm = gt.sqlSaving_main(inputpath_configsql, 'ChineseValuationDate')
        capture_file_withdraw_output(sm.df_to_sql, df)
    def st_stock_sql(self):
        inputpath=os.path.join(self.inputpath,'st_stock.xlsx')
        df=pd.read_excel(inputpath)
        inputpath_configsql = glv.get('config_sql')
        sm = gt.sqlSaving_main(inputpath_configsql, 'STstock')
        capture_file_withdraw_output(sm.df_to_sql, df)
    def stockuni_sql(self):
        inputpath=os.path.join(self.inputpath,'StockUniverse_new.csv')
        df=gt.readcsv(inputpath)
        inputpath2=os.path.join(self.inputpath,'StockUniverse.csv')
        df2=gt.readcsv(inputpath2)
        df=df[['S_INFO_WINDCODE','S_INFO_LISTDATE','S_INFO_DELISTDATE']]
        df2=df2[['S_INFO_WINDCODE','S_INFO_LISTDATE','S_INFO_DELISTDATE']]
        df2['type']='stockuni_old'
        df['type']='stockuni_new'
        df_final=pd.concat([df,df2])
        inputpath_configsql = glv.get('config_sql')
        sm = gt.sqlSaving_main(inputpath_configsql, 'Stock_uni')
        capture_file_withdraw_output(sm.df_to_sql, df_final)
    def specialdate_sql(self):
        inputpath=os.path.join(self.inputpath,'month_first_6days.xlsx')
        df=pd.read_excel(inputpath)
        inputpath2=os.path.join(self.inputpath,'weeks_firstday.xlsx')
        df2=pd.read_excel(inputpath2)
        inputpath3 = os.path.join(self.inputpath, 'weeks_lastday.xlsx')
        df3 = pd.read_excel(inputpath3)
        df['type']='monthFirst6Days'
        df2['type']='weeksFirstDay'
        df3['type']='weeksLastDay'
        df_final=pd.concat([df,df2,df3])
        inputpath_configsql = glv.get('config_sql')
        sm = gt.sqlSaving_main(inputpath_configsql, 'SpecialDay')
        capture_file_withdraw_output(sm.df_to_sql, df_final)
    def Dataother_main(self):
        self.valuationData_sql()
        self.stockuni_sql()
        self.specialdate_sql()
        self.st_stock_sql()
if __name__ == '__main__':
    DOS = DataOther_sql()
    DOS.Dataother_main()



