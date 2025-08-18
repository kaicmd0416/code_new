from vix.vix_data_prepare import option_dataprepared
import pandas as pd
import numpy as np
from tqdm import *
import warnings
import sys
import os
path = os.getenv('GLOBAL_TOOLSFUNC_NEW')
sys.path.append(path)
import global_tools as gt
import global_setting.global_dic as glv
import os
import datetime
from setup_logger.logger_setup import setup_logger
import io
import contextlib

warnings.filterwarnings('ignore')
pd.set_option('display.max_rows',None)

#VIX指标计算部分
class CH_VIX(object):
    def __init__(self,target_date,index_type,realtime):
        self.logger = setup_logger('Vix_update')
        self.logger.info(f'\nProcessing VIX calculation for {target_date}, index type: {index_type}')
        odp=option_dataprepared(target_date,index_type,realtime)
        self.OptData = odp.crosssection_option_withdraw()

    def select_volume(self,cur_df: pd.DataFrame): #将主力和次主力合约提取（因考虑到两者中如果又一个到期时间小于5天的情况，故提取成交量第三名的合约备用）
        volume=cur_df['Volume'].unique()
        cczl,czl,zl=np.sort(volume)[-3:]
        cur_df=cur_df[cur_df['Volume'].isin([zl,czl,cczl])]
        return cur_df,cczl,czl,zl
    def filter_contract_Time(self, cur_df: pd.DataFrame,zl:float,czl:float,cczl:float): #将主力合约与次主力合约按照时间排列并进行提取和处理
        # 今天在交易的合约的到期日
        ex_t = cur_df['maturity'].unique()
        # 选择到期日大于等于5天的数据
        ex_t = ex_t[ex_t >= 5 / 365]
        # 到期日排序，最小两个为近月、
        if len(ex_t)>2:
            cur_df = cur_df[cur_df['Volume'].isin([zl, czl])]
            ex_t_new=cur_df['maturity'].unique()
            jy_dt, cjy_dt = np.sort(ex_t_new)[:2]
        else:
            jy_dt, cjy_dt = np.sort(ex_t)[:2]
        maturity_dict = dict(zip(['jy', 'cjy'], [jy_dt, cjy_dt]))
        # 选取近月及次近月合约
        cur_df = cur_df[cur_df['maturity'].isin([jy_dt, cjy_dt])]
        keep_cols = ['Close', 'contract_type', 'exercise_price','r']
        cur_df_jy = cur_df.query('maturity==@jy_dt')[keep_cols]
        cur_df_cjy = cur_df.query('maturity==@cjy_dt')[keep_cols]
        # TODO:df 中可能存在缺少call,put的情况需要加过滤check一下
        r_jy = cur_df_jy['r'].iloc[-1]
        r_cjy = cur_df_cjy['r'].iloc[-1]
        cur_df_jy = cur_df_jy.pivot_table(
            index='exercise_price', columns='contract_type', values='Close')

        cur_df_cjy = cur_df_cjy.pivot_table(
            index='exercise_price', columns='contract_type', values='Close')
        # 检查字段
        cur_df_jy = self._check_fields(cur_df_jy)
        cur_df_cjy = self._check_fields(cur_df_cjy)

        # 绝对值差异
        cur_df_jy['diff'] = np.abs(cur_df_jy['Call'] - cur_df_jy['Put'])
        cur_df_cjy['diff'] = np.abs(cur_df_cjy['Call'] - cur_df_cjy['Put'])
        return maturity_dict, cur_df_jy, cur_df_cjy,r_jy,r_cjy,jy_dt,cjy_dt
    def filter_contract_Volume(self, cur_df: pd.DataFrame):
        # 今天在交易的成交量
        ex_v = cur_df['Volume'].unique()
        # 到期日排序，最小两个为近月、
        czl_dt, zl_dt = np.sort(ex_v)[-2:]
        # 选取近月及次近月合约
        cur_df = cur_df[cur_df['Volume'].isin([czl_dt, zl_dt])]

        keep_cols = ['Close', 'contract_type', 'exercise_price','r','Volume','maturity']

        cur_df_czl = cur_df.query('Volume==@czl_dt')[keep_cols]
        cur_df_zl = cur_df.query('Volume==@zl_dt')[keep_cols]
        # TODO:df 中可能存在缺少call,put的情况需要加过滤check一下
        r_czl = cur_df_czl['r'].iloc[-1]
        r_zl = cur_df_zl['r'].iloc[-1]
        v_czl = cur_df_czl['Volume'].iloc[-1]
        v_zl = cur_df_zl['Volume'].iloc[-1]
        maturity_czl=cur_df_czl['maturity'].iloc[-1]
        maturity_zl=cur_df_zl['maturity'].iloc[-1]
        maturity_dict = dict(zip(['czl', 'zl'], [maturity_czl, maturity_zl]))
        cur_df_czl = cur_df_czl.pivot_table(
            index='exercise_price', columns='contract_type', values='Close')

        cur_df_zl = cur_df_zl.pivot_table(
            index='exercise_price', columns='contract_type', values='Close')
        # 检查字段
        cur_df_jy = self._check_fields(cur_df_czl)
        cur_df_cjy = self._check_fields(cur_df_zl)
        # 绝对值差异
        cur_df_jy['diff'] = np.abs(cur_df_jy['Call'] - cur_df_jy['Put'])
        cur_df_cjy['diff'] = np.abs(cur_df_cjy['Call'] - cur_df_cjy['Put'])
        return maturity_dict, cur_df_zl, cur_df_czl,r_czl,r_zl,v_czl,v_zl
    # 计算中间价格
    def cal_mid_price(self, maturity: dict, df: pd.DataFrame,
                      forward_price: float) -> pd.DataFrame:

        def _cal_mid_fun(x, val: float):
            res = None
            if x['exercise_price'] < val:
                res = x['Put']
            elif x['exercise_price'] > val:
                res = x['Call']
            else:
                res = (x['Put'] + x['Call']) / 2
            return res

        # 小于远期价格且最靠近的合约的行权价
        m_k = self._nearest_k(df, forward_price)

        ret = pd.DataFrame(index=df.index)

        # 计算中间件
        m_p_lst = df.reset_index().apply(lambda x: _cal_mid_fun(x, val=m_k), axis=1)

        ret['mid_p'] = m_p_lst.values

        return ret


    def cal_moments_sub(self, df: pd.DataFrame, maturity: float, rf_rate: float, forward_price: float,
                        nearest_k: float) -> float:

        e1, e2, e3 = self.cal_epsilon(forward_price, nearest_k)

        temp_p1 = -np.sum(
            df['mid_p'] * df['diff_k'] / np.square(df['exercise_price']))

        p1 = np.exp(maturity * rf_rate) * (temp_p1) + e1

        temp_p2 = np.sum(df['mid_p'] * df['diff_k'] * 2 *
                         (1 - np.log(df['exercise_price'] / forward_price)) /
                         np.square(df['exercise_price']))
        p2 = np.exp(maturity * rf_rate) * (temp_p2) + e2

        temp_p3 = temp_p3 = np.sum(
            df['mid_p'] * df['diff_k'] * 3 *
            (2 * np.log(df['exercise_price'] / forward_price) -
             np.square(np.log(df['exercise_price'] / forward_price))) /
            np.square(df['exercise_price']))

        p3 = np.exp(maturity * rf_rate) * (temp_p3) + e3

        s = (p3 - 3 * p1 * p2 + 2 * p1 ** 3) / (p2 - p1 ** 2) ** (3 / 2)

        return s

    # 计算近、次近月VIX
    def cal_vix_time(self, df_jy: pd.DataFrame, forward_price_jy: float, rf_rate_jy: float,
                maturity_jy: float, nearest_k_jy: float, df_cjy: pd.DataFrame,
                forward_price_cjy: float, rf_rate_cjy: float, maturity_cjy: float,
                nearest_k_cjy: float,timegap:float):

        sigma_jy = self.cal_vix_sub(df_jy, forward_price_jy, rf_rate_jy, maturity_jy,
                                    nearest_k_jy)
        sigma_cjy = self.cal_vix_sub(df_cjy, forward_price_cjy, rf_rate_cjy,
                                     maturity_cjy, nearest_k_cjy)
        w = (maturity_cjy - timegap) / (maturity_cjy - maturity_jy)
        to_sqrt = maturity_jy * sigma_jy * w + maturity_cjy * sigma_cjy * (1 - w)
        if to_sqrt >= 0:
            vix = 100 * np.sqrt(to_sqrt * (1/timegap))
        else:
            vix = np.nan

        return vix
    def cal_vix_volume(self, df_jy: pd.DataFrame, forward_price_jy: float, rf_rate_jy: float,
                maturity_jy: float, nearest_k_jy: float, df_cjy: pd.DataFrame,
                forward_price_cjy: float, rf_rate_cjy: float, maturity_cjy: float,
                nearest_k_cjy: float,volume_zl:float,volume_czl:float,volume_T:float):
        sigma_jy = self.cal_vix_sub(df_jy, forward_price_jy, rf_rate_jy, maturity_jy,
                                    nearest_k_jy)
        sigma_cjy = self.cal_vix_sub(df_cjy, forward_price_cjy, rf_rate_cjy,
                                     maturity_cjy, nearest_k_cjy)
        w=volume_zl/volume_T
        to_sqrt = sigma_jy * w + sigma_cjy * (1 - w)
        if to_sqrt >= 0:
            vix = 100 * np.sqrt(to_sqrt * ((volume_zl+volume_czl)/volume_T))
        else:

            vix = np.nan
        return vix

    # 计算VIX
    @staticmethod
    def cal_vix_sub(df: pd.DataFrame, forward_price: float, rf_rate: float,
                        maturity: float, nearest_k: float):

            def _vix_sub_fun(x):
                ret = x['diff_k'] * np.exp(rf_rate * maturity) * x['mid_p'] / np.square(
                    x['exercise_price'])
                return ret

            temp_var = df.apply(lambda x: _vix_sub_fun(x), axis=1)

            sigma = 2 * temp_var.sum() / maturity - np.square(forward_price /
                                                              nearest_k - 1) / maturity

            return sigma
    @staticmethod
    def _check_fields(x_df: pd.DataFrame):

        # 目标字段
        target_fields = ['Call', 'Put']

        for col in target_fields:
            if col not in x_df.columns:
                print("%s字段为空" % col)
                x_df[col] = 0
        return x_df

    @staticmethod
    def cal_forward_price(maturity: float, rf_rate: float,
                          df: pd.DataFrame) -> float:

        # 获取认购与认沽的绝对值差异最小值的信息
        min_con = df.sort_values('diff').iloc[0]

        # 获取的最小exercise_price
        k_min = min_con.name

        # F = Strike Price + e^RT x (Call Price - Put Price)
        f_price = k_min + np.exp(maturity * rf_rate) * (
                min_con['Call'] - min_con['Put'])

        return f_price

    # 寻找最近合约
    @staticmethod
    def _nearest_k(df: pd.DataFrame, forward_price: float) -> float:
        # 行权价等于或小于远期价格的合约
        temp_df = df[df.index <= forward_price]
        if temp_df.empty:
            temp_df = df

        m_k = temp_df.sort_values('diff').index[0]

        return m_k
    # 计算行权价间隔
    @staticmethod
    def cal_k_diff(df: pd.DataFrame) -> pd.DataFrame:

        arr_k = df.index.values
        ret = pd.DataFrame(index=df.index)
        res = []
        res.append(arr_k[1] - arr_k[0])
        res.extend(0.5 * (arr_k[2:] - arr_k[0:-2]))  # 这里用的delta——k 是 用 i+1 天减去i-1天的
        res.append(arr_k[-1] - arr_k[-2])
        ret['diff_k'] = res
        return ret

    @staticmethod
    def cal_epsilon(forward_price: float, nearest_k: float) -> tuple:

        e1 = -(1 + np.log(forward_price / nearest_k) - forward_price / nearest_k)

        e2 = 2 * np.log(nearest_k / forward_price) * (
                nearest_k / forward_price - 1) + np.square(
            np.log(nearest_k / forward_price)) * 0.5

        e3 = 3 * np.square(np.log(nearest_k / forward_price)) * (
                np.log(nearest_k / forward_price) / 3 - 1 + forward_price / nearest_k)

        return e1, e2, e3

    def GetCBOE_VIX(self,method):
        self.logger.info(f'Calculating VIX using {method} method...')
        error_list = []
        error_type=[]
        error=pd.DataFrame()
        date_index = []  # 储存index
        vix_value = []  # 储存vix
        if self.OptData.empty:
            self.logger.warning('No option data available')
            data=pd.DataFrame()
            error=pd.DataFrame()
            return data,error
        for trade_date, slice_df in tqdm(self.OptData.groupby('Date'), desc='计算中..', leave=True):
            try:
                if method=='TimeWeighted':
                    slice_df, cczl, czl, zl = self.select_volume(slice_df)
                    maturity, df_jy, df_cjy, r_jy, r_cjy, jy_dt, cjy_dt = self.filter_contract_Time(slice_df, zl, czl, cczl)
                    # 计算天数：
                    timegap = (jy_dt + (cjy_dt - jy_dt) / 2)
                    # 计算无风险利率
                    rf_rate_jy = r_jy
                    rf_rate_cjy = r_cjy
                    # 计算远期价格
                    fp_jy = self.cal_forward_price(maturity['jy'], rf_rate=rf_rate_jy, df=df_jy)
                    fp_cjy = self.cal_forward_price(maturity['cjy'], rf_rate=rf_rate_cjy, df=df_cjy)
                    # 计算中间价格
                    df_mp_jy = self.cal_mid_price(maturity['jy'], df_jy, fp_jy)
                    df_mp_cjy = self.cal_mid_price(maturity['cjy'], df_cjy, fp_cjy)
                    # 计算行权价差
                    df_diff_k_jy = self.cal_k_diff(df_jy)
                    df_diff_k_cjy = self.cal_k_diff(df_cjy)
                    # 计算VIX
                    df_tovix_jy = pd.concat([df_mp_jy, df_diff_k_jy], axis=1).reset_index()
                    df_tovix_cjy = pd.concat([df_mp_cjy, df_diff_k_cjy], axis=1).reset_index()

                    nearest_k_jy = self._nearest_k(df_jy, fp_jy)
                    nearest_k_cjy = self._nearest_k(df_cjy, fp_cjy)

                    vix = self.cal_vix_time(df_tovix_jy, fp_jy, rf_rate_jy, maturity['jy'],
                                       nearest_k_jy, df_tovix_cjy, fp_cjy, rf_rate_cjy,
                                       maturity['cjy'], nearest_k_cjy, timegap)
                    date_index.append(trade_date)
                    vix_value.append(vix)
                else:
                    volume_T = slice_df['Volume_Shares'].sum()
                    maturity, df_zl, df_czl, r_czl, r_zl, v_czl, v_zl = self.filter_contract_Volume(slice_df)
                    # 计算主力与次主力成交量
                    volume_zl = v_zl
                    volume_czl = v_czl
                    # 计算无风险利率
                    rf_rate_zl = r_zl
                    rf_rate_czl = r_czl
                    # 计算远期价格
                    fp_zl = self.cal_forward_price(maturity['zl'], rf_rate=rf_rate_zl, df=df_zl)
                    fp_czl = self.cal_forward_price(maturity['czl'], rf_rate=rf_rate_czl, df=df_czl)
                    # 计算中间价格
                    df_mp_zl = self.cal_mid_price(maturity['zl'], df_zl, fp_zl)
                    df_mp_czl = self.cal_mid_price(maturity['czl'], df_czl, fp_czl)
                    # 计算行权价差
                    df_diff_k_zl = self.cal_k_diff(df_zl)
                    df_diff_k_czl = self.cal_k_diff(df_czl)
                    # 计算VIX
                    df_tovix_zl = pd.concat([df_mp_zl, df_diff_k_zl], axis=1).reset_index()
                    df_tovix_czl = pd.concat([df_mp_czl, df_diff_k_czl], axis=1).reset_index()
                    nearest_k_zl = self._nearest_k(df_zl, fp_zl)
                    nearest_k_czl = self._nearest_k(df_czl, fp_czl)
                    vix = self.cal_vix_volume(df_tovix_zl, fp_zl, rf_rate_zl, maturity['zl'],
                                       nearest_k_zl, df_tovix_czl, fp_czl, rf_rate_czl,
                                       maturity['czl'], nearest_k_czl, volume_zl, volume_czl, volume_T)
                    date_index.append(trade_date)
                    vix_value.append(vix)
            except (IndexError , ValueError):
                self.logger.warning(f'Error processing date {trade_date}: Call Put data missing')
                error_list.append(trade_date)
                error_type.append('Call Put 都缺失')
                continue
        data = pd.DataFrame({
            "ch_vix": vix_value},
            index=date_index)
        data.fillna(method='pad', inplace=True)
        data.index = pd.DatetimeIndex(data.index)
        data['vix_type']=method
        error['error_date'] = error_list
        error['type'] = error_type
        self.logger.info(f'Successfully calculated VIX for {len(date_index)} dates')
        return data, error

def capture_file_withdraw_output(func, *args, **kwargs):
    """捕获file_withdraw的输出并记录到日志"""
    logger = setup_logger('Vix_update_sql')
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        result = func(*args, **kwargs)
        output = buf.getvalue()
        if output.strip():
            logger.info(output.strip())
    return result

def VIX_calculation_main(start_date,end_date,realtime,is_sql): #将前面计算部分进行整合运用
    logger = setup_logger('Vix_update')
    logger.info('\n' + '*'*50 + '\nVIX CALCULATION PROCESSING\n' + '*'*50)
    
    if realtime==False:
        logger.info('Processing historical VIX calculation...')
        outputpath = glv.get('output_vix')
        gt.folder_creator2(outputpath)
        outputlist = os.listdir(outputpath)
        if len(outputlist) == 0:
            start_date = '2024-01-02'
            logger.info('No existing files found, setting start date to 2024-01-02')
        working_days_list = gt.working_days_list(start_date, end_date)
        if is_sql == True:
            inputpath_configsql = glv.get('config_sql')
            sm=gt.sqlSaving_main(inputpath_configsql,'VIX')
        for target_date in working_days_list:
            logger.info(f'Processing date: {target_date}')
            target_date2 = gt.intdate_transfer(target_date)
            outputpath_daily = os.path.join(outputpath, 'vix_' + str(target_date2) + '.csv')
            df_final = pd.DataFrame()
            df_error = pd.DataFrame()
            for index_type in ['sz50', 'hs300', 'zz1000']:
                logger.info(f'Processing index type: {index_type}')
                VIX = CH_VIX(target_date, index_type, realtime)
                for method in ['TimeWeighted', 'VolumeWeighted']:
                    a, b = VIX.GetCBOE_VIX(method=method)
                    a['organization'] = index_type
                    df_final = pd.concat([df_final, a])
                    df_error = pd.concat([df_error, b])
            if len(df_error) > 0:
                logger.warning(f'Errors found for date {target_date}:')
                logger.warning(str(df_error))
            if len(df_final) > 0:
                df_final.reset_index(inplace=True)
                df_final.rename(columns={'index': 'valuation_date'}, inplace=True)
                try:
                    df_final['valuation_date']=df_final['valuation_date'].apply(lambda x: gt.strdate_transfer(x))
                except:
                    pass
                df_final.to_csv(outputpath_daily, index=False)
                logger.info(f'Successfully saved VIX data for date: {target_date2}')
                if is_sql == True:
                    capture_file_withdraw_output(sm.df_to_sql, df_final)
            else:
                logger.warning(f'VIX calculation failed for date: {target_date}')
        logger.info('Completed historical VIX calculation')
    else:
        logger.info('Processing realtime VIX calculation...')
        outputpath=glv.get('output_vix_realtime')
        gt.folder_creator2(outputpath)
        today=datetime.date.today()
        today=gt.strdate_transfer(today)
        today2=gt.intdate_transfer(today)
        outputpath=os.path.join(outputpath,'vix_realtime_'+str(today2)+'.csv')
        df_final = pd.DataFrame()
        for index_type in ['sz50', 'hs300', 'zz1000']:
            logger.info(f'Processing index type: {index_type}')
            VIX = CH_VIX(today2, index_type, realtime)
            a, b = VIX.GetCBOE_VIX(method='Time weighted')
            df_final = pd.concat([df_final, a])
        if len(df_final) > 0:
            df_final.reset_index(inplace=True)
            df_final.rename(columns={'index': 'valuation_date'}, inplace=True)
            df_final.to_csv(outputpath, index=False)
            logger.info(f'Successfully saved realtime VIX data for date: {today2}')
        else:
            logger.warning('VIX realtime calculation failed')
        logger.info('Completed realtime VIX calculation')




if __name__ == '__main__':
       VIX_calculation_main('2025-07-08','2025-07-08',False,True)


