import pandas as pd
import os
import global_setting.global_dic as glv

def optimizer_args_withdraw(portfolio_name):
    inputpath_config = glv.get('backtest_config')
    df_config = pd.read_excel(inputpath_config,sheet_name='portfolio_constraint')
    df_config=df_config[['constraint_name',portfolio_name]]
    df_config.set_index('constraint_name',inplace=True,drop=True)
    parameters=df_config.to_dict()
    parameters=parameters.get(portfolio_name)
    parameters['portfolio_name'] = portfolio_name
    return parameters
def factor_constraint_withdraw(portfolio_name):
      inputpath_config = glv.get('backtest_config')
      df_config=pd.read_excel(inputpath_config,sheet_name='factor_constraint')
      df_upper= df_config[['factor_name','is_valid',str(portfolio_name)+'_upper']]
      df_lower = df_config[['factor_name','is_valid',str(portfolio_name)+'_lower']]
      df_upper=df_upper[df_upper['is_valid']==1]
      df_lower = df_lower[df_lower['is_valid'] == 1]
      df_upper=df_upper[['factor_name',str(portfolio_name)+'_upper']]
      df_lower=df_lower[['factor_name',str(portfolio_name)+'_lower']]
      df_upper.columns=['factor_name','upper']
      df_lower.columns=['factor_name','lower']
      df_constraint=df_upper.merge(df_lower,on='factor_name',how='left')
      return df_constraint
def valid_factor_withdraw(portfolio_name):
    df_constraint=factor_constraint_withdraw(portfolio_name)
    factor_name_list=df_constraint['factor_name'].tolist()
    # Initialize empty lists
    style_list = []
    industry_list = []
    # Check each factor name
    for factor in factor_name_list:
        # Check if the factor name contains Chinese characters
        if any('\u4e00' <= char <= '\u9fff' for char in factor):
            industry_list.append(factor)
        else:
            if factor!='TE':
                 style_list.append(factor)
    return style_list, industry_list
if __name__ == '__main__':
    valid_factor_withdraw('kai_1')