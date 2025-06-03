from Optimizer_python.Optimizer.optimizer_V5 import Optimizer_python
import sys
import os
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import global_tools as gt
import pandas as pd
import global_setting.global_dic as glv
import argparse

class Optimizer_main:
    def __init__(self,df_st, df_stock_universe):
            self.df_st=df_st
            self.df_stock_pool=df_st
            self.df_stock_universe=df_stock_universe

    #日更function组
    def optimizer_history_main(self,df_config):
        outputpath_list=[]
        outputpath_list_yes=[]
        df_config['start_date']=pd.to_datetime(df_config['start_date'])
        df_config['end_date']=pd.to_datetime(df_config['end_date'])
        start_date_min=df_config['start_date'].min()
        end_date_max=df_config['end_date'].max()
        start_date_min=gt.strdate_transfer(start_date_min)
        end_date_max=gt.strdate_transfer(end_date_max)
        target_date_list = gt.working_days_list(start_date_min, end_date_max)
        df_config['start_date']=df_config['start_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        df_config['end_date'] = df_config['end_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        for target_date in target_date_list:
              score_name_list=df_config[(df_config['start_date']<=target_date)&(df_config['end_date']>=target_date)]['portfolio_name'].tolist()
              if len(score_name_list)!=0:
                  print(target_date)
                  Optimizer_V5 = Optimizer_python(target_date, self.df_st,self.df_stock_universe)
                  for score_name in score_name_list:
                         print(score_name)
                         outputpath,outputpath_yes = Optimizer_V5.main_optimizer(score_name)
                         outputpath_list.append(outputpath)
                         outputpath_list_yes.append(outputpath_yes)
        outputpath_list.sort()
        outputpath_list_yes.sort()
        return outputpath_list,outputpath_list_yes

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Optimizer main script")
    parser.add_argument("--target_date", type=str, help="Target date for optimization")
    parser.add_argument("--score_name", type=str, help="Score name for optimization")
    args = parser.parse_args()

    target_date = args.target_date
    score_name = args.score_name

    df_st = pd.read_csv('path_to_your_data.csv')  # Replace with actual path to your data
    df_stock_universe = pd.read_csv('path_to_your_universe.csv')  # Replace with actual path to your universe data

    optimizer_main_instance = Optimizer_main(df_st, df_stock_universe)

    if target_date:
        # Call the optimizer_update_main method
        outputpath_list, outputpath_list_yes = optimizer_main_instance.optimizer_update_main(target_date, [score_name])
        print("Updated optimization results:")
        for outputpath, outputpath_yes in zip(outputpath_list, outputpath_list_yes):
            print(f"Output path: {outputpath}")
            print(f"Output path yes: {outputpath_yes}")
    else:
        # Call the optimizer_history_main method
        df_config = pd.read_csv('path_to_your_config.csv')  # Replace with actual path to your config data
        outputpath_list, outputpath_list_yes = optimizer_main_instance.optimizer_history_main(df_config)
        print("Historical optimization results:")
        for outputpath, outputpath_yes in zip(outputpath_list, outputpath_list_yes):
            print(f"Output path: {outputpath}")
            print(f"Output path yes: {outputpath_yes}")




