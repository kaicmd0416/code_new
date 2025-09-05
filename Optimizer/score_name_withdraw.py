import json
import os
import pandas as pd
import sys
import yaml
import re
from datetime import date
import datetime
import global_setting.global_dic as glv
path = os.getenv('GLOBAL_TOOLSFUNC')
sys.path.append(path)
import global_tools as gt
def mode_dic_withdraw(prod=True):
   config_path = glv.get('config_path')
   inputpath=glv.get('mode_dic')
   df=gt.data_getting(inputpath,config_path)
   df['score_type'] = df['score_name'].apply(lambda x: str(x)[:4])
   if prod==True:
       score_name_list=['fm01','fm03','comb']
       df=df[df['score_type'].isin(score_name_list)]
   return df