from Portfolio_tracking.futures_options_position_tracking.FOL_tracking_new import portfolio_tracking
from Portfolio_tracking.futures_options_position_tracking.product_tracking_dp_new import product_data
import Portfolio_tracking.global_setting.global_dic as glv
import datetime
import global_tools_func.global_tools as gt
import os
from shutil import copyfile
def lasted_datawithdraw(inputpath,name=None):
    inputlist=os.listdir(inputpath)
    if name!=None:
        if name=='Hold' or name=='持仓':
             inputlist = [i for i in inputlist if name in i]
        else:
             inputlist=[i for i in inputlist if name in i and '_' not in i ]
    inputlist.sort()
    inputname = inputlist[-1]
    date = inputname[-12:-4]
    try:
        int(date)
    except:
        date=inputname[-13:-5]
    try:
        int(date)
    except:
        date=inputname[-10:-4]
    return date,inputname
def file_update(product_name):
    today=datetime.date.today()
    if gt.is_workday2()==True:
        today = gt.intdate_transfer(today)
        today2 = today
        if product_name == 'xy':
            today2 = str(today)[2:]
            inputpath = glv.get('future_info_xy')
        elif product_name == 'zx':
            inputpath = glv.get('future_info_zx')
        elif product_name == 'rj':
            inputpath = glv.get('future_info_rj')
        elif product_name == 'gy':
            inputpath = glv.get('future_info_gy')
        elif product_name == 'renr':
            inputpath = glv.get('future_info_renr')
        else:
            raise ValueError
        # 持仓
        inputpath_real = os.path.join(inputpath, 'future')
        today3=gt.strdate_transfer(today)
        if product_name!='xy':
            available_date = gt.last_workday_calculate(today3)
            available_date = gt.last_workday_calculate(available_date)
            available_date = gt.intdate_transfer(available_date)
            inputpath_old = gt.file_withdraw(inputpath_real, available_date)
        else:
            available_date = gt.last_workday_calculate(today3)
            available_date = gt.intdate_transfer(available_date)
            available_date=available_date[2:]
            inputpath_old=os.path.join(inputpath_real,'持仓_' + str(available_date) + '.csv')
        inputname_new = '持仓_' + str(today2) + '.csv'
        inputpath_new = os.path.join(inputpath_real, inputname_new)
        copyfile(inputpath_old, inputpath_new)
        # 模拟持仓
        inputpath_simulation = os.path.join(inputpath, 'future_simulation')
        date_simulation, inputname_old_simulation = lasted_datawithdraw(inputpath_simulation, '持仓')
        if date_simulation != today2:
            inputname_new = '持仓_' + str(today2) + '.csv'
            inputpath_new = os.path.join(inputpath_simulation, inputname_new)
            copyfile(inputpath_old, inputpath_new)
def FOL_running_main():#触发这个
    for product_name in ['renr','xy','rj','zx','gy']:
        file_update(product_name)
    pt=portfolio_tracking()
    try:
        pt.saving_main('仁睿价值精选1号')
    except:
        pass
    try:
        pt.saving_main('惠盈一号')
    except:
        pass
    try:
        pt.saving_main('瑞锐精选')
    except:
        pass
    try:
         pt.saving_main('瑞锐500')
    except:
        pass
    try:
         pt.saving_main('念觉沪深300')
    except:
        pass
    try:
         pt.saving_main('念觉知行4号')
    except:
        pass
    try:
         pt.saving_main('高益振英一号')
    except:
        pass
def FOL_running_main_simulation():#触发这个
    for product_name in ['xy','rj','zx','gy']:
        file_update(product_name)
    pt=portfolio_tracking(simulation=True)
    pt.saving_main('惠盈一号')
    pt.saving_main('瑞锐精选')
    pt.saving_main('瑞锐500')
    pt.saving_main('念觉沪深300')
    pt.saving_main('念觉知行4号')
    pt.saving_main('高益振英一号')
if __name__ == '__main__':
      FOL_running_main()

