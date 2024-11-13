from datetime import datetime, timedelta
import pandas as pd
import calendar
import numpy as np
import os
from tqdm import tqdm
import shutil
import time
def multi_extension_fileload(path,sort_column=None):
    """
    读取文件然后，根据文件后缀选择读取方式，并可选择是否对数据进行排序。
    
    参数:
    path (str): 文件绝对路径。
    sort_column (str, optional): 排序列名。默认为空。
    """
    # 获取文件后缀
    _, file_extension = os.path.splitext(path)
    
    # 根据文件后缀选择读取方式
    if file_extension == '.xlsx':
        df = pd.read_excel(path)
    elif file_extension == '.csv':
        df = pd.read_csv(path)
    elif file_extension == '.pkl':
        df = np.load(path, allow_pickle=True)
    else:
        raise ValueError(f"Unsupported file extension: {file_extension}") 
    if sort_column:
        df=df.sort_values(by=sort_column)
    return df 

def dateset_start_end_produce(start_date,end_date):
    """
    给定str格式的起止日期('20180101')，生成标准的集合起止日期，主要用于大训练（测试）集的起止日期划分,严格按实际月份划分
    """
    # 初始化变量
    print("正在产生按实际年月生成的数据集划分列表")
    test_set_start_list = []
    test_set_end_list = []
    # 转换为日期对象
    start_date_dt = datetime.strptime(start_date, '%Y%m%d')
    end_date_dt = datetime.strptime(end_date, '%Y%m%d')

    # 开始遍历从 start_date 到 end_date 之间的每个月
    current_date = start_date_dt.replace(day=1)

    while current_date <= end_date_dt:
        # 获取当前月的第一天和最后一天
        first_day = current_date.replace(day=1)
        last_day = current_date.replace(day=calendar.monthrange(current_date.year, current_date.month)[1])
        # 转换回字符串格式并加入列表
        test_set_start_list.append(first_day.strftime('%Y%m%d'))
        test_set_end_list.append(last_day.strftime('%Y%m%d'))
        # 移动到下一个月
        next_month = current_date.month % 12 + 1
        next_year = current_date.year + (current_date.month // 12)
        current_date = current_date.replace(year=next_year, month=next_month, day=1)
    return test_set_start_list, test_set_end_list


def unique_dateset_start_end_produce(date_list,interval):
    """
    函数作用为：给定特定的date_list，来每隔interval个元素就设置一个测试集区间
    """
   
    test_set_start_list = []
    test_set_end_list = []
    print('正在生成测试集区间')
    
    # 遍历date_list，每隔interval个元素进行划分
    for i in range(0, len(date_list), interval):
        # 起始时间为当前区间的第一个元素，如果不是第一个划分，则取上一个end_date的下一个元素
        if i == 0:
            start_date = date_list[i]
        else:
            start_date = date_list[min(i, len(date_list) - 1)]
        
        # 结束时间为当前区间的最后一个元素，如果不足interval则取date_list的最后一个元素
        end_date = date_list[min(i + interval - 1, len(date_list) - 1)]
        
        # 添加到结果列表
        test_set_start_list.append(start_date)
        test_set_end_list.append(end_date)
    
    return test_set_start_list, test_set_end_list

def clean_cache(path):
    """
    删除指定路径下的 __pycache__ 和 .ipynb_checkpoints 文件夹。
    
    参数:
    path (str): 要清理的根目录的绝对路径。
    """
    
    for root, dirs, files in os.walk(path):
        root_split = root.split(os.sep)  # 使用 os.sep 以支持跨平台路径分隔符
        if '__pycache__' in root_split or '.ipynb_checkpoints' in root_split:
            print('删除：', root)
            shutil.rmtree(root)


def run_at_time(target_time_str, sleep_time, task_func=None, *args, **kwargs):
    """
    在系统当前时间超过指定时间后执行指定任务函数。
    
    参数:
    - target_time_str (str): 目标时间，格式为 'YYYYMMDDHHMMSS'，例如 '20180103101530'。
    - sleep_time (int): 每隔多少秒检查一次当前时间，如果为 0 则立即重新检查。
    - task_func (function, optional): 要执行的任务函数。如果为空则不执行任务，只退出循环。
    - *args, **kwargs: 可变参数和关键字参数，用于传递给 task_func。
    """
    if target_time_str:
    # 转换为 datetime 对象
        # 将输入的时间字符串转换为 datetime 对象
        target_time = datetime.strptime(target_time_str, "%Y%m%d%H%M%S")
        print("当时间超过", target_time,'时执行任务...')
        while True :
            # 获取系统当前时间
            current_time = datetime.now()
            
            # 判断当前时间是否超过目标时间
            if current_time >= target_time:
                print("目标时间已到")
                # 检查 task_func 是否为空
                if task_func:
                    print("执行任务...")
                    task_func(*args, **kwargs)  # 执行任务函数
                break  # 任务执行后或 task_func 为空时，跳出循环
            
            # 如果没有到达目标时间，等待指定的秒数
            if sleep_time > 0:
                time.sleep(sleep_time)
            else:
                time.sleep(0)  # 立即重新检查当前时间
# 方法1：使用 datetime 处理
def add_days(time_str,days,time_format='%Y%m%d%H%M%S'):
    # 将字符串转换为 datetime 对象
    dt = datetime.strptime(time_str, time_format)
    # 加一天
    next_day = dt + timedelta(days=days)
    # 转回字符串格式
    return next_day.strftime(time_format)

def stockcode_transfer(stock_code, transfe_type):
    # 去除每个股票代码中非数字部分
    stock_code = [''.join(filter(str.isdigit, code)) for code in stock_code]
    # 根据 transfe_type 处理不同情况
    if transfe_type == 'number' or transfe_type == 0:
        return stock_code
    elif transfe_type == '4' or transfe_type == 4:
        # 判断首位并添加后缀
        stock_code = [
            code + '.XSHG' if code.startswith('6') else code + '.XSHE' 
            if code.startswith('0') or code.startswith('3') else code 
            for code in stock_code
        ]
        return stock_code
    elif transfe_type == '2' or transfe_type == 2:
        # 判断首位并添加后缀
        stock_code = [
            code + '.SH' if code.startswith('6') else code + '.SZ' 
            if code.startswith('0') or code.startswith('3') else code 
            for code in stock_code
        ]
        return stock_code
    else:
        raise ValueError("Invalid transfe_type. Expected 'number', '4', or '2'.")
    
def df_value_type(df_column):
    return type(list(df_column)[0])

