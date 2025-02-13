import os
import pandas as pd
import pywencai
from tqdm import tqdm
import sys
import tushare as ts
from junwei_util import *     #这是我自己常用的工具函数
from tqdm import tqdm
from datetime import datetime
import re
import time
import random

def process_single(tem_df,date,query):
    if tem_df is None:
        print(date,"为None")
        return False
    elif tem_df.empty:
        print(date,"没有数据")
        return False
    else:
        tem_df=tem_df.dropna()
        duplicate_col=['涨跌幅','最大涨幅']
        required_columns = ['股票代码', '股票简称', '个股热度','个股热度排名']
        for col in duplicate_col:
            matching_columns = [column for column in tem_df.columns if col in column and '最新' not in column]
            matching_columns.sort()
            
            # 调整列顺序
            tem_df = tem_df[[col for col in tem_df.columns if col not in matching_columns] + matching_columns]
            
            # 给 matching_columns 列表中的列添加后缀 _n, _n+1, _n+2 ...
            new_column_names = [f"{matching_columns[0]}_{i}" for i in range(len(matching_columns))]
            rename_mapping = dict(zip(matching_columns, new_column_names))
            tem_df = tem_df.rename(columns=rename_mapping)

            required_columns.extend(new_column_names)
        # 保留包含特定关键词且不包含'最新'的列
        
        tem_df = tem_df[[col for col in tem_df.columns if any(keyword in col for keyword in required_columns)]]
        # 删除列名中'['后的字符串
        tem_df.columns = [re.sub(r'\[.*?\]', '', col) for col in tem_df.columns]
        tem_df['日期']=date
        tem_df['语句']=query
        
        return tem_df
def process_single_concept(tem_df,date,query):
    if tem_df is None:
        print(date,"为None")
        return False
    elif tem_df.empty:
        print(date,"没有数据")
        return False
    else:
        tem_df=tem_df.dropna()
        required_columns = ['指数代码', '指数简称', '板块热度','板块热度排名']
        tem_df = tem_df[[col for col in tem_df.columns if any(keyword in col for keyword in required_columns)]]
        # 删除列名中'['后的字符串
        tem_df.columns = [re.sub(r'\[.*?\]', '', col) for col in tem_df.columns]
        tem_df['日期']=date
        tem_df['语句']=query
        
        return tem_df

def remove_consecutive_duplicates(df):
    """
    删除'指数简称'列中连续重复的行，保留第一次出现的行
    
    参数:
    df (pandas.DataFrame): 包含'指数简称'列的数据框
    
    返回:
    pandas.DataFrame: 处理后的数据框
    """
    # 创建布尔掩码，标识需要保留的行
    df=df.drop_duplicates(subset='日期',keep='first')  #保证选到的是第一个热度
    mask = df['指数简称'].shift() != df['指数简称']
    
    # 第一行需要保留（因为shift会产生NaN）
    mask.iloc[0] = True
    
    # 返回经过筛选的数据框
    return df[mask]





if __name__ == '__main__':
    date_list=pd.read_excel(r"交易所日历.xlsx")
    date_list= date_list['day'].dt.strftime('%Y%m%d').tolist()
    st=input("输入开始日期，格式类似于20210101：")
    ed=input("输入结束日期，格式类似于20210101：")
    hot=input("输入板块热度要求，输入10则代表10万以上：")
    date_list=[i for i in date_list if i >= st and i <= ed]
    all_list=[]
    for i in tqdm(date_list):
        #run_at_time('20250128190500',120)

        query=f'{i}板块热度降序 {i}板块热度大于10万 指数代码886开头'
        print(query)
        df=pywencai.get(query=query, perpage=100, sort_order='asc', query_type='zhishu', loop=False,no_detail=True)#user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 SLBrowser/9.0.5.12181 SLBChan/25 SLBVPV/64-bit')
        tem=process_single_concept(df,i,query)
        all_list.append(tem)
    all_list=[i for i in all_list if isinstance(i,pd.DataFrame)]
    all_df=pd.concat(all_list)
    all_df.to_csv('板块轮动.csv',index=False)
    all_df=pd.read_csv('板块轮动.csv')
    # 使用示例
    all_df = remove_consecutive_duplicates(all_df)
    # 计算平均天数间隔
    all_df['日期'] = pd.to_datetime(all_df['日期'], format='%Y%m%d')
    date_diff = all_df['日期'].diff().dt.days
    average_interval = date_diff.mean()
    print(f"平均天数间隔为：{average_interval} 天")

    all_list=[]
    for _, row in tqdm(all_df.iterrows()):
            # 获取日期和指数简称
            date = row['日期']
            concept = row['指数简称']
            if isinstance(date, str):
                # 如果已经是字符串，移除可能的分隔符
                date = ''.join(filter(str.isdigit, date))
            else:
                # 如果是datetime对象，转换为字符串格式
                date = date.strftime('%Y%m%d')
            today_index = date_list.index(date)
            #print(f"正在查询 {concept} 板块 {date} 日数据")
                
            # 确保today_index+1不会超出列表范围
            if today_index + 1 < len(date_list):
                # 生成查询语句
                query = f'属于{concept}板块的股票{date_list[today_index]}和{date_list[today_index+1]}涨跌幅 {date_list[today_index+1]}最高涨幅 {date_list[today_index]}个股热度排名升序'
                tem_df=pywencai.get(query=query, perpage=100, sort_order='asc', query_type='stock', loop=True,no_detail=True)
                tem_df=process_single(tem_df,date,query)
                if isinstance(tem_df,pd.DataFrame):
                    tem_df['板块'] = concept
                    all_list.append(tem_df)

            else:
                print(f"警告: 日期 {date} 是最后一个日期，无法获取下一日数据")
    new_all_df=pd.concat(all_list)
    new_all_df.to_csv('板块轮动股票.csv',index=False)
