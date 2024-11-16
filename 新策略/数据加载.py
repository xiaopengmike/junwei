import os
import pandas as pd
import pywencai
from tqdm import tqdm
import sys
import tushare as ts
#sys.path.append(r'D:\作业\实习工作相关')
#from junwei_util import *     #这是我自己常用的工具函数
from tqdm import tqdm
from datetime import datetime
def get_popular_stock(st, calendar,hot=150):
    # 1. 对 calendar 列表进行排序并筛选出比 st 大的元素
    temp=calendar[calendar.index(st)-1]
    calendar = sorted([date for date in calendar if date >=temp ])
    #print(len(calendar))
    # 2. 创建一个空的 DataFrame，设置列名
    merged_df = pd.DataFrame(columns=['股票代码', '股票简称',  '个股热度排名', '个股热度'])

    # 3. 开始循环遍历 calendar 中的每个日期
    for i in tqdm(range(3,len(calendar))):
        # 构建 query 字符串
        
        query=f'{calendar[i-3]}下跌 {calendar[i-2]}上涨 {calendar[i-2]}热度排名小于{hot} {calendar[i-1]}上涨 {calendar[i]}涨幅'
        #print(query)
        if calendar[i]>datetime.now().strftime('%Y%m%d'):
            continue
        # 4. 调用 pywencai.get() 获取数据
        df = pywencai.get(query=query, perpage=50, sort_order='asc', query_type='stock', loop=True)
        if  df is None:
            print(f"No data for {i}")
            continue
        #print(df)
        # 只保留指定的列
        required_columns = ['股票代码', '股票简称', '个股热度排名', '个股热度']
        
        duplicate_col=['涨跌幅','收盘价']
        for col in duplicate_col:
            matching_columns = [col for col in df.columns if '涨跌幅' in col]
            matching_columns.sort()
            required_columns.append(matching_columns[-1])
        df = df[[col for col in df.columns if any(keyword in col for keyword in required_columns)]]


        # 遍历列名并清理，如果列名包含 '[' 则去除后续部分
        df.columns = [col.split('[')[0] if '[' in col else col for col in df.columns]

        # 创建 "交易日期" 列并赋值为 require_time
        df['涨跌幅日期'] = calendar[i]
        df['热度排名日期']= calendar[i-2]

        # 5. 合并 df 和 merged_df
        merged_df = pd.concat([merged_df, df], ignore_index=True)
        merged_df.sort_values(by=['涨跌幅日期','个股热度排名'], inplace=True)

    # 6. 返回合并后的 DataFrame
    return merged_df

date_list=pd.read_excel("日历（上海）.xlsx")
date_list= date_list['day'].dt.strftime('%Y%m%d').tolist()


st=input('请输入要查询的你期望的开始日期，格式类似于20241008：')
hot=input('请输入你期望的个股热度升序排名，请注意你的输入会明显影响速度下载速度：')
a=get_popular_stock("20241008",date_list,hot)
a.to_excel("新策略.xlsx",index=False)