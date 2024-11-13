import os
import pandas as pd
import pywencai
from tqdm import tqdm
import sys
import tushare as ts
from junwei_util import *     #这是我自己常用的工具函数
from tqdm import tqdm
from datetime import datetime

def get_popular_stock(st, calendar):
    # 1. 对 calendar 列表进行排序并筛选出比 st 大的元素
    temp=calendar[calendar.index(st)-1]
    calendar = sorted([date for date in calendar if date >=temp ])
    print(len(calendar))

    # 2. 创建一个空的 DataFrame，设置列名
    merged_df = pd.DataFrame(columns=['股票代码', '股票简称', '连续涨停天数', '个股热度排名', '个股热度'])

    # 3. 开始循环遍历 calendar 中的每个日期
    for require_time in tqdm(calendar[1:]):
        # 构建 query 字符串
        
        query = f"{require_time}断板 {calendar[calendar.index(require_time) - 1]}热度排名升序 非ST 主板"
        if require_time>datetime.now().strftime('%Y%m%d'):
            continue
        # 4. 调用 pywencai.get() 获取数据
        df = pywencai.get(query=query, perpage=50, sort_order='asc', query_type='stock', loop=True)
        if  df is None:
            print(f"No data for {require_time}")
            continue

        # 只保留指定的列
        required_columns = ['股票代码', '股票简称', '连续涨停天数', '个股热度排名', '个股热度']
        df = df[[col for col in df.columns if any(keyword in col for keyword in required_columns)]]

        # 遍历列名并清理，如果列名包含 '[' 则去除后续部分
        df.columns = [col.split('[')[0] if '[' in col else col for col in df.columns]

        # 创建 "交易日期" 列并赋值为 require_time
        df['断板日期'] = require_time
        df['热度排名日期']=calendar[calendar.index(require_time)-1]

        # 5. 合并 df 和 merged_df
        merged_df = pd.concat([merged_df, df], ignore_index=True)

    # 6. 返回合并后的 DataFrame
    return merged_df

def update_data(data_path,calendar):
    calendar=pd.read_excel(calendar)
    calendar= calendar['day'].dt.strftime('%Y%m%d').tolist()

    a=pd.read_excel(data_path)
    # 4. 调用 pywencai.get() 获取数据
    require_time=datetime.now().strftime('%Y%m%d')
    query = f"{require_time}断板 {calendar[calendar.index(require_time) - 1]}热度排名升序 非ST 主板"
    df = pywencai.get(query=query, perpage=50, sort_order='asc', query_type='stock', loop=True)
    if  df is None:
        print(f"No data for {require_time}")
        a.to_excel(data_path, index=False)
        return a

    # 只保留指定的列
    required_columns = ['股票代码', '股票简称', '连续涨停天数', '个股热度排名', '个股热度']
    df = df[[col for col in df.columns if any(keyword in col for keyword in required_columns)]]

    # 遍历列名并清理，如果列名包含 '[' 则去除后续部分
    df.columns = [col.split('[')[0] if '[' in col else col for col in df.columns]

    # 创建 "交易日期" 列并赋值为 require_time
    df['断板日期'] = require_time
    df['热度排名日期']=calendar[calendar.index(require_time)-1]
    

    # 5. 合并 df 和 merged_df
    a = pd.concat([a, df], ignore_index=True)
    a.drop_duplicates(subset=['股票代码','断板日期'], inplace=True)
    a.to_excel(data_path, index=False)
    return a

def price_df_load(a,calendar_list,api=None):
    calendar_list=list(set(calendar_list).union(set(a['断板日期'])))
    calendar_list.sort()
    a_groups=a.groupby('断板日期')
    price_df = pd.DataFrame()
    if api:
        pro=ts.pro_api(api)
    else:
        pro=ts.pro_api('b7b78af576177988e770b8a59acf6eecfcd085cad34f40e2b970843d')
    for date ,stock in tqdm(a_groups):
        try:
            idx = calendar_list.index(date)
        except ValueError:
            raise ValueError(f"The date {date} is not found in the calendar list.")
        
        # 2. 获取前20个值作为 `st`，后20个值作为 `et`
        if idx < 20 or idx > len(calendar_list) - 21:
            raise IndexError("Date is too close to the start or end of the calendar list for this operation.")
        
        st = date
        et = calendar_list[idx + 20]

        # 3. 获取 `stock` DataFrame 的 '股票代码' 列并生成字符串
        codes = ','.join(stock['股票代码'].tolist())
        # 4. 获取 `st` 到 `et` 之间 `codes` 股票的日线数据
        df = pro.daily(ts_code=codes, start_date=st, end_date=et)
        # 5. 合并 `df` 到 `price_df`
        price_df = pd.concat([price_df, df])
    price_df.sort_values(by=['trade_date', 'ts_code'], inplace=True)
    price_df.reset_index(drop=True, inplace=True)
    price_df.drop_duplicates(subset=['trade_date', 'ts_code'], inplace=True)
    return price_df

def callback_analysis(a,calendar_list,price_df):
    b=a.copy()
    a_groups=a.groupby('断板日期')
    high_list = []
    down_count_list = []
    down_date=[]
    low_list = []
    ban_stock=[]
    for date ,stock in tqdm(a_groups):
        
        try:
            idx = calendar_list.index(date)
        except ValueError:
            raise ValueError(f"The date {date} is not found in the calendar list.")
        
        # 2. 获取前20个值作为 `st`，后20个值作为 `et`
        if idx < 20 or idx > len(calendar_list) - 21:
            raise IndexError("Date is too close to the start or end of the calendar list for this operation.")
        
        st = calendar_list[idx - 20]
        et = calendar_list[idx + 20]
        day_price=price_df[(price_df['trade_date']>=date) & (price_df['trade_date']<=et)]
        # 3. 获取 `stock` DataFrame 的 '股票代码' 列并生成字符串
        #codes = ','.join(stock['股票代码'].tolist())
        codes = stock['股票代码'].tolist()
        
        for i in range(len(codes)):
            found_break = False
            down_count=0#get_price(security, start_date=None, end_date=None, frequency='daily', fields=None, skip_paused=False, fq='pre', count=None, panel=True, fill_paused=True)
            stock_price=day_price[day_price['ts_code']==codes[i]]
            # high=stock_price['close']
            if stock_price.empty:
                print(f"{codes[i]} 在 {date} 没有数据，因为当日停牌或者价格数据没有在tushare中及时更新")
                print(f'本次代码中会将停牌股票从结果中剔除,即当做{date}不能存在{codes[i]}的断板')
                ban_stock.append([codes[i],date])
                continue
            else:
                high=list(stock_price['close'])[0]
                high_list.append(high)

            for j in range(1,len(stock_price)):
                if stock_price.iloc[j]['close']<=stock_price.iloc[j-1]['close']:
                    down_count+=1
                else:
                    #print(codes[i],down_count)
                    low_list.append(stock_price.iloc[j-1]['close'])
                    down_count_list.append(down_count)
                    down_date.append(stock_price.iloc[j - 1]['trade_date'])
                    found_break=True
                    break
            #print(len(down_count_list),i)
            # 如果循环没有 break，则执行下面的 else 的内容
            if not found_break:
                print(date,codes[i],'出现持续回调')
                low_list.append(stock_price.iloc[len(stock_price)-1]['close'])
                #print(len(low_list))
                down_count_list.append(down_count)
                down_date.append(999)
    for stock in ban_stock:
        stock_code, break_date = stock[0], stock[1]
        # 删除股票代码列为 stock_code 且断板日期为 break_date 的行
        b = b[~((b['股票代码'] == stock_code) & (b['断板日期'] == break_date))]
    print(len(b),len(down_count_list))
    b['断板收盘价']=high_list
    b['回调天数']=down_count_list
    b['回调结束日期']=down_date
    b['回调结束收盘价']=low_list
    return b
def rebound_analysis(b_cleaned,calendar_list,price_df):
    rebound_count_list=[]
    rebonud_price_list=[]
    rebound_date_list=[]
    b_cleaned_groups = b_cleaned.groupby('回调结束日期')
    for date, stock in tqdm(b_cleaned_groups):
        # 将 `date` 转换为字符串格式以匹配 `calendar`
        date_str = date
        
        try:
            idx = calendar_list.index(date_str)
        except ValueError:
            raise ValueError(f"The date {date_str} is not found in the calendar list.")
        
        # 获取前20个值作为 `st`，后20个值作为 `et`
        if idx < 20 or idx > len(calendar_list) - 21:
            raise IndexError("Date is too close to the start or end of the calendar list for this operation.")
        
        st = calendar_list[idx - 20]
        et = calendar_list[idx + 20]
        day_price=price_df[(price_df['trade_date']>=date) & (price_df['trade_date']<=et)]
        codes = stock['股票代码'].tolist()
        for i in range(len(codes)):
            found_break = False
            rebound_count=0
            stock_price=day_price[day_price['ts_code']==codes[i]]
            if codes[i] =='601727.SH':
                print(codes[i])

            for j in range(1,len(stock_price)):
                if stock_price.iloc[j]['close']>=stock_price.iloc[j-1]['close']:
                    rebound_count+=1
                else:
                    rebound_count_list.append(rebound_count)
                    rebonud_price_list.append(stock_price.iloc[j-1]['close'])
                    rebound_date_list.append(stock_price.iloc[j-1]['trade_date'])
                    found_break=True
                    break
            if not found_break:
                print(date,codes[i],'出现持续反弹')
                rebonud_price_list.append(stock_price.iloc[len(stock_price)-1]['close'])
                #print(len(low_list))
                rebound_count_list.append(rebound_count)
                rebound_date_list.append(999)
    print(len(b_cleaned),len(rebound_date_list))
    b_cleaned['回弹结束收盘价']=rebonud_price_list
    b_cleaned['回弹天数']=rebound_count_list
    b_cleaned['回弹结束日期']=rebound_date_list
    b_cleaned['回弹百分比'] = (b_cleaned['回弹结束收盘价'] - b_cleaned['回调结束收盘价']) / b_cleaned['回调结束收盘价']
    return b_cleaned

if __name__ == '__main__':
    calendar_path = "日历（上海）.xlsx"
    calendar_list = pd.read_excel(calendar_path) #注意，这个来自聚宽的日历有问题，需要用另一个日历源，现在只是把问财里出现的日期和他的日期取并集
    calendar_list= calendar_list['day'].dt.strftime('%Y%m%d').tolist()
    # 判断文件是否存在
    file_name = '临时数据.xlsx'
    file_exists = os.path.exists(file_name)
    a=None
    if file_exists:
        print(f"文件 '{file_name}' 存在,说明数据文件存在，仅更新最新的数据")
        a=update_data(file_name,calendar_path)
        a.to_excel(file_name, index=False) 
        print('初始数据已保存')   
    else:
        print(f"文件 '{file_name}' 不存在,从头开始获取数据,第一个参数请填写你希望的开始时间")
        a=get_popular_stock('20240801', calendar_list)
    
    print('-'*80)
    print('下载日线数据中...')
    preice_df=price_df_load(a,calendar_list)
    print('下载日线数据完成,开始回调统计...')
    b=callback_analysis(a,calendar_list,preice_df)
    b.to_excel('回调分析.xlsx',index=False)
    b_cleaned = b[b['回调结束日期'] != 999]
    b_cleaned=b_cleaned.sort_values(by=['回调结束日期','股票代码'])
    b_cleaned['回调结束日期'] = pd.to_datetime(b_cleaned['回调结束日期']).dt.strftime('%Y%m%d')
    calendar_list=set(calendar_list).union(set(b_cleaned['回调结束日期'].tolist()))
    calendar_list=sorted(calendar_list)
    # 重新赋值为列表格式
    calendar_list = list(calendar_list)
    print('回调统计完成,开始回弹统计...')
    b_cleaned=rebound_analysis(b_cleaned,calendar_list,preice_df)
    b_cleaned.to_excel('回弹分析.xlsx',index=False)
    merged_df = b.merge(
    b_cleaned.drop(columns=b.columns.intersection(b_cleaned.columns)),  # 只保留b_cleaned新增的列
    how='left',  # 左连接，保留b的所有行
    left_index=True,
    right_index=True
    )
    merged_df.to_excel('合并分析.xlsx',index=False)
    print('回弹统计完成,数据已保存至文件')



    
