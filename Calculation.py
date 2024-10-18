from datetime import date
import math
import pandas as pd
from sqlalchemy import create_engine

from Modules import authenticate_google_sheets, get_sheet, get_cell_value, Connect_to_MSSQL, extract_data, filter_data ,hearders_to_sheet, write_to_sheet, clear_sheet, process_data_and_update_sheet
from data_SQLquery_list import Operation_data_list, User_data_list_week

def ProductContribution_Ranking(client, config):
    """
    彙整整個過程: 資料清理->計算WOW->計算window_sum->計算貢獻度->排序

    param: 使用 extract_data 取出的資料
    param 
    """
    # 認證並獲取工作表、撈取並過濾SQL數據、清空並更新Google Sheet
    clean_data(data_list, date)

def clean_data(data_list, date):
    """
    資料清理，將NaN替換成0、將指定日期的資料過慮出來。

    param data_list: 資料陣列，each element is of DataFrame type
    param date: 用來篩選資料的日期
    return clean_data: 使用日期篩選出來的資料
    """
    # Replace NaN with 0
    data_without_NaN = [element.fillna(0) for element in data_list] # data_without_NaN: list. element: DataFrame

    # Filter out data for specific date
    cleaned_data = filter_data(data_without_NaN, date)
    
    return cleaned_data


def get_WoW_Stats(cleaned_data):
    """
    計算 Week over Week(WoW)。兩週間的變化量。

    param cleaned_data: 使用日期篩選出來的資料
    return: 包含處理後 DataFrame 的列表
    """
    WoW_list = []
    for k in cleaned_data:
        
        if 'previous_active' in k.columns:
            previous_column = 'previous_active'
        elif 'previous_appsession' in k.columns:
            previous_column = 'previous_appsession'
        else:
            print("缺少 'previous_active' 或 'previous_appsession' 列")
            continue
        
        # 計算WoW + 把NaN替換成0
        k.loc[:, 'WoW'] = round(k['difference'] / k[previous_column] * 100, 1).fillna(0)
        # 將 WoW 列轉換成百分比格式
        k.loc[:, 'WoW']  = k['WoW'].astype(str) + '%'

        WoW_list.append(k)
        
    return k

def window_sum(cleaned_data):
    """
    加總指定日期的所有的差額。

    param cleaned_data: 使用日期篩選出來的資料
    return: 
    """
    winsum = [l['difference'].sum() for l in cleaned_data] 

    return winsum

def get_Contribution_Stats(cleaned_data, winsum_list):
    """
    計算成長貢獻度 = 單一產品L01 / 所有產品總L01，用以判斷該產品在整體的成長或是衰退，有多大的貢獻度。

    param cleaned_data: 使用日期篩選出來的資料
    param winsum: 指定日期的所有的差額總計
    """
    Contribution_list = []
    for j, winsum in zip(cleaned_data, winsum_list): # zip: pair 2 (or more) lists into tuples，j 配上 winsum(一組)
        j['Contribution'] = j['difference'].apply(lambda x: round(x / winsum * 100, 2))
        Contribution_list.append(j)
    return Contribution_list

def Rank_Contribution(Contribution_list):
    """"
    對「貢獻度」進行排序: 高->低

    param Contribution_list: 有計算貢獻度的資料
    return Ranked_Data: 以貢獻度排序的資料
    """
    Ranked_Data = []
    for df in Contribution_list:
        # 對 'Contribution' 列進行排序
        sorted_df = df.sort_values(by=['Contribution'], ascending=False)
        # 將 'Contribution' 列轉換為百分比
        sorted_df['Contribution'] = sorted_df['Contribution'].astype(str) + '%'
        # 取前十 & 後十
        top_ten = sorted_df.head(10)
        bottom_ten = sorted_df.tail(10)
        
        Ranked_Data.append((top_ten, bottom_ten))

    return Ranked_Data


# 取資料看看
data_list = extract_data(User_data_list_week)
test_data_list = clean_data(data_list, '2024-10-07') # list
ws = window_sum(test_data_list)
# print(ws)
CS = get_Contribution_Stats(test_data_list, ws)
# print(CS)
print(Rank_Contribution(CS))

