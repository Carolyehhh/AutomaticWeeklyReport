from datetime import date
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
from sqlalchemy import create_engine
import re

"""
目的：
1. 連接Python到Google Sheet API
2. 資料清理
"""
# 通用 functions
def authenticate_google_sheets(api_key_path, scopes):
    """"
    認證 Google Sheets API 並返回客戶端對象。
    
    param api_key_path: API 金鑰的文件路徑
    param scopes: 認證範圍
    return: gspread 客戶端對象
    """
    credentials = Credentials.from_service_account_file(api_key_path, scopes=scopes)
    client = gspread.authorize(credentials)
    return client

def Connect_to_MSSQL():
    """
    連接 SQL Server。
    """
    try: # try, except 處理異常，發生異常時直接跳到 except，不執行完其餘部分
        print("GSQLAlchemy 連接 MSSQL 資料庫")
        # 使用 SQLAlchemy 連接 MSSQL 資料庫
        engine = create_engine('mssql+pymssql://carol_yeh:Cmoneywork1102@192.168.121.50/master') #已改
        connection = engine.connect()
        print("GSQLAlchemy 連接.success")  
        return engine
    except Exception as e:
        print(f"An error occured: {e}")
        return None
    finally:
        # engine.dispose() # 關閉 SQLAlchemy 連接
        pass

def extract_data(sql_queries_list):
    """
    依照list順序，吃SQL資料。
    
    param sql_queries_list: 要吃的所有資料的SQL query
    return all_data: list，每個 elements 為 Dataframe
    """
    engine = Connect_to_MSSQL()
    if engine is not None:
        all_data = []
        for query in sql_queries_list:
            try:
                read_data = pd.read_sql(query, engine)
                all_data.append(read_data)
            except Exception as e:
                print(f"讀取數據時發生錯誤: {e}")
        engine.dispose()
        
    else:
        print("無法連接到資料庫")
        return None
    return all_data

def filter_data(all_data, b1_value):
    """"
    過濾資料。

    param data (list但內容物是DataFrame): 要過濾的資料
    param b1_value (float): Google Sheet上輸入的篩選日期

    return: 過濾後的資料 DataFrame
    """

    filtered_data_list = []
    for content in all_data:
        # print('ct', content)
        content['日期'] = content['日期'].astype(str) # 日期格式為 str
        # print(content['日期'][0], type(content['日期'][0])) # str
        filtered_data = content.loc[content['日期']==b1_value]
        filtered_data_list.append(filtered_data)

    return filtered_data_list

def write_to_sheet(data, worksheet, cell):
    """
    將資料寫入工作表

    param data: 要寫入的資料
    param worksheet: 指定資料更新的路徑
    param cell: 填入資料的起始格

    return 在Google Shhet上更新資料
    """
    all_data = []
    if isinstance(data, list):
        for element in data:
            data_to_list = element.values.tolist()
            all_data.extend(data_to_list)
    elif isinstance(data, pd.DataFrame):
        for index, row in data.iterrows():
            data_to_list = row.values.tolist()
            all_data.extend(data_to_list)

    # # 把資料轉成list才能寫入Google Sheet
    # data_list = data.values.tolist()

    return worksheet.update(cell, all_data)

def transpose_data_prdline(data, value_column = 'current_value'):
    """
    將資料轉置成以產品線為欄位名稱的格式

    param data: 需要轉置的 DataFrame
    param value_column: 指定需要轉置的欄位名稱
    """
    data_list = []

    # 檢查資料是否為 list，如果是 list，將其轉換為 DataFrame
    if isinstance(data, list):
        df = pd.concat(data, ignore_index=True)
    else:
        df = data

    # 確認 DataFrame 的形狀為 2D
    if df.ndim != 2:
        raise ValueError("資料必須是 2D 的 DataFrame。")

    # 使用 pivot 函數轉置資料
    df_pivot = df.pivot(index=['日期', '月、日'], columns='產品線', values=value_column)

    # 處理缺失值
    df_pivot = df_pivot.fillna(0)

    # 重置索引並將欄位名稱改為平行格式
    df_pivot.reset_index(inplace=True)
    df_pivot.columns.name = None

    # 設定欄位順序
    desired_order = ['日期','月、日','X實驗室','Money錢','網紅','發票','記帳','社群','其他','作者','同學會','大眾']
    columns_order = desired_order + [col for col in df_pivot.columns if col not in desired_order]
    df_pivot = df_pivot[columns_order]
    
    # 計算同一日期所有值的總計
    df_pivot['總和'] = df_pivot.drop(columns=['日期', '月、日']).sum(axis=1)

    # 計算差額
    df_pivot['總和差額'] = df_pivot['總和'].diff()
    # 計算金融事業群的差額

    # 定義分類函數
    def classify_diff(diff):
        if pd.isna(diff): #處理第一個值為NAN的情況
            return None, None
        elif diff > 0:
            return diff, None
        else:
            return None, diff

    df_pivot[['總和差額(正)', '總和差額(負)']] = df_pivot['總和差額'].apply(lambda x: pd.Series(classify_diff(x))).fillna("")
    # print(df_pivot)

    def calculate_diff(df, columns):
        for col in columns:
            if col not in ['日期', '月、日', '總和', '總和差額(正)', '總和差額(負)', '總和差額']:
                df[col + '差額']  = df[col].diff().fillna(0)
        return df

    df_pivot = calculate_diff(df_pivot, df_pivot.columns)
    df_pivot = df_pivot.tail(25)
    # print(df_pivot)

    # JSON 不支持 datetime 格式，要轉成str
    df_pivot['日期'] = df_pivot['日期'].astype(str)


    # 將df_pivot裝進list, element為DataFrame
    data_list.append(df_pivot)

    return data_list

def transpose_data_lifecycle(data):
    """
    將資料轉置成以產品線為欄位名稱的格式
    # 產品線:金融事業群

    param data: Lists need to be transposed, with DataFrame elements
    """
    # 篩選指定產品線
    target_prdlinename = ['Money錢', '大眾', '同學會', '作者']
    data = data[0][data[0]['產品線'].isin(target_prdlinename)]
    data = data[['日期', '月、日', 'recall_uesr', 'retained_user', 'new_user']]

    # 轉換為 '日期' 和 '月、日' 作為索引
    data.set_index(['日期', '月、日'], inplace=True)

    # 將數值欄位根據 '日期' 進行加總
    data_aggregated = data.groupby(['日期', '月、日']).sum().reset_index()  
    data_aggregated[['日期', '月、日']] = data_aggregated[['日期', '月、日']].astype(str)
    # print(data_aggregated) # DataFrame

    # Insert DataFrame into a list
    data_list = []
    data_list.append(data_aggregated)

    return data_list



class GoogleSheetProcessor:
    def __init__(self, client, config):
        """
        初始化類
        param client: gspread client
        param config: 包含所需參數的字典，包括:
            - 'date_sheet_url': 獲取"當前日期"工作表的 URL
            - 'date_sheet_name': 獲取"當前日期"工作表的名稱
            - 'output_sheet_url': "寫入數據"工作表的 URL
            - 'output_sheet_name': "寫入數據"工作表的名稱
            - 'current_date_cell': 存儲"當前日期"的單元格位置
            - 'raw_data_cell': 寫入數據的起始單元格位置
            - 'data_list': 包含 SQL 查詢的列表
            - 'clear_cell_range': 刪除資料的範圍
        """ 
        self.client = client
        self.config = config
        self.date_worksheet = self.get_sheet(config['date_sheet_url'], config['date_sheet_name'])
        self.output_worksheet = self.get_sheet(config['output_sheet_url'], config['output_sheet_name'])
    
    def get_sheet(self, sheet_url, sheet_name):
        """
        獲取 Google Sheets 的工作表對象。
        
        param sheet_url: Google Sheets 的 URL
        param sheet_name: 工作表名稱
        return: 工作表對象
        """
        return self.client.open_by_url(sheet_url).worksheet(sheet_name)

    def get_current_date(self):
        """"
        獲取當前日期
        """
        return self.date_worksheet.acell(self.config['current_date_cell']).value

    def extract_and_filter_data(self):
        """"
        撈取並過濾數據
        """
        raw_data = extract_data(self.config['data_list'])
        current_date = self.get_current_date()
        return filter_data(raw_data, current_date)

    def clean_data(self, filtered_data_list):
        """
        資料清理，將NaN替換成0。

        param filtered_data_list: 已透過日期篩選出的資料陣列，each element is of DataFrame type
        return data_without_NaN: 不含NaN或是0的資料
        """
        # Replace NaN with 0
        data_without_NaN = [element.fillna(0) for element in filtered_data_list] # data_without_NaN: list. element: DataFrame
    
        return data_without_NaN

    def clear_and_update_sheet(self, data):
        self.output_worksheet.batch_clear([self.config['clear_cell_range']])
        write_to_sheet(data, self.output_worksheet, self.config['raw_data_cell'])

    def run_all_overview(self):
        """
        執行所有方法
        """
        current_date = self.get_current_date() # 獲取當前日期
        filtered_data = self.extract_and_filter_data() # Extract and filter data
        cleaned_data = self.clean_data(filtered_data) # Clean data
        self.clear_and_update_sheet(cleaned_data) # Update data to Google Sheet
        







