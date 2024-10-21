from datetime import date
import pandas as pd
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
    for element in data:
        data_to_list = element.values.tolist()
        all_data.extend(data_to_list)

    # # 把資料轉成list才能寫入Google Sheet
    # data_list = data.values.tolist()

    return worksheet.update(cell, all_data)


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

    # def process_data(self):
    #     """
    #     處理數據並更新工作表
    #     """
    #     filtered_data = self.extract_and_filter_data()
    #     data_without_NaN = self.clean_data(filtered_data)
    #     self.clear_and_update_sheet(data_without_NaN)

    def run_all(self):
        """
        執行所有方法
        """
        current_date = self.get_current_date() # 獲取當前日期
        filtered_data = self.extract_and_filter_data() # Extract and filter data
        cleaned_data = self.clean_data(filtered_data) # Clean data
        self.clear_and_update_sheet(cleaned_data) # Update data to Google Sheet
        







