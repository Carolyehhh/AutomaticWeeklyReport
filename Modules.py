from datetime import date
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from sqlalchemy import create_engine
import re

# 日期資料轉換、資料寫入 Google Sheets、filter、sheet!

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

def get_sheet(client, sheet_url, sheet_name):
    """
    獲取 Google Sheets 的工作表對象。
    
    param client: gspread 客戶端對象
    param sheet_url: Google Sheets 的 URL
    param sheet_name: 工作表名稱
    return: 工作表對象
    """
    worksheet = client.open_by_url(sheet_url).worksheet(sheet_name)
    return worksheet

def get_cell_value(worksheet, cell):
    """
    獲取指定單元格的值。
    
    param sheet: 工作表對象，gspread.models.Worksheet
    param cell: 單元格位置
    return: 單元格的值
    """
    return worksheet.acell(cell).value

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

def clear_sheet(worksheet, start_cell, end_cell):
    """
    清除所選欄位的現有資料。
    
    param sheet: 工作表對象，gspread.models.Worksheet
    param start_cell(str): 清除起始格
    param end_cell(str): 清除結束格
    """

    range_notation = f"{start_cell}:{end_cell}"

    return worksheet.batch_clear([range_notation])

def hearders_to_sheet(worksheet, headers, start_cell):
    """
    ***縱向寫入***
    將標頭寫入工作表的指定起始格。

    param sheet: 工作表對象，gspread.models.Worksheet
    param headers: 標頭列表
    param start_cell: 起始單元格，例如 'A2'
    """
    # 將 start_cell 切割成數字及文字
    match = re.match(r"([a-z]+)([0-9]+)", start_cell, re.I)
    if match:
        items = match.groups()
    else:
        items = None
    # print(items[0], items[1]) 
    # print(type(items[0]), type(items[1])) #str

    # 取start_cell位置的數字
    int_start_cell = int(items[1])

    # 計算end_cell位置
    int_end_cell = int_start_cell + len(headers) - 1

    # end_cell 轉換成 str type
    end_cell = items[0] + str(int_end_cell)
    # print(end_cell)
  
    return worksheet.update(f"{start_cell}:{end_cell}", headers)


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



# ----------------------------------------------------------------------------------------------
# # 操作測試
# # 設定參數
# scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"] # 認證範圍: Google Sheet, Google Drive
# api_key_path = r'C:\Users\user1\Desktop\Cmoney\PythonProject\營運數據自動化\GoogleSheet\admob-autoupdate-66e985563cab.json' #已改
# sheet_url = 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=0#gid=0' #已改
# sheet_name = '用戶數據總覽(週)'

# client = authenticate_google_sheets(api_key_path, scopes)
# sheet = get_sheet(client, sheet_url, sheet_name)
# cell = 'B1'
# b1_value = get_cell_value(sheet, cell)
# # print(b1_value)

# rd = extract_data(data_list)
# filtered_data = filter_data(rd, b1_value)
# print(filtered_data)






