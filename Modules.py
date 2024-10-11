from datetime import date
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from sqlalchemy import create_engine

# 日期資料轉換、資料寫入 Google Sheets、filter、sheet
#filter_data有問題!!!

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
    sheet = client.open_by_url(sheet_url).worksheet('用戶數據總覽(週)')
    return sheet

def get_cell_value(sheet, cell):
    """
    獲取指定單元格的值。
    
    param sheet: 工作表對象
    param cell: 單元格位置
    return: 單元格的值
    """
    return sheet.acell(cell).value



def format_date(data):
    formatted_data = []
    for row in data[1:]:
        formatted_row = []
        for item in row:
            if isinstance(item, date):  # 檢查是否為日期類型
                formatted_row.append(item.strftime('%Y-%m-%d'))  # 格式化日期為字串
            else:
                formatted_row.append(item)  # 非日期則保留為原始資料
        formatted_data.append(formatted_row)
    return formatted_data


def filter_data(data):
    """"
    目的: 過濾資料

    data (list of lists): 要過濾的資料
    b1_value (float): 用於過濾的值

    return: 過濾後的資料 list
    """
    filtered_data = [row for row in data if row[0] == b1_value]
    # print('filtered_data', filtered_data) # Debugging
    return filtered_data

def write_to_sheet(sheet, data):
    sheet.batch_clear(['A2:B'])  # 清除現有的資料
    # 設定標題(縱向)
    headers = [['日期'], ['成交金額(總計)'], ['交易天數'], ['成交金額(日均)'], ['活躍數'], ['活躍數(金融)'], ['造訪數'], ['註冊數']]
    sheet.update('A2:A', headers)

    # 資料處理(日期、其他資料)
    formatted_data = format_date(data)
    # print("Formatted Data:", formatted_data) # Debugging line OK

    # Filtered data
    filtered_data = filter_data(formatted_data)

    # if filtered_data:
    #     # 只提取成交金額
    #     value = [row[1:] for row in filtered_data]
    #     sheet.update('B3:C3', value)
    #     print('Data written to sheet:', value)  # Debugging line)
    # else:
    #     print('No matching data found for B1 value:', b1_value)

def main():
    data = [
        [1, 'A', 'B'],
        [2, 'C', 'D'],
        [1, 'E', 'F']
    ]
      

    # 過濾資料
    filtered_data = filter_data(data)
    print(filtered_data)

b1_value = 1  
# main()