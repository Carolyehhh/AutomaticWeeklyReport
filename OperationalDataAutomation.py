from datetime import date
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from sqlalchemy import create_engine
from Modules import authenticate_google_sheets, get_sheet, get_cell_value, Connect_to_MSSQL, extract_data, filter_data
from data_SQLquery_list import data_list

# 設定參數
scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"] # 認證範圍: Google Sheet, Google Drive
api_key_path = r'C:\Users\user1\Desktop\Cmoney\PythonProject\營運數據自動化\GoogleSheet\admob-autoupdate-66e985563cab.json' #已改
sheet_url = 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=0#gid=0' #已改
sheet_name = '用戶數據總覽(週)'

# 認證並獲取工作表
client = authenticate_google_sheets(api_key_path, scopes)
sheet = get_sheet(client, sheet_url, sheet_name)
cell = 'B1'

# 獲取日期單元格的值
date_value = get_cell_value(sheet, cell)
raw_data = extract_data(data_list)
filtered_data = filter_data(raw_data, date_value)
print(filtered_data)


# def connect_and_update_sheets():

#     try: # try, except 處理異常，發生異常時直接跳到 except，不執行完其餘部分      

#         # 寫入工作表
#         sheet = client.open_by_url(sheet_url).worksheet('用戶數據總覽(週)')
#         write_to_sheet(sheet, data2)
#         print("step2.success")

#     except Exception as e:
#         print(f"An error occured: {e}")

# connect_and_update_sheets()



