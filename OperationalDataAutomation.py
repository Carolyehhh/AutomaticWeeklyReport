from datetime import date
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from sqlalchemy import create_engine
from Modules import authenticate_google_sheets, get_sheet, get_cell_value, Connect_to_MSSQL, extract_data, filter_data, hearders_to_sheet, write_to_sheet, clear_sheet
from data_SQLquery_list import data_list

# 設定參數
scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"] # 認證範圍: Google Sheet, Google Drive
api_key_path = r'C:\Users\user1\Desktop\Cmoney\PythonProject\營運數據自動化\GoogleSheet\admob-autoupdate-66e985563cab.json' #已改
sheet_url = 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=0#gid=0' #已改
sheet_name = '用戶數據總覽(週)'

sheet_url2 = 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=1388457908#gid=1388457908' #已改
sheet_name2 = 'raw_data'

# 認證並獲取工作表
client = authenticate_google_sheets(api_key_path, scopes)
worksheet = get_sheet(client, sheet_url, sheet_name)
worksheet2 = get_sheet(client, sheet_url2, sheet_name2)
current_date_cell = 'B2'
raw_data_cell = 'A2'

# 獲取日期單元格的值_本週
current_date = get_cell_value(worksheet, current_date_cell)

# 撈取SQL資料、只取相關日期的資料
raw_data = extract_data(data_list)
current_filtered_data = filter_data(raw_data, current_date)
""" # print(filtered_data)
# print(type(filtered_data)) # list
# print(type(filtered_data[0])) # DataFrame """

# 全刪寫資料至 Google Sheet
clear_sheet(worksheet2, "A2", "D100")
write_to_sheet(current_filtered_data, worksheet2, raw_data_cell)





""" # # 獲取日期單元格的值_上週
# previous_date_cell = 'C2'
# previous_date = get_cell_value(worksheet, previous_date_cell)
# previous_filtered_data = filter_data(raw_data, previous_date)

# # Combine previous_filtered_data with current_filtered_data
# all_filtered_data = current_filtered_data + previous_filtered_data
# print(all_filtered_data)

# 將 headers 列表轉換為單列列表
# headers = [['日期'], ['成交金額(總計)'], ['交易天數'], ['成交金額(日均)'], ['活躍數'], ['活躍數(金融)'], ['造訪數'], ['註冊數']]
# hearders_to_sheet(worksheet, headers, "A2")
 """

