from datetime import date
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from sqlalchemy import create_engine
from Modules import extract_data, filter_data, write_to_sheet, GoogleSheetProcessor, authenticate_google_sheets, transpose_data
from Calculation import Contribution_Calculation
# from Calculation import clean_data
from data_SQLquery_list import Operation_data_list, User_data_list_week, Active_User_week

# 設定參數
scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"] # 認證範圍: Google Sheet, Google Drive
api_key_path = r'C:\Users\user1\Desktop\Cmoney\PythonProject\營運數據自動化\GoogleSheet\admob-autoupdate-66e985563cab.json' #已改
client = authenticate_google_sheets(api_key_path, scopes)

# # 用戶數據總覽(週)_更新
# config = {
#     'date_sheet_url': 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=0#gid=0',
#     'date_sheet_name': '用戶數據總覽(週)',
#     'output_sheet_url': 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=1388457908#gid=1388457908',
#     'output_sheet_name': 'raw_data',
#     'current_date_cell': 'B2',
#     'raw_data_cell': 'A2',
#     'data_list': Operation_data_list,
#     'clear_cell_range': 'A2:D100'
# }

# # 用戶數據_概覽
# processor = GoogleSheetProcessor(client, config)
# processor.run_all_overview()

# #-----------------------------------------------------------------------------------------------------------------------------------------------------------------

# # 用戶數據_產品貢獻度排序表格
# config_userdata = {
#     'date_sheet_url': 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=0#gid=0',
#     'date_sheet_name': '用戶數據總覽(週)',
#     'output_sheet_url': 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=2062253493#gid=2062253493',
#     'output_sheet_name': '用戶數據_raw_data',
#     'current_date_cell': 'B2',
#     'raw_data_cell': 'A2',
#     'data_list': User_data_list_week,
#     'clear_cell_range': 'A3:G100'
# }

# scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"] # 認證範圍: Google Sheet, Google Drive
# api_key_path = r'C:\Users\user1\Desktop\Cmoney\PythonProject\營運數據自動化\GoogleSheet\admob-autoupdate-66e985563cab.json' #已改

# # client = authenticate_google_sheets(api_key_path, scopes)
# processor_contribution = GoogleSheetProcessor(client, config_userdata)

# # 提取+清理數據
# fiiltered_data_1 = processor_contribution.extract_and_filter_data()
# cleaned_data_1 = processor_contribution.clean_data(fiiltered_data_1)
# # print(type(cleaned_data_1[0])) #list

# # 將清理過的數據傳遞給 Contribution_Calculation class
# p2 = Contribution_Calculation(cleaned_data_1)
# p2_1 = p2.Contrbution_Ranking() # 成功撈取前、後10個排名的軟體
# # print(type(p2_1[1])) # DataFrame
# # print(type(p2_1)) # list

# # 在Google Sheets上更新資料
# processor_contribution.clear_and_update_sheet(p2_1)

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------
# 週活躍數
config_active_week = {
    'date_sheet_url': 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=0#gid=0',
    'date_sheet_name': '用戶數據總覽(週)',
    'output_sheet_url': 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=2100891440#gid=2100891440',
    'output_sheet_name': '週活躍數_raw_data',
    'current_date_cell': 'B2',
    'raw_data_cell': 'A3',
    'data_list': Active_User_week,
    'clear_cell_range': 'A3:AA1000'
}

scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"] # 認證範圍: Google Sheet, Google Drive
api_key_path = r'C:\Users\user1\Desktop\Cmoney\PythonProject\營運數據自動化\GoogleSheet\admob-autoupdate-66e985563cab.json' #已改

# client_2 = authenticate_google_sheets(api_key_path, scopes)
processor_ActiveUser_week = GoogleSheetProcessor(client, config_active_week)

# 撈取 25 週的週活躍資料 + 差額
Active_data_25week = extract_data(Active_User_week)
ActiveData_week = transpose_data(Active_data_25week)
print(ActiveData_week)

# print(type(ActiveData_week)) # list
# print(type(ActiveData_week[0])) # DataFrame

# 全刪寫
# processor_ActiveUser_week = GoogleSheetProcessor(client, config_active_week)
# processor_ActiveUser_week.clear_and_update_sheet(ActiveData_week)




# ------備

# sheet_url = 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=0#gid=0' #已改
# sheet_name = '用戶數據總覽(週)'

# sheet_url2 = 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=1388457908#gid=1388457908' #已改
# sheet_name2 = 'raw_data'

# # 認證並獲取工作表
# client = authenticate_google_sheets(api_key_path, scopes)
# worksheet = get_sheet(client, sheet_url, sheet_name)
# worksheet2 = get_sheet(client, sheet_url2, sheet_name2)
# current_date_cell = 'B2'
# raw_data_cell = 'A2'

# # 獲取日期單元格的值_本週
# current_date = get_cell_value(worksheet, current_date_cell)

# # 撈取SQL資料、只取相關日期的資料
# raw_data = extract_data(Operation_data_list)
# current_filtered_data = filter_data(raw_data, current_date)
# """ # print(filtered_data)
# # print(type(filtered_data)) # list
# # print(type(filtered_data[0])) # DataFrame """

# # 全刪寫資料至 Google Sheet
# clear_sheet(worksheet2, "A2", "D100")
# write_to_sheet(current_filtered_data, worksheet2, raw_data_cell)

# process_data_and_update_sheet(client, sheet_url, sheet_name, sheet_url2, sheet_name2, api_key_path, scopes, current_date_cell, raw_data_cell, Operation_data_list, "A2", "D100")



