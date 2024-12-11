from datetime import date
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from sqlalchemy import create_engine
from Modules import extract_data, filter_data, write_to_sheet, GoogleSheetProcessor, authenticate_google_sheets, transpose_data_prdline, transpose_data_lifecycle
from Calculation import Contribution_Calculation
# from Calculation import clean_data
from data_SQLquery_list import Operation_data_list, User_data_list_week, Active_User_week, Active_User_month, APP_Session_week, Reg_week

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

# #-----------------------------------------------------------------------------------------------------------------------------------------------------------------
# # 週活躍數_prdlinename
# config_active_week_prd = {
#     'date_sheet_url': 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=0#gid=0',
#     'date_sheet_name': '用戶數據總覽(週)',
#     'output_sheet_url': 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=2100891440#gid=2100891440',
#     'output_sheet_name': '週活躍數_raw_data',
#     'current_date_cell': 'B2',
#     'raw_data_cell': 'A3',
#     'data_list': Active_User_week,
#     'clear_cell_range': 'A3:Z1000'
# }
# processor_ActiveUser_week = GoogleSheetProcessor(client, config_active_week_prd)

# # 撈取 25 週的週活躍資料 + 差額
# Active_data_25week = extract_data(Active_User_week)
# ActiveData_week = transpose_data_prdline(Active_data_25week)

# # print(ActiveData_week)
# # print(type(ActiveData_week)) # list
# # print(type(ActiveData_week[0])) # DataFrame

# # 全刪寫
# processor_ActiveUser_week = GoogleSheetProcessor(client, config_active_week_prd)
# processor_ActiveUser_week.clear_and_update_sheet(ActiveData_week)

# #-----------------------------------------------------------------------------------------------------------------------------------------------------------------
# # 週活躍數_生命週期
# config_active_week_life = {
#     'date_sheet_url': 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=0#gid=0',
#     'date_sheet_name': '用戶數據總覽(週)',
#     'output_sheet_url': 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=2100891440#gid=2100891440',
#     'output_sheet_name': '週活躍數_raw_data',
#     'current_date_cell': 'B2',
#     'raw_data_cell': 'AF3',
#     'data_list': Active_User_week,
#     'clear_cell_range': 'AF3:AJ1000'
# }

# # 撈取 25 週的週活躍資料 + 差額
# Active_data_25week_life = extract_data(Active_User_week)
# # print(type(Active_data_25week_life)) # list
# t = transpose_data_lifecycle(Active_data_25week_life)
# # print(t) # List
# # print(type(t[0])) # DataFrame

# # 全刪寫
# processor_ActiveUser_week_life = GoogleSheetProcessor(client, config_active_week_life)
# processor_ActiveUser_week_life.clear_and_update_sheet(t)

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------
# # 週活躍數_prdlinename
# config_active_week_prd = {
#     'date_sheet_url': 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=0#gid=0',
#     'date_sheet_name': '用戶數據總覽(週)',
#     'output_sheet_url': 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=2100891440#gid=2100891440',
#     'output_sheet_name': '週活躍數_raw_data',
#     'current_date_cell': 'B2',
#     'raw_data_cell': 'A3',
#     'data_list': Active_User_week,
#     'clear_cell_range': 'A3:Z1000'
# }
# processor_ActiveUser_week = GoogleSheetProcessor(client, config_active_week_prd)

# # 撈取 25 週的週活躍資料 + 差額
# Active_data_25week = extract_data(Active_User_week)
# ActiveData_week = transpose_data_prdline(Active_data_25week, 'current_active_user')

# # print(ActiveData_week)
# # print(type(ActiveData_week)) # list
# # print(type(ActiveData_week[0])) # DataFrame

# # 全刪寫
# processor_ActiveUser_week = GoogleSheetProcessor(client, config_active_week_prd)
# processor_ActiveUser_week.clear_and_update_sheet(ActiveData_week)

# #-----------------------------------------------------------------------------------------------------------------------------------------------------------------
# # 月活躍數_生命週期
# config_active_month_life = {
#     'date_sheet_url': 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=0#gid=0',
#     'date_sheet_name': '用戶數據總覽(週)',
#     'output_sheet_url': 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=439610448#gid=439610448',
#     'output_sheet_name': '月活躍數_raw_data',
#     'current_date_cell': 'B2',
#     'raw_data_cell': 'AF3',
#     'data_list': Active_User_month,
#     'clear_cell_range': 'AF3:AJ1000'
# }

# # 撈取 25 週的週活躍資料 + 差額
# Active_data_25month_life = extract_data(Active_User_month)
# # print(type(Active_data_25week_life)) # list
# t_month = transpose_data_lifecycle(Active_data_25month_life)
# print(t_month) # List
# # print(type(t[0])) # DataFrame

# # 全刪寫
# processor_ActiveUser_week_life = GoogleSheetProcessor(client, config_active_month_life)
# processor_ActiveUser_week_life.clear_and_update_sheet(t_month)

# #-----------------------------------------------------------------------------------------------------------------------------------------------------------------
# # 週造訪數_prdlinename
# config_appsession_week_prd = {
#     'date_sheet_url': 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=0#gid=0',
#     'date_sheet_name': '用戶數據總覽(週)',
#     'output_sheet_url': 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=1810486565#gid=1810486565',
#     'output_sheet_name': '週造訪數_raw_data',
#     'current_date_cell': 'B2',
#     'raw_data_cell': 'A3',
#     'data_list': APP_Session_week,
#     'clear_cell_range': 'A3:Z1000'
# }
# processor_AppSession_week = GoogleSheetProcessor(client, config_appsession_week_prd)

# # 撈取 25 週的週活躍資料 + 差額
# Appsession_data_25week = extract_data(APP_Session_week)
# AppSessionData_week = transpose_data_prdline(Appsession_data_25week, 'current_appsession')

# # 全刪寫
# processor_AppSession_week = GoogleSheetProcessor(client, config_appsession_week_prd)
# processor_AppSession_week.clear_and_update_sheet(AppSessionData_week)

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------
# transpose_data_prdline 有 bug
# 週註冊數_prdlinename
config_reg_week_prd = {
    'date_sheet_url': 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=0#gid=0',
    'date_sheet_name': '用戶數據總覽(週)',
    'output_sheet_url': 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=34342962#gid=34342962',
    'output_sheet_name': '週註冊數_raw_data',
    'current_date_cell': 'B2',
    'raw_data_cell': 'A3',
    'data_list': Reg_week,
    'clear_cell_range': 'A3:Z1000'
}
processor_Reg_week = GoogleSheetProcessor(client, config_reg_week_prd)

# 撈取 25 週的週活躍資料 + 差額
Reg_data_25week = extract_data(Reg_week)
RegData_week = transpose_data_prdline(Reg_data_25week, 'current_reg')
print(RegData_week)

# # 全刪寫
# processor_Reg_week = GoogleSheetProcessor(client, config_reg_week_prd)
# processor_Reg_week.clear_and_update_sheet(RegData_week)


