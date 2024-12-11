# ------備

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
raw_data = extract_data(Operation_data_list)
current_filtered_data = filter_data(raw_data, current_date)
""" # print(filtered_data)
# print(type(filtered_data)) # list
# print(type(filtered_data[0])) # DataFrame """

# 全刪寫資料至 Google Sheet
clear_sheet(worksheet2, "A2", "D100")
write_to_sheet(current_filtered_data, worksheet2, raw_data_cell)

process_data_and_update_sheet(client, sheet_url, sheet_name, sheet_url2, sheet_name2, api_key_path, scopes, current_date_cell, raw_data_cell, Operation_data_list, "A2", "D100")



