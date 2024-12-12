from data_SQLquery_list import Operation_data_list, User_data_list_week, Active_User_week, Active_User_month, APP_Session_week, Reg_week

# 共用的設定模板：產生一個標準化的設定"字典" (configuration dictionary)，提供統一的輸入參數格式並避免重複撰寫相似的程式碼
def create_config(data_list, output_sheet_name, output_gid, input_cell, clear_cell_range, mode="default",transpose_key=None):
    # 將其預設為 None，允許該參數在不需要時被省略，避免必須提供不必要的值。
    return {
    'date_sheet_url': 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=0#gid=0',
    'date_sheet_name': '用戶數據總覽(週)',
    'output_sheet_url': 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid={output_gid}#gid={output_gid}',
    'output_sheet_name': output_sheet_name,
    'current_date_cell': 'B2',
    'raw_data_cell': input_cell,
    'data_list': data_list,
    'clear_cell_range': clear_cell_range,
    'mode':mode,
    'transpose_key':transpose_key
}

# 為用戶數據總覽生成專屬的配置
def get_overview_config():
    return create_config(
        data_list = Operation_data_list,
        output_sheet_name = 'raw_data',
        output_gid = 1388457908,
        clear_cell_range = 'A2:D100',
        input_cell = 'A2'
    )
 
# 為用戶數據貢獻度生成專屬的配置
def get_contribution_config():
    return create_config(
        data_list = User_data_list_week,
        output_sheet_name = '用戶數據_raw_data',
        output_gid = 2062253493,
        clear_cell_range = 'A3:G100',
        input_cell = 'A2'
    )