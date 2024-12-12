from Modules import extract_data, filter_data, write_to_sheet, GoogleSheetProcessor, authenticate_google_sheets, transpose_data_prdline, transpose_data_lifecycle
from data_SQLquery_list import Operation_data_list, User_data_list_week, Active_User_week, Active_User_month, APP_Session_week, Reg_week
from Calculation import Contribution_Calculation

# 認證範圍與 API 金鑰
scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"] # 認證範圍: Google Sheet, Google Drive
api_key_path = r'C:\Users\user1\Desktop\Cmoney\PythonProject\營運數據自動化\GoogleSheet\admob-autoupdate-66e985563cab.json' #已改
client = authenticate_google_sheets(api_key_path, scopes)

# 共用的設定模板
def create_config(data_list, output_sheet_name, output_gid, input_cell, clear_cell_range):
    return {
    'date_sheet_url': 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=0#gid=0',
    'date_sheet_name': '用戶數據總覽(週)',
    'output_sheet_url': 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid={output_gid}#gid={output_gid}',
    'output_sheet_name': output_sheet_name,
    'current_date_cell': 'B2',
    'raw_data_cell': input_cell,
    'data_list': data_list,
    'clear_cell_range': clear_cell_range
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

# 依產品線分類
def process_data(config, transpose_key=None, mode='prdline'): # 將其預設為 None，允許該參數在不需要時被省略，避免必須提供不必要的值。
    # 撈取資料
    raw_data = extract_data(config['data_list'])

    if mode == 'prdline':
        processed_data = transpose_data_prdline(raw_data, transpose_key)
    elif mode == 'lifecycle':
        processed_data = transpose_data_lifecycle(raw_data)
    else:
        raise ValueError("Invalid mode. Use 'prdline' or 'lifecycle'.")

    # 寫入 Google Sheet
    processor = GoogleSheetProcessor(client, config)
    processor.clear_and_update_sheet(processed_data)

def process_overview_data(config):
    processor = GoogleSheetProcessor(client, config)
    processor.run_all_overview()

def process_contribution_data(config):
    processor = GoogleSheetProcessor(client, config)
    filtered_data = processor.extract_and_filter_data()
    cleaned_data = processor.clean_data(filtered_data)

    # 計算貢獻度排名
    contribution_calculator = Contribution_Calculation(cleaned_data)
    ranked_data = contribution_calculator.Contrbution_Ranking()

    # 寫入 Google Sheet
    processor.clear_and_update_sheet(ranked_data)

def main():
    # 所有配置清單
    configs = [
        # 產品線
        {"data_list":APP_Session_week, "output_sheet_name":'週造訪數_raw_data', "output_gid":1810486565, "transpose_key":'current_appsession', "input_cell":'A3', "clear_cell_range":'A3:Z1000', "mode":'prdline'}, 
        {"data_list":Reg_week, "output_sheet_name":'週註冊數_raw_data', "output_gid":34342962, "transpose_key":'current_reg', "input_cell":'A3', "clear_cell_range":'A3:Z1000', "mode":'prdline'},
        {"data_list":Active_User_week, "output_sheet_name":'週活躍數_raw_data', "output_gid":2100891440, "transpose_key":'current_active_user', "input_cell":'A3', "clear_cell_range":'A3:Z1000', "mode":'prdline'},
        # 生命週期
        {"data_list":Active_User_week, "output_sheet_name":'週活躍數_raw_data', "output_gid":2100891440, "transpose_key":None, "input_cell":'AF3', "clear_cell_range":'AF3:AJ1000', "mode":'lifecycle'},
        {"data_list":Active_User_month, "output_sheet_name":'月活躍數_raw_data', "output_gid":439610448, "transpose_key":None, "input_cell":'AF3', "clear_cell_range":'AF3:AJ1000', "mode":'lifecycle'}
    ]

    # 依次處理每個配置, list  of dictionaries
    for conf in configs:
        config = create_config(conf["data_list"], conf["output_sheet_name"], conf["output_gid"], conf["input_cell"], conf["clear_cell_range"])
        process_data(config, conf.get("transpose_key"), conf["mode"]) # 使用 get 方法則能在鍵缺失時返回 None（或其他指定的預設值），這樣程式不會崩潰。

    # 用戶數據總覽
    process_overview_data(get_overview_config())

    # 用戶數據貢獻度
    process_contribution_data(get_contribution_config())

if __name__=="__main__":
    main()