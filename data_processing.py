from Modules import extract_data, filter_data, write_to_sheet, GoogleSheetProcessor, authenticate_google_sheets, transpose_data_prdline, transpose_data_lifecycle
from config import create_config, get_overview_config, get_contribution_config
from Calculation import Contribution_Calculation

# 認證範圍與 API 金鑰
scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"] # 認證範圍: Google Sheet, Google Drive
api_key_path = r'C:\Users\user1\Desktop\Cmoney\PythonProject\營運數據自動化\GoogleSheet\admob-autoupdate-66e985563cab.json' #已改
client = authenticate_google_sheets(api_key_path, scopes)

# 資料處理
def process_data(config): 
    # 撈取資料
    raw_data = extract_data(config['data_list'])

    if config['mode'] == 'prdline':
        processed_data = transpose_data_prdline(raw_data, config['transpose_key'])
    elif config['mode'] == 'lifecycle':
        processed_data = transpose_data_lifecycle(raw_data)
    else:
        raise ValueError("Invalid mode. Use 'prdline' or 'lifecycle'.")

    # 寫入 Google Sheet
    processor = GoogleSheetProcessor(client, config)
    processor.clear_and_update_sheet(processed_data)

# 用戶數據表格(全)
def process_overview():
    config = get_overview_config()
    processor = GoogleSheetProcessor(client, config)
    processor.run_all_overview()

# 貢獻度排名計算
def process_contribution():
    config = get_contribution_config()
    processor = GoogleSheetProcessor(client, config)
    filtered_data = processor.extract_and_filter_data()
    cleaned_data = processor.clean_data(filtered_data)

    # 計算貢獻度排名
    contribution_calculator = Contribution_Calculation(cleaned_data)
    ranked_data = contribution_calculator.Contrbution_Ranking()

    # 寫入 Google Sheet
    processor.clear_and_update_sheet(ranked_data)