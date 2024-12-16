from Modules import extract_data, filter_data, write_to_sheet, GoogleSheetProcessor, authenticate_google_sheets, transpose_data_prdline, transpose_data_lifecycle_finance
from data_SQLquery_list import Active_User_month
from config import create_config, get_overview_config, get_contribution_config
from data_processing import process_data
from Calculation import Contribution_Calculation


def main():
    # 所有配置清單
    configs = [
        # 產品線
        # 生命週期
        {"data_list":Active_User_month, "output_sheet_name":'月活躍數_raw_data', "output_gid":439610448, "transpose_key":None, "input_cell":'AF3', "clear_cell_range":'AF3:AJ1000', "mode":'lifecycle_finance'}
    ]

    # 依次處理每個配置, list  of dictionaries
    for conf in configs:
        config = create_config(
            # 使用 get 方法則能在鍵缺失時返回 None（或其他指定的預設值），這樣程式不會崩潰。
            conf["data_list"], conf["output_sheet_name"], conf["output_gid"], conf["input_cell"], conf["clear_cell_range"], conf["mode"], conf.get("transpose_key")
            )
        process_data(config)

if __name__=="__main__":
    main()