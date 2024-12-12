from Modules import extract_data, filter_data, write_to_sheet, GoogleSheetProcessor, authenticate_google_sheets, transpose_data_prdline, transpose_data_lifecycle
from data_SQLquery_list import Operation_data_list, User_data_list_week, Active_User_week, Active_User_month, APP_Session_week, Reg_week
from config import create_config, get_overview_config, get_contribution_config
from data_processing import process_data, process_overview, process_contribution
from Calculation import Contribution_Calculation


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
        config = create_config(
            # 使用 get 方法則能在鍵缺失時返回 None（或其他指定的預設值），這樣程式不會崩潰。
            conf["data_list"], conf["output_sheet_name"], conf["output_gid"], conf["input_cell"], conf["clear_cell_range"], conf["mode"], conf.get("transpose_key")
            )
        process_data(config) 

    # 用戶數據總覽
    process_overview()

    # 用戶數據貢獻度
    process_contribution()

if __name__=="__main__":
    main()