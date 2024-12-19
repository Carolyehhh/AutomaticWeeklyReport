# 專門處理訂戶資料
import pandas as pd
from Modules import Connect_to_MSSQL, authenticate_google_sheets
from sqlalchemy import create_engine
from data_SQLquery_list import test_SQL

scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"] # 認證範圍: Google Sheet, Google Drive
api_key_path = r'C:\Users\user1\Desktop\Cmoney\PythonProject\營運數據自動化\GoogleSheet\admob-autoupdate-66e985563cab.json' #已改
client = authenticate_google_sheets(api_key_path, scopes)

class Process_SubData:  

    def __init__(self, config):
        self.config = config  # 存储配置供其他方法使用

    def get_config(self):
        return self.config

    def fetch_SubData(self):
        engine = Connect_to_MSSQL()
        if engine is not None:
            insert_data = []
            for query in self.config['data_list']:
                try: 
                    read_data = pd.read_sql(query, engine)
                    insert_data.append(read_data)
                except Exception as e:
                    print(f"讀取數據時發生錯誤: {e}")
            engine.dispose()
        else:
            print("無法連接到資料庫")
            return None
        return [df.values.tolist() for df in insert_data if not df.empty]
    

class GoogleSheetPrs_SubData:
    def __init__(self, client, config):
        self.client = client 
        self.config = config
        # self.date_worksheet = self.get_sheet(config['date_sheet_url'], config['date_sheet_name'])
        self.output_worksheet = self.get_sheet(config['output_sheet_url'], config['output_sheet_name'])
        
    def get_sheet(self, sheet_url, sheet_name):
        """
        通過 API 指定到單一作業簿的位置

        param sheet_url: Google Sheets 的 URL
        param sheet_name: 工作表名稱
        return: 工作表對象
        """
        return self.client.open_by_url(sheet_url).worksheet(sheet_name)

    def clear_and_update_sheet(self, insert_data):
        """
        清除舊資料、填入新資料

        """
        # 清理部分範圍 => OK
        self.output_worksheet.batch_clear([self.config['clear_cell_range']])
        


        # self.output_worksheet.update(range_name=self.config['input_cell'], values=insert_data)

# Testing
config = {"data_list":test_SQL, "output_sheet_name":'test', "output_sheet_url":'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?pli=1&gid=299852225#gid=299852225', "transpose_key":'current_counts', "input_cell":'A35', "clear_cell_range":'A35:A36'}

t = Process_SubData(config)
google_test = GoogleSheetPrs_SubData(client, config)
data = t.fetch_SubData()
google_test.clear_and_update_sheet(data)


# print(t.fetch_SubData())





 