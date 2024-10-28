from datetime import date
import math
import pandas as pd
from sqlalchemy import create_engine
from Modules import extract_data, filter_data, write_to_sheet, GoogleSheetProcessor, authenticate_google_sheets
from data_SQLquery_list import Operation_data_list, User_data_list_week


class Contribution_Calculation:
    def __init__(self, data):
        """"
        初始化類
        param data: list with DataFrame elements
        """
        self.data = data
        self.windowsum_list = self.window_sum()

    def window_sum(self):
        """
        加總指定日期的所有的差額。
        """
        return [l['difference'].astype(float).sum() for l in self.data] 
    
    def computeWoWAndGrowth(self):
        """
        1. 計算 Week over Week(WoW)。兩週間的變化量。
        2. 計算成長貢獻度
        """
        result_list = []
        for k, winsum in zip(self.data, self.windowsum_list):   
            if 'previous_active' in k.columns:
                previous_column = 'previous_active'
            elif 'previous_appsession' in k.columns:
                previous_column = 'previous_appsession'
            elif 'previous_register' in k.columns:
                previous_column = 'previous_register'
            else:
                print("缺少 'previous_active' 或 'previous_appsession' 或 'previous_register' 列")
                continue   

            # 添加標題行
            new_columms = k.columns.to_list() + ['WoW', 'Contribution']
            title_row = pd.DataFrame([new_columms], columns=new_columms) 
            result_list.append(title_row)

            # 計算WoW + 把NaN替換成0
            k['WoW'] = round(k['difference'].astype(float) / k[previous_column].astype(float) * 100, 1).fillna(0)
            
            # 將 WoW 列轉換成百分比格式
            k['WoW']  = k['WoW'].apply(lambda x: f"{x: .1f}%")

            # 確保 winsum 是 float 類型
            winsum = float(winsum)

            # 計算 Contribution
            if winsum < 0:
                k['Contribution'] = k['difference'].apply(lambda x: round(float(x) / (-winsum) * 100, 2))
            else:
                k['Contribution'] = k['difference'].apply(lambda x: round(float(x) / winsum * 100, 2))
            
            result_list.append(k)
 
        return result_list

    def Contrbution_Ranking(self):
        """
        貢獻度排序: 高->低
        """
        Ranked_Data = []
        # 計算 WoW 和 Contribution
        data_with_growth = self.computeWoWAndGrowth()

        # 對每個 DataFrame 中的貢獻度進行排序
        for df in data_with_growth:
            # large to small
            sorted_contribution = df.sort_values(by=['Contribution'], ascending=False)
            # 將 'Contribution' 列轉換為百分比
            sorted_contribution['Contribution'] = sorted_contribution['Contribution'].astype(str) + '%'
            # 取前十 & 後十
            top_ten = sorted_contribution.head(10)
            bottom_ten = sorted_contribution.tail(10)

            # Ranked_Data.append([top_ten, bottom_ten])
            # Concatement top_ten and bottom_ten into a single DataFrame
            combined_df = pd.concat([top_ten, bottom_ten], ignore_index=True)
            Ranked_Data.append(combined_df)

        return Ranked_Data


# 取資料看看
config = {
    'date_sheet_url': 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=0#gid=0',
    'date_sheet_name': '用戶數據總覽(週)',
    'output_sheet_url': 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=2062253493#gid=2062253493',
    'output_sheet_name': '用戶數據_raw_data',
    'current_date_cell': 'B2',
    'raw_data_cell': 'A2',
    'data_list': User_data_list_week,
    'clear_cell_range': 'A3:G100'
}

scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"] # 認證範圍: Google Sheet, Google Drive
api_key_path = r'C:\Users\user1\Desktop\Cmoney\PythonProject\營運數據自動化\GoogleSheet\admob-autoupdate-66e985563cab.json' #已改

client = authenticate_google_sheets(api_key_path, scopes)
processor_contribution = GoogleSheetProcessor(client, config)

# 提取+清理數據
fiiltered_data_1 = processor_contribution.extract_and_filter_data()
cleaned_data_1 = processor_contribution.clean_data(fiiltered_data_1)

# 將清理過的數據傳遞給 Contribution_Calculation class
p2 = Contribution_Calculation(cleaned_data_1)
p2_1 = p2.Contrbution_Ranking() # 成功撈取前、後10個排名的軟體
# print(type(p2_1[1])) # DataFrame
# print(p2_1[1])
# print(p2_1)
processor_contribution.clear_and_update_sheet(p2_1)
# 標題重複兩次


# -----
# def get_WoW_Stats(cleaned_data):
#     """
#     計算 Week over Week(WoW)。兩週間的變化量。

#     param cleaned_data: 使用日期篩選出來的資料
#     return: 包含處理後 DataFrame 的列表
#     """
#     WoW_list = []
#     for k in cleaned_data:
        
#         if 'previous_active' in k.columns:
#             previous_column = 'previous_active'
#         elif 'previous_appsession' in k.columns:
#             previous_column = 'previous_appsession'
#         else:
#             print("缺少 'previous_active' 或 'previous_appsession' 列")
#             continue
        
#         # 計算WoW + 把NaN替換成0
#         k.loc[:, 'WoW'] = round(k['difference'] / k[previous_column] * 100, 1).fillna(0)
#         # 將 WoW 列轉換成百分比格式
#         k.loc[:, 'WoW']  = k['WoW'].astype(str) + '%'

#         WoW_list.append(k)
        
#     return k

# def window_sum(cleaned_data):
#     """
#     加總指定日期的所有的差額。

#     param cleaned_data: 使用日期篩選出來的資料
#     return: 
#     """
#     winsum = [l['difference'].sum() for l in cleaned_data] 

#     return winsum

# def get_Contribution_Stats(cleaned_data, winsum_list):
#     """
#     計算成長貢獻度 = 單一產品L01 / 所有產品總L01，用以判斷該產品在整體的成長或是衰退，有多大的貢獻度。

#     param cleaned_data: 使用日期篩選出來的資料
#     param winsum: 指定日期的所有的差額總計
#     """
#     Contribution_list = []
#     for j, winsum in zip(cleaned_data, winsum_list): # zip: pair 2 (or more) lists into tuples，j 配上 winsum(一組)
#         j['Contribution'] = j['difference'].apply(lambda x: round(x / winsum * 100, 2))
#         Contribution_list.append(j)
#     return Contribution_list