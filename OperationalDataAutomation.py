from datetime import date
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from sqlalchemy import create_engine
from Modules import authenticate_google_sheets, get_sheet, get_cell_value, Connect_to_MSSQL, extract_data, filter_data

# 設定參數
scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"] # 認證範圍: Google Sheet, Google Drive
api_key_path = r'C:\Users\user1\Desktop\Cmoney\PythonProject\營運數據自動化\GoogleSheet\admob-autoupdate-66e985563cab.json' #已改
sheet_url = 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=0#gid=0' #已改
sheet_name = '用戶數據總覽(週)'

# 營運數據查詢_SQL_query
data_list =["""
    --成交金額
    SELECT
        日期,
        --[週成交金額(億)],
        FORMAT(ROUND([週成交金額(億)] / 10000, 2), 'N2') + ' 兆' AS '週成交金額(兆)',
        --[上週成交金額(億)],
        CASE
            WHEN [上週成交金額(億)] = 0 THEN 'N/A'
            ELSE FORMAT(ROUND(([週成交金額(億)] - [上週成交金額(億)]) / [上週成交金額(億)] * 100, 1), 'N2') + '%'
        END AS '成長率'
    FROM (
        SELECT
            [Ddate] AS '日期',
            [週成交金額(億)],
            LAG([週成交金額(億)]) OVER (ORDER BY Ddate) AS '上週成交金額(億)'
        FROM [CMAPP].[dbo].[View_TWA00_Info]
        WHERE DATEPART(weekday, Ddate) = 2
    ) a
    ORDER BY 日期 DESC
""", 
"""
    --成交天數
    select convert(nvarchar(8), dateadd(dd, -datediff(dy, 0, 日期)%7, 日期), 112) as 日期, count(distinct 日期) as 交易天數
    from CMServer.Northwind.dbo.sysdbase
    where 日期>='20230101'
    group by convert(nvarchar(8), dateadd(dd, -datediff(dy, 0, 日期)%7, 日期), 112)
    order by  convert(nvarchar(8), dateadd(dd, -datediff(dy, 0, 日期)%7, 日期), 112)
""",
""" 
    --活躍用戶
    select [日期], [產品線], sum([總活躍用戶]) as active_usess
    FROM [DataViews].[dbo].[週活躍用戶數(平台+付費+生命週期)]
    where [產品線] in ('Money錢', 'X實驗室', '大眾', '同學會', '作者', '社群', '記帳', '發票', '網紅')
    group by [日期], [產品線]
"""
]

# 認證並獲取工作表
client = authenticate_google_sheets(api_key_path, scopes)
sheet = get_sheet(client, sheet_url, sheet_name)
cell = 'B1'

# 獲取日期單元格的值
date_value = get_cell_value(sheet, cell)
raw_data = extract_data(data_list)
filtered_data = filter_data(raw_data, date_value)
print(filtered_data)


# def connect_and_update_sheets():

#     try: # try, except 處理異常，發生異常時直接跳到 except，不執行完其餘部分      

#         # 寫入工作表
#         sheet = client.open_by_url(sheet_url).worksheet('用戶數據總覽(週)')
#         write_to_sheet(sheet, data2)
#         print("step2.success")

#     except Exception as e:
#         print(f"An error occured: {e}")

# connect_and_update_sheets()



