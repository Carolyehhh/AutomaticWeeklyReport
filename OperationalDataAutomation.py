from datetime import date
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from sqlalchemy import create_engine
from Modules import authenticate_google_sheets, get_sheet, get_cell_value, format_date, filter_data, write_to_sheet

# 設定參數
scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
api_key_path = r'C:\Users\user1\Desktop\Cmoney\PythonProject\營運數據自動化\GoogleSheet\admob-autoupdate-66e985563cab.json' #已改
sheet_url = 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=0#gid=0' #已改
sheet_name = '用戶數據總覽(週)'
date_cell = 'B1'

# 認證並獲取工作表
client = authenticate_google_sheets(api_key_path, scopes)
sheet = get_sheet(client, sheet_url, sheet_name)

# 獲取日期單元格的值
b1_value = get_cell_value(sheet, date_cell)
# print(b1_value) # 2024-10-07

def connect_and_update_sheets():

    try: # try, except 處理異常，發生異常時直接跳到 except，不執行完其餘部分
        print("GSQLAlchemy 連接 MSSQL 資料庫")
        # 使用 SQLAlchemy 連接 MSSQL 資料庫
        engine = create_engine('mssql+pymssql://carol_yeh:Cmoneywork1102@192.168.121.50/master') #已改
        print("GSQLAlchemy 連接.success")  
        
        # print("Google Sheets API 驗證")
        # # 定義 Google Sheets API 驗證
        # scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        # api_key_path = r'C:\Users\user1\Desktop\Cmoney\PythonProject\營運數據自動化\GoogleSheet\admob-autoupdate-66e985563cab.json' #已改
        # credentials = Credentials.from_service_account_file(api_key_path, scopes=scopes)
        # client = gspread.authorize(credentials)
        # sheetUrl = 'https://docs.google.com/spreadsheets/d/1gSbdB-JhykNk88-6yOD9pB0pbC3QrhkvpXSmpP3Dse4/edit?gid=0#gid=0' #已改
        # print("Google Sheets API.success")
        

        print("step1.stat")
        # 營運數據查詢
        OverallUserData_sql = """
            select
                日期,
                --[週成交金額(億)],
                format(round([週成交金額(億)]/10000, 2), 'N2')　+ ' 兆' as '週成交金額(兆)',
                --[上週成交金額(億)],
                case 
                    when [上週成交金額(億)]=0 then 'N/A'
                    else format(round(([週成交金額(億)] - [上週成交金額(億)])/[上週成交金額(億)] * 100, 1), 'N2') + '%' 
                    end as '成長率'
            from (
                SELECT 
                    [Ddate] as '日期'
                    ,[週成交金額(億)]
                    , lag([週成交金額(億)]) over (order by ddate) as '上週成交金額(億)'
                FROM [CMAPP].[dbo].[View_TWA00_Info]
                where DATEPART(weekday, ddate)=2
            ) a
            Order by 日期 desc
        """
        OverallUserData = pd.read_sql(OverallUserData_sql, engine)
        print("Extracted data:", OverallUserData) # Debugging line
        data = [OverallUserData.columns.values.tolist()] + OverallUserData.values.tolist()

        # 寫入第一個工作表
        # sheet = client.open_by_url(sheet_url).worksheet('用戶數據總覽(週)')
        # write_to_sheet(sheet, data)
        # print("step1.success")

        print('data 0', data[1][0])
        print('[0]', OverallUserData.iloc[0][0])
        f = filter_data(OverallUserData)
        print('F:', f)

        # print("step2.stat")
        # Trading_Days_sql = """
        #     select convert(nvarchar(8), dateadd(dd, -datediff(dy, 0, 日期)%7, 日期), 112) as wdate, count(distinct 日期) as 交易天數
        #     from CMServer.Northwind.dbo.sysdbase
        #     where 日期>='20230101'
        #     group by convert(nvarchar(8), dateadd(dd, -datediff(dy, 0, 日期)%7, 日期), 112)
        #     order by  convert(nvarchar(8), dateadd(dd, -datediff(dy, 0, 日期)%7, 日期), 112)
        # """
        # Trading_Days = pd.read_sql(Trading_Days_sql, engine)
        # print("Trading Days", Trading_Days) # Debugging lind
        # data2 = [Trading_Days.columns.values.tolist()] + Trading_Days.values.tolist()
        # print(data2) # Debugging lind

        # # 寫入工作表
        # sheet = client.open_by_url(sheet_url).worksheet('用戶數據總覽(週)')
        # write_to_sheet(sheet, data2)
        # print("step2.success")

        # 待  
        # ActiveUser_sql = """
        #     select [日期], [產品線], sum([總活躍用戶]) as active_usess
        #     FROM [DataViews].[dbo].[週活躍用戶數(平台+付費+生命週期)]
        #     where [產品線] in ('Money錢', 'X實驗室', '大眾', '同學會', '作者', '社群', '記帳', '發票', '網紅')
        #     group by [日期], [產品線]
        # """
        # ActiveUser = pd.read_sql(ActiveUser_sql, engine)
        # print("ActiveUser:", ActiveUser) # Debugging line
        # data = [ActiveUser.columns.values.tolist()] + ActiveUser.values.tolist()

        # # 寫入工作表
        # sheet = client.open_by_url(sheet_url).worksheet('用戶數據總覽(週)')
        # write_to_sheet(sheet, data)
        # print("step1.success")
    except Exception as e:
        print(f"An error occured: {e}")

    finally:
        engine.dispose()  # 關閉 SQLAlchemy 連接

connect_and_update_sheets()



