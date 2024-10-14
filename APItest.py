from fastapi import FastAPI
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pymssql

# email: operationaldataautomation@tableau-434707.iam.gserviceaccount.com

app = FastAPI()

def Fetch_data_from_sql():
    # Connect to SQL Server
    conn = pymssql.connect(server='192.168.121.50', user='carol_yeh', password='Cmoneywork1102', database='master')
    cursor = conn.cursor(as_dict=True) # 確保以字典形式返回數據
    cursor.execute('SELECT TOP (1000) [Yyear],[Ddate],[週成交金額(億)] FROM [CMAPP].[dbo].[View_TWA00_Info]')
    data = cursor.fetchall()
    conn.close()
    return data

def update_google_sheet(data):
    # 設置 Google Sheets API 憑證
    # scope 在這裡的作用是定義應用程式所需要的訪問權限範圍 (Google Sheets, Google Drive)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"] 
    creds = ServiceAccountCredentials.from_json_keyfile_name(r'C:\Users\user1\Desktop\OperationDataAutomaticJSON.json', scope) #已改
    client = gspread.authorize(creds)
    sheet = client.open('報告自動化').sheet1 

    # 更新Google Sheet
    for i, row in enumerate(data, start=2): # 假設我要從第二行開始寫
        sheet.update(f'A{i}', row['Yyear'])
        sheet.update(f'B{i}', row['Ddate'])
        sheet.update(f'C{i}', row['週成交金額(億)'])

def main():
    print("Access data via MSSQL...")
    data = Fetch_data_from_sql()

    print("Access and Upload to Google Sheet via API...")
    update_google_sheet(data)

@app.get("/") 
# ▲ 用來定義路由。這個路由指定了一個 HTTP GET 請求的端點。在這個例子中，"/" 是根路徑
# 所以當有人訪問你部署的應用的根 URL，例如 https://your_actual_python_api_url/，這個 root 函數就會被調用。
# 根據你的代碼，當訪問 "/" 路徑時，FastAPI 會執行 main() 函數，然後返回一個 JSON 消息，表明任務已經完成

def root():
    main()
    return {"message": "Data Updated successfully"}

# 這行確保當這個腳本被直接執行時，而不是被作為模組導入時，以下的代碼會被運行
# 確保某些代碼段只有在腳本以"主程序"的方式運行時才會執行。
if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)
    # host='0.0.0.0' 時，表示我們希望應用程式接收來自所有網絡接口的請求，而不僅僅是本地主機。

def update_google_sheet(data):
    # 設置 Google Sheets API 憑證
    # scope 在這裡的作用是定義應用程式所需要的訪問權限範圍 (Google Sheets, Google Drive)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"] 
    creds = ServiceAccountCredentials.from_json_keyfile_name(r'C:\Users\user1\Desktop\OperationDataAutomaticJSON.json', scope) #已改
    client = gspread.authorize(creds)
    sheet = client.open('報告自動化').sheet1 

    # 更新Google Sheet
    for i, row in enumerate(data, start=2): # 假設我要從第二行開始寫
        sheet.update(f'A{i}', row['Yyear'])
        sheet.update(f'B{i}', row['Ddate'])
        sheet.update(f'C{i}', row['週成交金額(億)'])

def main():
    print("Access data via MSSQL...")
    data = Fetch_data_from_sql()

    print("Access and Upload to Google Sheet via API...")
    update_google_sheet(data)

@app.get("/") 
# ▲ 用來定義路由。這個路由指定了一個 HTTP GET 請求的端點。在這個例子中，"/" 是根路徑
# 所以當有人訪問你部署的應用的根 URL，例如 https://your_actual_python_api_url/，這個 root 函數就會被調用。
# 根據你的代碼，當訪問 "/" 路徑時，FastAPI 會執行 main() 函數，然後返回一個 JSON 消息，表明任務已經完成

def root():
    main()
    return {"message": "Data Updated successfully"}

# 這行確保當這個腳本被直接執行時，而不是被作為模組導入時，以下的代碼會被運行
# 確保某些代碼段只有在腳本以"主程序"的方式運行時才會執行。
if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)
    # host='0.0.0.0' 時，表示我們希望應用程式接收來自所有網絡接口的請求，而不僅僅是本地主機。