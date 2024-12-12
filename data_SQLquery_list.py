# 營運數據查詢_SQL_query
Operation_data_list =["""
    --成交金額
    SELECT
        日期,
        'Weekly transaction amount (T)' as name,
        FORMAT(ROUND([週成交金額(億)] / 10000, 5), 'N5') AS '週成交金額(兆)',
        FORMAT(ROUND([上週成交金額(億)] / 10000, 5), 'N5') AS '上週成交金額(兆)'
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
    --交易天數
    select 日期, name, current_交易天數, lag(current_交易天數) over (order by 日期) as previous_交易天數
    from (
    SELECT 
        CONVERT(varchar, DATEADD(dd, -DATEDIFF(dy, 0, 日期) % 7, 日期), 23) AS 日期,
        'transaction days' AS name, 
        COUNT(DISTINCT 日期) AS current_交易天數
    FROM 
        CMServer.Northwind.dbo.sysdbase
    WHERE 
        日期 >= '20230101'
    GROUP BY 
        CONVERT(varchar, DATEADD(dd, -DATEDIFF(dy, 0, 日期) % 7, 日期), 23)
    ) a
    ORDER BY 日期
""",
""" 
    --總活躍用戶(不分產品線)
    select 
        [日期], 
        'Active users (all)' as name,
        sum([總活躍用戶]) as current_active_user, 
        lag(sum([總活躍用戶])) over (order by [日期]) as previous_active_user
    FROM [DataViews].[dbo].[週活躍用戶數(平台+付費+生命週期)]
    where [產品線] in ('Money錢', 'X實驗室', '大眾', '同學會', '作者', '社群', '記帳', '發票', '網紅', '其他')
    group by [日期]
    order by [日期] desc
""",
"""
    --總活躍用戶(金融事業群)
    select 
        [日期], 
        'Active users (finance)' as name,
        sum([總活躍用戶]) as current_active_user, 
        lag(sum([總活躍用戶])) over (order by [日期]) as previous_active_user
    FROM [DataViews].[dbo].[週活躍用戶數(平台+付費+生命週期)]
    where [產品線] in ('大眾', '同學會', '作者', 'Money錢')
    group by [日期]
    order by [日期] desc
""",
"""
    --總造訪數(不分產品線)
    SELECT [日期]
        , 'session' as name
        ,sum([總造訪次數]) as total_session
        ,lag(sum([總造訪次數])) over (order by 日期) as previous_active_session
    FROM [DataViews].[dbo].[週造訪次數 & 造訪天數]
    where [產品線] in ('Money錢', 'X實驗室', '大眾', '同學會', '作者', '社群', '記帳', '發票', '網紅', '其他')
    group by [日期]
""",
"""
    -- 總註冊數(不含消費)
    SELECT  
        convert(date, [wdate], 23) as 日期
        ,'register' as name
        ,sum([Register]) as current_reg
        ,LAG(sum(register)) over (order by wdate) as previous_reg
    FROM [CMAPP].[dbo].[View_RegistrationRecord_Week] r
    left join cmapp.dbo.View_TableauAppInfo a
    on r.appid=a.AppId
    where a.PrdLineName in  ('Money錢', 'X實驗室', '大眾', '同學會', '作者', '社群', '網紅', '其他')
    group by wdate
"""
]


# "週"用戶數據_data_list
User_data_list_week = [
    """
    -- 活躍數-分產品名稱
    select 
        [日期], 產品名稱,[產品線], 
        current_active, previous_active, (current_active-previous_active) as difference
    from (
    select 
        [日期], 產品名稱,[產品線], sum([總活躍用戶]) as current_active, 
        lag(sum([總活躍用戶])) over (partition by 產品線, 產品名稱 order by [日期]) as previous_active
    FROM [DataViews].[dbo].[週活躍用戶數(平台+付費+生命週期)]
    where [產品線] in ('Money錢', 'X實驗室', '大眾', '同學會', '作者', '社群', '記帳', '發票', '網紅')
    group by [日期], [產品線], 產品名稱
    ) a
    """,
    """
    -- 造訪數-分產品名稱
    select [日期], 產品名稱,[產品線], current_appsession, previous_appsession, (current_appsession-previous_appsession) as difference
    from (
    select 
        [日期], 產品名稱,[產品線], sum([總造訪次數]) as current_appsession, 
        lag(sum([總造訪次數])) over (partition by 產品線, 產品名稱 order by [日期]) as previous_appsession
    FROM [DataViews].[dbo].[週造訪次數 & 造訪天數]
    where [產品線] in ('Money錢', 'X實驗室', '大眾', '同學會', '作者', '社群', '記帳', '發票', '網紅')
    group by [日期], [產品線], 產品名稱
    ) a
    """,
    """
    select *, (current_register - previous_register) as difference
    from (
        SELECT convert(date,[wdate], 23) as 日期
            ,b.appname, b.prdlinename
            ,sum([Register]) as current_register
            ,lag(sum([Register])) over (partition by b.prdlinename, b.appname order by wdate) as previous_register
        FROM [CMAPP].[dbo].[View_RegistrationRecord_Week] a
        left join (select appid,appname, PrdLineName  from cmapp.dbo.View_TableauAppInfo) b
        on a.appid=b.AppId
        where b.PrdLineName in ('Money錢', 'X實驗室', '大眾', '同學會', '作者', '社群', '網紅', '其他')
        group by wdate, b.AppName, b.PrdLineName
        ) tmp
    """
]

# 週活躍數_產品線_生命週期
Active_User_week = [
""" 
    --總活躍用戶(分產品線)
    select da.日期, da.[月、日], da.產品線, da.current_active_user, da.new_user,da.retained_user, da.recall_uesr
    from 
    (
        select distinct top 26 ([日期]) as [日期]
        FROM [DataViews].[dbo].[週活躍用戶數(平台+付費+生命週期)]
        order by [日期] desc
    ) dt
    left join 
    (
        select 
            [日期], 
            right(日期, 5)as '月、日',
            產品線,
            sum([總活躍用戶]) as current_active_user,
			sum(新用戶) as new_user,
			sum(留存用戶) as retained_user,
			sum(召回用戶) as recall_uesr
        FROM [DataViews].[dbo].[週活躍用戶數(平台+付費+生命週期)]
        where [產品線] in ('Money錢', 'X實驗室', '大眾', '同學會', '作者', '社群', '記帳', '發票', '網紅', '其他')
        group by [日期],產品線
    ) da
    on dt.日期=da.日期
"""]

# 月活躍數_產品線_生命週期
Active_User_month = [
""" 
    --總活躍用戶(分產品線)
    select da.日期, da.[月、日], da.產品線, da.current_active_user, da.new_user,da.retained_user, da.recall_uesr
    from 
    (
        select distinct top 26 ([日期]) as [日期]
        FROM [DataViews].[dbo].[月活躍用戶數(平台+付費+生命週期)]
        order by [日期] desc
    ) dt
    left join 
    (
        select 
            [日期], 
            right(日期, 5)as '月、日',
            產品線,
            sum([總活躍用戶]) as current_active_user,
			sum(新用戶) as new_user,
			sum(留存用戶) as retained_user,
			sum(召回用戶) as recall_uesr
        FROM [DataViews].[dbo].[月活躍用戶數(平台+付費+生命週期)]
        where [產品線] in ('Money錢', 'X實驗室', '大眾', '同學會', '作者', '社群', '記帳', '發票', '網紅', '其他')
        group by [日期],產品線
    ) da
    on dt.日期=da.日期
"""]

# print(type(Active_User_week)) #list

# 週造訪數-產品線(全)
APP_Session_week = ["""
    select da.日期, da.[月、日], da.[產品線], da.current_appsession
    from (
        select distinct top 26 日期
        from [DataViews].[dbo].[週造訪次數 & 造訪天數]
        where 日期 < (select max(日期) from [DataViews].[dbo].[週造訪次數 & 造訪天數])
        order by 日期 desc
    ) dt
    left join 
    (
        select  
            [日期] 
            ,right(日期, 5) as '月、日'
            ,[產品線]	 
            ,sum([總造訪次數]) as current_appsession

        FROM [DataViews].[dbo].[週造訪次數 & 造訪天數]
        where [產品線] in ('Money錢', 'X實驗室', '大眾', '同學會', '作者', '社群', '記帳', '發票', '網紅', '其他')
        group by 日期 ,產品線
    ) da
    on dt.日期=da.日期
"""]

# 週註冊數
Reg_week = ["""
    select da.日期, da.[月、日], da.產品線, sum(da.current_reg ) as current_reg
    from  (
        select distinct top 26 (convert(varchar(10), cast(wdate as date), 120)) as 日期
        from [CMAPP].[dbo].[View_RegistrationRecord_Week]
        where wdate < (select max(wdate) from [CMAPP].[dbo].[View_RegistrationRecord_Week])
        order by (convert(varchar(10), cast(wdate as date), 120)) desc
    ) dt
    left join
    (
        SELECT 
            convert(varchar(10), cast(a.wdate as date), 120) as 日期, 
            right(convert(varchar(10), cast(a.wdate as date), 120), 5) as '月、日',
            sum(a.register) as current_reg, 
            case when b.Appname='超慢跑節拍器' then 'X實驗室' 
            else b.PrdLineName end as 產品線, b.AppId, b.AppName
        FROM [CMAPP].[dbo].[View_RegistrationRecord_Week] as a
    left join (
        select * from [CMAPP].[dbo].[View_TableauAppInfo] 
    ) as b
    on a.appid = b.appid
    where b.PrdLineName in ('Money錢', 'X實驗室', '大眾', '同學會', '作者', '社群', '記帳', '發票', '網紅', '其他')
    group by a.wdate, b.prdlinename, b.AppId, b.AppName
    ) da
    on dt.日期=da.日期
    group by da.日期, da.[月、日], da.產品線
"""]

# 週訂單數