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