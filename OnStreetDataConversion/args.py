# date_type 默認為自動查找可處理範圍, 如需自訂處理日期則使用 date_type = 'DateRange'
date_type = 'Auto'

# 自訂處理日期開始結束時間
start = '2022-09-07'
end = '2022-10-14'


# 分時資料表格種類
HOURLY_TABLES = ['large_vehicle', 'special_vehicle', 'truck', 'vehicle']

# 各車格類型代碼
NAME_TYPES_CONVERSION = {
    '大型車停車位':0,
    '汽車身心障礙專用':1,
    '裝卸貨專用停車位':2,
    '汽車停車位':3,
    '家長接送區':4,
    '時段性禁停停車位':5
}

# 各車格類型代碼儲存表格
NAME_TYPE_TO_TABLE = {
    0:'large_vehicle',
    1:'special_vehicle',
    2:'truck',
    3:'vehicle',
    4:'vehicle',
    5:'vehicle'
}

# 表格欄位
HOURLY_DATA_COLUMNS = [
    'officialid', 'infotime', 'totalstay', 'stay', 'usage',
    'newcar', 'oldcar','totalcar', 'leavecar', 'supply', 'turnover', 'cnt'
]
DAILY_DATA_COLUMNS = [
    'officialid', 'infodate', 'name_type', 'bill_number', 'park_times', 'amount', 'charging_hrs', 'cnt'
]