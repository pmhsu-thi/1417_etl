import logging
from data_process import DataProcess
from sql_conn import insert_data, insert_test_data

'''
supply, usage:
|-----供給-----|
>> 路邊 on_street_static
分為汽車 (家長接送區, 時段性禁停停車位, 汽車停車位) 及特殊汽車 (汽車身心障礙專用), 所有時段均為固定車格數

>> 路外 off_street_static, off_street_parkings
off_street_static 抓取汽車格位資料, off_street_parkings 抓取特殊汽車格位資料, 僅計算有動態資料之路外停車場

>> 供需調查 multi_sd_supply
僅採用汽車格位數 (car_type = 2), 所有時段均為固定車格數

|-----需求-----|
>> 路邊 on_street_dynamic_hour_vehicle, on_street_dynamic_hour_special_vehicle
分為汽車及特殊汽車資料, 僅收費時段有資料

>> 路外 off_street_dynamic_hours
僅有汽車停車資料

>> 供需調查 multi_sd_demand
採用汽車及特殊汽車資料 (car_type = 2 or 3), 僅計算有調查

totalcar:
|-----供給-----|
>> 路外 off_street_static, off_street_parkings
採用全部靜態資料表格資料, 特殊車格資料取同id最大值
'''

def main():
    # DataProcess('args') 使用 args.py 中參數決定處理時間，如不指定參數則處理前一天資料
    dp = DataProcess('args')
    dp.set_date()
    dp.data_integration()
    result = dp.calc_result()
    insert_data(result)

    # 測試資料處理
    # tmp = dp.get_test_data()
    # insert_test_data(tmp)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()