from sql_conn import get_data
from args import ONSTREET_NAMETYPE, DISTRICT_COMPARISON

class Utils:
    def append_key(self, key, dictionary:dict, content):
        if key not in dictionary.keys():
            dictionary[key] = content

class PreProcess(Utils):
    def __init__(self, period_type):
        data = get_data(period_type)
        self.on_street_static_process(data)
        self.off_street_static_process(data)
        self.on_street_vehicle = data['OnStreet_Vehicle']
        self.on_street_special = data['OnStreet_SpecialVehicle']
        self.off_street_data = data['OffStreet_Dynamic']
        self.sd_supply = self.supply_demand_data_process(data['Supply'])
        self.sd_demand = self.supply_demand_data_process(data['Demand'])
        self.holiday = data['Holiday'][:-1]
        self.is_workday_comparison(self.holiday)
    
    def on_street_static_process(self, data):
        self.on_street_comparison = {}
        for row in data['Onstreet_Static']:
            road = row[0]
            district = DISTRICT_COMPARISON[row[2][:2]]
            self.append_key(road, self.on_street_comparison, {'district':district, 0:0, 2:0, 3:0})
            veh_type = ONSTREET_NAMETYPE[row[1]]
            self.on_street_comparison[road][veh_type] += 1

    def off_street_static_process(self, data):
        self.off_street_comparison = {}
        for row in data['OffStreet_Static_Vehicle']:
            park = row[0]
            district = DISTRICT_COMPARISON[row[1][:2]]
            self.append_key(park, self.off_street_comparison, {'district':district, 2:row[2], 3:0})

        for row in data['OffStreet_Static_Special']:
            park = row[0]
            if park in self.off_street_comparison.keys():
                self.off_street_comparison[park][3] = row[1]

    def is_workday_comparison(self, data):
        self.is_workday = {}
        for row in data:
            date = row[0]
            self.append_key(date, self.is_workday, row[1])
            
    def supply_demand_data_process(self, data):
        sd_data = {}
        for row in data:
            year = row[0]
            district = row[1]
            car_type = row[2]
            is_workday = row[-1]
            self.append_key(district, sd_data, {})
            self.append_key(car_type, sd_data[district], {'year':0})
            self.append_key(0, sd_data[district][car_type], [0 for _ in range(24)])
            self.append_key(1, sd_data[district][car_type], [0 for _ in range(24)])
            # 跳過年份較舊資料
            if year < sd_data[district][car_type]['year']:
                continue
            sd_data[district][car_type]['year'] = year
            for hr, value in enumerate(row[3:-1]):
                sd_data[district][car_type][is_workday][hr] = value

        return sd_data