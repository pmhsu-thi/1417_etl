import logging
from pre_process import PreProcess

def stop():
    import sys
    sys.exit()

class DataProcess(PreProcess):
    def __init__(self, period_type='default'):
        super().__init__(period_type)
        self.result = {
            'total_supply' : {},
            'supply' : {},
            'demand' : {}
        }
        self.exception = {
            'on_street' : [],
            'off_street' : []
        }

    def data_integration(self):
        ''' 同源資料中，靜態資料表應最先處理 '''
        # 路邊停車資料
        self.join_on_street_static()
        self.join_on_street_dynamic_vehicle()
        self.join_on_street_dynamic_special()

        # 路外停車場資料
        self.join_off_street_static()
        self.join_off_street_dynamic()

        # 人工供需調查資料
        self.join_demand_data()
        self.join_supply_data()

    def join_on_street_static(self):
        for road in self.on_street_comparison:
            district = self.on_street_comparison[road]['district']
            for date in self.is_workday:
                for key in self.result:
                    self.append_key(district, self.result[key][date], {})
                    if key == 'demand':
                        self.append_key(2, self.result[key][date][district], [0 for _ in range(24)])
                        self.append_key(3, self.result[key][date][district], [0 for _ in range(24)])
                    else:
                        self.append_key(2, self.result[key][date][district], 0)
                        self.append_key(3, self.result[key][date][district], 0)
                
                self.result['total_supply'][date][district][2] += self.on_street_comparison[road][2]
                self.result['total_supply'][date][district][3] += self.on_street_comparison[road][3]
                self.result['supply'][date][district][2] += self.on_street_comparison[road][2]
                self.result['supply'][date][district][3] += self.on_street_comparison[road][3]

    def join_on_street_dynamic_vehicle(self):
        for row in self.on_street_vehicle:
            if row[0] in self.exception['on_street']:
                continue
            try:
                district = self.on_street_comparison[row[0]]['district']
            except:
                logging.warning(f">> {row[0]} not in On Street Static Table!")
                self.exception['on_street'].append(row[0])
                continue

            date = row[1].strftime('%Y-%m-%d')
            hour = row[1].hour
            try:
                self.result['demand'][date][district][2][hour] += row[2]
            except:
                logging.warning(f">> Date: {date}, District: {district}, Hour: {hour}")
                logging.warning(">> On Street Static Table Vehicle Key Error!")

    def join_on_street_dynamic_special(self):
        for row in self.on_street_special:
            if row[0] in self.exception['on_street']:
                continue
            try:
                district = self.on_street_comparison[row[0]]['district']
            except:
                logging.warning(f">> {row[0]} not in On Street Static Table!")
                self.exception['on_street'].append(row[0])
                continue

            date = row[1].strftime('%Y-%m-%d')
            hour = row[1].hour
            try:
                self.result['demand'][date][district][3][hour] += row[2]
            except:
                logging.warning(f">> Date: {date}, District: {district}, Hour: {hour}")
                logging.warning(">> On Street Static Table Special Key Error!")

    def join_off_street_static(self):
        for park in self.off_street_comparison:
            district = self.off_street_comparison[park]['district']
            for date in self.is_workday:
                for key in self.result:
                    self.append_key(district, self.result[key][date], {})
                    if key == 'demand':
                        self.append_key(2, self.result[key][date][district], [0 for _ in range(24)])
                        self.append_key(3, self.result[key][date][district], [0 for _ in range(24)])
                    else:
                        self.append_key(2, self.result[key][date][district], 0)
                        self.append_key(3, self.result[key][date][district], 0)
                
                self.result['total_supply'][date][district][2] += self.off_street_comparison[park][2]
                self.result['total_supply'][date][district][3] += self.off_street_comparison[park][3]

    def join_off_street_dynamic(self):
        park_record = []
        for row in self.off_street_data:
            if row[0] in self.exception['off_street']:
                continue
            try:
                district = self.off_street_comparison[row[0]]['district']
            except:
                logging.warning(f">> {row[0]} not in Off Street Static Table!")
                self.exception['off_street'].append(row[0])
                continue

            date = row[1].strftime('%Y-%m-%d')
            pk = f'{date}#{row[0]}'
            if pk not in park_record:
                try:
                    self.result['supply'][date][district][2] += self.off_street_comparison[row[0]][2]
                    self.result['supply'][date][district][3] += self.off_street_comparison[row[0]][3]
                except:
                    logging.warning(f">> Date: {date}, District: {district}")
                    logging.warning(">> Off Street Static Table Vehicle Key Error!")
                park_record.append(pk)

            hour = row[1].hour
            try:
                self.result['demand'][date][district][2][hour] += row[2]
            except:
                logging.warning(f">> Date: {date}, District: {district}, Hour: {hour}")
                logging.warning(">> Off Street Static Table Vehicle Key Error!")

    def join_supply_data(self):
        for district in self.sd_supply:
            car_type = 2
            for row in self.holiday:
                date = row[0]
                workday = row[1]
                self.append_key(district, self.result['total_supply'][date], {})
                self.append_key(car_type, self.result['total_supply'][date][district], 0)
                self.append_key(district, self.result['supply'][date], {})
                self.append_key(car_type, self.result['supply'][date][district], 0)
                for hr in range(24):
                    if self.sd_supply[district][car_type][workday][hr] > 0:
                        self.result['total_supply'][date][district][car_type] += self.sd_supply[district][car_type][workday][hr]
                        self.result['supply'][date][district][car_type] += self.sd_supply[district][car_type][workday][hr]
                        break

    def join_demand_data(self):
        for district in self.sd_demand:
            for car_type in self.sd_demand[district]:
                for row in self.holiday:
                    date = row[0]
                    workday = row[1]
                    self.append_key(district, self.result['demand'][date], {})
                    self.append_key(car_type, self.result['demand'][date][district], [0 for _ in range(24)])
                    for hr in range(24):
                        self.result['demand'][date][district][car_type][hr] += self.sd_demand[district][car_type][workday][hr]

    def set_date(self):
        for row in self.holiday:
            date = row[0]
            for key in self.result:
                self.result[key][date] = {}

    def calc_supply_rate(self, supply, demand):
        if supply:
            return (demand / supply)
        if not demand:
            return 0
        logging.debug(f'Supply = {supply}, Demand = {demand}')
        return 0

    def calc_result(self):
        result = {
            'supply' : [],
            'usage' : [],
            'totalcar' : []
        }
        for date in self.result['supply']:
            for district in self.result['supply'][date]:
                for car_type in self.result['supply'][date][district]:
                    for key in result:
                        result[key].append([date, district, car_type])
                    total_supply = self.result['total_supply'][date][district][car_type]
                    supply = self.result['supply'][date][district][car_type]
                    for hr in range(24):
                        demand = self.result['demand'][date][district][car_type][hr]
                        supply_rate = self.calc_supply_rate(supply, demand)
                        result['supply'][-1] += [supply_rate]
                        result['usage'][-1] += [min(supply_rate, 1) * 100]
                        result['totalcar'][-1] += [total_supply]

        return result

    def get_test_data(self):
        tmp = {
            'supply' : [],
            'demand' : []
        }
        for date in self.result['supply']:
            for district in self.result['supply'][date]:
                for car_type in self.result['supply'][date][district]:
                    tmp['supply'].append([date, district, car_type])
                    for _ in range(24):
                        tmp['supply'][-1] += [self.result['supply'][date][district][car_type]]
                    tmp['demand'].append([date, district, car_type])
                    tmp['demand'][-1] += self.result['demand'][date][district][car_type]

        return tmp
                        

def main():
    dp = DataProcess('args')
    print(dp.is_workday)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()