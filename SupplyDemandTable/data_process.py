import logging
from read_csv import read_csv
from sql_conn import insert_single_system_data
from args import TABLE_COLUMNS, ARGS, VEH_CODE, IS_WORKDAY, USED_COLUMNS, INTERPOLATION_RULES

def stop():
    import sys
    sys.exit()

def data_to_dict(raw_data):
    '''
    raw_dict
    {
        area :{
            time_sharing :[ Supply and Demand Data ]
        }
    }
    '''
    raw_dict = {}
    for row in raw_data:
        area = row[1]
        time_sharing = row[0]
        if area not in raw_dict.keys():
            raw_dict[area] = {}
        if time_sharing not in raw_dict[area].keys():
            raw_dict[area][time_sharing] = row[2:]
    return raw_dict

class CalcSupplyDemand:
    pass

class DataProcess:
    def single_system(self, year, calendar, veh_type, raw_dict):
        result = {
            'usage':[],
            'supply':[],
            'totalcar':[]
        }
        self.veh_type = veh_type
        self.calendar = calendar
        self.raw_dict = raw_dict
        
        car_type = VEH_CODE[self.veh_type]
        is_workday = IS_WORKDAY[self.calendar]
        for self.area in self.raw_dict:
            for table in result:
                result[table].append([0 for _ in range(len(TABLE_COLUMNS))])
                result[table][-1][0] = year
                result[table][-1][1] = self.area[:2]
                result[table][-1][2] = self.area
                result[table][-1][3] = car_type
                result[table][-1][28] = is_workday

            for hour in range(8, 24):
                time_sharing = self.interpolation_time(str(hour).zfill(2))
                supply, demand = self.calc_supply_demand('single_system', time_sharing)
                supply_rate = self.calc_supply_rate(supply, demand)
                result['supply'][-1][hour+4] = supply_rate
                result['usage'][-1][hour+4] = min(supply_rate, 1) * 100
                result['totalcar'][-1][hour+4] = supply

        return result

    def multi_system(self, year, calendar, veh_type, raw_dict):
        result = {
            'supply':[],
            'demand':[]
        }
        self.veh_type = veh_type
        self.calendar = calendar
        self.raw_dict = raw_dict

        car_type = VEH_CODE[self.veh_type]
        is_workday = IS_WORKDAY[self.calendar]
        for self.area in self.raw_dict:
            for table in result:
                result[table].append([0 for _ in range(len(TABLE_COLUMNS))])
                result[table][-1][0] = year
                result[table][-1][1] = self.area[:2]
                result[table][-1][2] = self.area
                result[table][-1][3] = car_type
                result[table][-1][28] = is_workday

            for hour in range(8, 24):
                time_sharing = self.interpolation_time(str(hour).zfill(2))
                supply, demand = self.calc_supply_demand('multi_system', time_sharing)
                result['supply'][-1][hour+4] = supply
                result['demand'][-1][hour+4] = demand
        
        return result

    def calc_supply_demand(self, system_type, time_sharing):
        supply = 0
        for index in USED_COLUMNS[system_type][self.veh_type]['supply']:
            if self.raw_dict[self.area][time_sharing][index] <= 0:
                continue
            logging.debug(f'>> {self.area} {time_sharing}-{index} supply + {self.raw_dict[self.area][time_sharing][index]}')
            supply += self.raw_dict[self.area][time_sharing][index]

        demand = 0
        for index in USED_COLUMNS[system_type][self.veh_type]['demand']:
            if self.raw_dict[self.area][time_sharing][index] <= 0:
                continue
            logging.debug(f'>> {self.area} {time_sharing}-{index} demand + {self.raw_dict[self.area][time_sharing][index]}')
            demand += self.raw_dict[self.area][time_sharing][index]
        
        return supply, demand

    def interpolation_time(self, time_sharing):
        if time_sharing in self.raw_dict[self.area]:
            return time_sharing
        if time_sharing in INTERPOLATION_RULES[self.calendar]:
            for hour in INTERPOLATION_RULES[self.calendar][time_sharing]:
                if hour in self.raw_dict[self.area]:
                    return hour
        logging.critical(f'{self.area} Unexpected time_sharing {time_sharing}')
        stop()

    def calc_supply_rate(self, supply, demand):
        if supply:
            return (demand / supply)
        if not demand:
            return 0
        logging.debug(f'supply rate = {demand} / {supply}')
        return 0


def main(interpolation=0):
    logging.basicConfig(level=logging.INFO)
    data_process = DataProcess()

    year = ARGS['year'][0]
    calendar = ARGS['calendar'][1]
    veh_type = ARGS['vehicle_types'][5]
    print(f'{year}/{calendar}/{veh_type}')

    raw_data = read_csv(year, calendar, veh_type)
    raw_dict = data_to_dict(raw_data)
    result = data_process.single_system(year, calendar, veh_type, raw_dict)

    for table in result:
        print(table)
        print(len(result[table]))
        for i in range(5):
            print(result[table][i])

    if interpolation:
        insert_single_system_data(result)


if __name__ == '__main__':
    main(1)
