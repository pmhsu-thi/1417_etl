import logging

class DataConversion:
    def __init__(self, static_data, dynamic_data):
        self.dynamic_data = dynamic_data
        self.static_data = static_data
        self.total_cnt = {}
        self.data_hrs = {}
        
    def calc_total_cnt(self):
        for item in self.static_data:
            if item[0] in self.total_cnt.keys():
                logging.warning('dict keys duplicate')
            else:
                self.total_cnt[item[0]] = item[1] + item[2] + item[3]

    def calc_data_hrs(self):
        for item in self.dynamic_data:
            p_id = item[0]
            cnt = self.total_cnt.get(p_id)
            if cnt is None:
                continue
            if p_id not in self.data_hrs.keys():
                self.data_hrs[p_id] = {}
            
            hrs = f'{item[2].year}-{item[2].month}-{item[2].day} {item[2].hour}:00:00'
            if hrs not in self.data_hrs[p_id].keys():
                self.data_hrs[p_id][hrs] = []
            
            self.data_hrs[p_id][hrs].append([cnt - item[1], item[2].minute])

    def data_process(self):
        dynamic_hrs = []
        for p_id in self.data_hrs:
            cnt = self.total_cnt.get(p_id)
            if cnt is None:
                continue
            for hrs in self.data_hrs[p_id]:
                self.data_hrs[p_id][hrs] = sorted(self.data_hrs[p_id][hrs], key=lambda x:x[1])   
                total_stay = 0
                for i, item in enumerate(self.data_hrs[p_id][hrs]):
                    # calc total_stay
                    total_stay += item[0] * 15
                    # calc total_car
                    if not i:
                        total_car = item[0]
                        car_record = item[0]
                    else:
                        diff_number = item[0] - car_record
                        total_car += max(0, diff_number)
                        car_record = item[0]
                
                total_stay = max(0, total_stay)
                total_car = max(0, total_car)
                usage = total_stay / (cnt * 60)
                supply = total_car / cnt
                
                dynamic_hrs.append(
                    [p_id, hrs, 2, total_stay, usage, total_car, supply]
                )

        return dynamic_hrs