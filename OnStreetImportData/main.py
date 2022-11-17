import os
import logging
import pandas as pd

from sql_conn import PostgreSQL

class GetData:
    def read_txt(self, file):
        data = []
        with open(file) as txtfile:
            for row in txtfile.readlines():
                row = row[:-1].split(',')
                if len(row) == 1:
                    continue
                data.append(row)
        return data

    def get_columns(self):
        return ['bill_no', 'infodate', 'official_id', 'lac_code', 'vehicle_type', 'start_time', 'park_time', 'bill_count', 'amount']

def main():
    logging.basicConfig(level=logging.INFO)
    engine = PostgreSQL().sql_conn()
    # path = "./OnStreetImportData/data/"
    path = "/home/thi/download/"
    for file in os.listdir(path):
        if 'NewTaipeiTicket' in file and '.txt' in file:
            logging.info(f'|----- Import {file}')
            file = os.path.join(path, file)
            try:
                data_column = GetData().get_columns()
                data = GetData().read_txt(file)
                data = pd.DataFrame(data, columns=data_column)
                data.to_sql('on_street_dynamic', con=engine, if_exists='append', index=False)
            # except ValueError as err:
            #     logging.error(f'>> {err}, {type(err)}')
            except:
                logging.error(f'>> already exist')
            else:
                logging.info(f'>> import complete')

if __name__ == '__main__':
    main()
    
    