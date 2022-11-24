import logging
from sql_conn import get_off_street_data, insert_off_street_data


def date_input():
    print('Enter Data Recovery Scope (YYYY-mm-dd)')
    start = input('>> start date: ')
    end = input('>> end date: ')
    logging.info(f'|----- Recovery Data Between {start} and {end} -----|')
    return start, end

def main():
    start, end = date_input()
    data = get_off_street_data(start, end)
    insert_off_street_data(data)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()