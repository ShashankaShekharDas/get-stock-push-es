import time

import schedule

from get_stock_data_push_to_ES import *
from get_stock_data_push_to_ES.get_data_api import Get_stock_data

schedule.every(GET_DATA_PERIOD_MINUTES).minutes.do(Get_stock_data().get_stock_data)

while True:
    schedule.run_pending()
    time.sleep(1)