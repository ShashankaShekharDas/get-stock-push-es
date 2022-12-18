import time

from get_stock_data_push_to_ES import GET_DATA_PERIOD_MINUTES
from get_stock_data_push_to_ES.elastic_search import Elastic_Search
from get_stock_data_push_to_ES.get_data_api import Get_stock_data
import schedule


def get_data_and_send_to_es():
    Get_stock_data().get_stock_data()
    Elastic_Search().send_data_to_es()


schedule.every(GET_DATA_PERIOD_MINUTES).minutes.do(get_data_and_send_to_es)

while True:
    schedule.run_pending()
    time.sleep(1)
