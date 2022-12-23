import time

import schedule
from opensearchpy.exceptions import ConnectionTimeout

from get_stock_data_push_to_ES import GET_DATA_PERIOD_MINUTES
from get_stock_data_push_to_ES.elastic_search import Elastic_Search
from get_stock_data_push_to_ES.get_data_api import Get_stock_data


def get_data_and_send_to_es():
    Get_stock_data().get_stock_data()
    es = Elastic_Search()
    try:
        es.send_data_to_es()
    except ConnectionTimeout:
        es.refresh_connection()


def try_sending_error_data_to_es():
    es = Elastic_Search()
    es.send_errored_data_es()


schedule.every(GET_DATA_PERIOD_MINUTES).minutes.do(get_data_and_send_to_es)
# Try sending errored files every hour
schedule.every(1).hours.do(try_sending_error_data_to_es)

while True:
    schedule.run_pending()
    time.sleep(1)
