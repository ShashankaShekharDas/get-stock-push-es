#Just for Testing Remove Later
from get_stock_data_push_to_ES.elastic_search import Elastic_Search
from get_stock_data_push_to_ES.get_data_api import Get_stock_data
from scripts.manage_keyvault import Manage_Keyvault

# Get_stock_data().get_stock_data()
# Elastic_Search().send_data_to_es()

Manage_Keyvault().refresh_environment_variable()