import csv
import os
from datetime import datetime

from get_stock_data_push_to_ES import INDEX_NAME, INDEX_SHARDS, DATA_DIRECTORY
from scripts.manage_es import Manage_ES


def format_data(columns, row):
    keys = list(columns.keys())
    for i in range(len(keys)):
        if keys[i] == "Datetime":
            # Converting Datetime from string so Kibana can view it as timestamp
            columns[keys[i]] = datetime.strptime(row[i], '%Y-%m-%d %H:%M:%S%z')
            continue
        columns[keys[i]] = row[i]
    return columns


class Elastic_Search:
    def __init__(self):
        self.__elastic = Manage_ES()

    def send_file_content_to_es(self, file_name):
        csv_file = os.path.join(DATA_DIRECTORY, file_name)
        columns = {}
        with open(csv_file, newline='\n') as csvfile:
            reader = csv.reader(csvfile, delimiter='\t', quotechar='|')
            for row in reader:
                if not columns:
                    columns = {i: None for i in row}
                    continue
                self.__elastic.insert_document(INDEX_NAME, format_data(columns, row))

    def send_data_to_es(self):
        # check if index is created, accessible and then send data
        print("Index --> " + str(self.__elastic.create_index_if_not_created(INDEX_NAME, INDEX_SHARDS)))
        if not os.path.exists(DATA_DIRECTORY):
            raise FileNotFoundError("Directory " + DATA_DIRECTORY + " doesn't exist. Maybe previous step did not run")

        for file in os.listdir(DATA_DIRECTORY):
            self.send_file_content_to_es(file)
