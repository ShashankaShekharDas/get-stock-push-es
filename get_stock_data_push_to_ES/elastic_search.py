import csv
import os

from Serde.Serializer_Deserializer import *
from get_stock_data_push_to_ES import INDEX_NAME, INDEX_SHARDS, DATA_DIRECTORY, ERROR_DOCUMENTS_FILE_NAME, \
    ERROR_DIRECTORY
from scripts.manage_es import Manage_ES


def format_data(columns, row):
    keys = list(columns.keys())
    for i in range(len(row)):
        if keys[i] == "Datetime":
            # Converting Datetime from string so Kibana can view it as timestamp
            columns[keys[i]] = datetime.datetime.strptime(row[i], '%Y-%m-%d %H:%M:%S%z')
            continue
        columns[keys[i]] = row[i]
    return columns


class Elastic_Search:
    def __init__(self):
        self.__elastic = Manage_ES()

    def send_to_elastic_search(self, data_send_es):
        try:
            if not self.__elastic(INDEX_NAME, data_send_es):
                raise TimeoutError("Cannot index document to ES")
        except Exception:
            if not os.path.exists(ERROR_DIRECTORY):
                os.makedirs(ERROR_DIRECTORY)
            with open(os.path.join(ERROR_DIRECTORY, ERROR_DOCUMENTS_FILE_NAME), 'a') as error_file:
                error_file.write(serializer(data_send_es) + "\n")

    def send_file_content_to_es(self, file_name):
        ticker_name = "".join([i for i in file_name.replace(".csv", "") if i not in "123456567890"])
        csv_file = os.path.join(DATA_DIRECTORY, file_name)
        columns = {}
        with open(csv_file, newline='\n') as csvfile:
            reader = csv.reader(csvfile, delimiter='\t', quotechar='|')
            for row in reader:
                if not columns:
                    columns = {i: None for i in row}
                    columns["ticker"] = ticker_name
                    continue
                self.send_to_elastic_search(format_data(columns, row))

        # After reading file, all data is either sent or put in errored file
        # Delete the file
        os.remove(csv_file)

    def send_data_to_es(self):
        # check if index is created, accessible and then send data
        print("Index --> " + str(self.__elastic.create_index_if_not_created(INDEX_NAME, INDEX_SHARDS)))
        if not os.path.exists(DATA_DIRECTORY):
            raise FileNotFoundError("Directory " + DATA_DIRECTORY + " doesn't exist. Maybe previous step did not run")

        for file in os.listdir(DATA_DIRECTORY):
            self.send_file_content_to_es(file)

    def send_errored_data_es(self):
        import shutil
        shutil.copyfile(os.path.join(ERROR_DIRECTORY, ERROR_DOCUMENTS_FILE_NAME),
                        os.path.join(ERROR_DIRECTORY, "tmp.txt"))
        os.remove(os.path.join(ERROR_DIRECTORY, ERROR_DOCUMENTS_FILE_NAME))
        with open(os.path.join(ERROR_DIRECTORY, "tmp.txt")) as errored_reader:
            for record in errored_reader.readlines():
                self.send_to_elastic_search(deserializer(record))
