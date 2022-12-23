import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth

from scripts.manage_keyvault import Manage_Keyvault


class Manage_ES:
    """
        Manages Elasticsearch Index.
        Exposes Function to manage ES to
            - Create Index
            - Search Index
            - Delete Document
            - Create Document
            - Delete Index
    """

    def __init__(self):
        # Initialize Objects
        self.manage_keyvault = Manage_Keyvault()

        # Initialize Class Variables
        self.__host = self.manage_keyvault.get_secret_bash("ES_URL")
        self.__aws_access_key_id = self.manage_keyvault.get_secret_bash("aws_access_key_id")
        self.__aws_secret_access_key = self.manage_keyvault.get_secret_bash("aws_secret_access_key")
        self.region = 'ap-south-1'

        # Initialize Credentials
        self.__credentials = boto3.Session(
            aws_access_key_id=self.__aws_access_key_id,
            aws_secret_access_key=self.__aws_secret_access_key
        ).get_credentials()

        self.__auth = AWSV4SignerAuth(self.__credentials, self.region)

        self.__client = OpenSearch(
            hosts=[{'host': self.__host, 'port': 443}],
            http_auth=self.__auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )

    def refresh_connection(self):
        self.__client = OpenSearch(
            hosts=[{'host': self.__host, 'port': 443}],
            http_auth=self.__auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )

    def create_index(self, index_name, number_of_shards=5):
        index_body = {
            'settings': {
                'index': {
                    'number_of_shards': number_of_shards
                }
            }
        }
        return self.__client.indices.create(index_name, body=index_body)

    def create_index_if_not_created(self, index_name, number_of_shards=5):
        try:
            index_body = {
                'settings': {
                    'index': {
                        'number_of_shards': number_of_shards
                    }
                }
            }
            return self.__client.indices.create(index_name, body=index_body)["acknowledged"]
        except Exception as http_exception:
            return http_exception.error == "resource_already_exists_exception"

    def search_index(self, query, index, fields=[]):
        return self.__client.search(
            body={
                'size': 5,
                'query': {
                    'multi_match': {
                        'query': query,
                        'fields': ['name']
                    }
                }
            },
            index=index
        )

    def delete_document(self, id, index_name):
        return self.__client.delete(
            id=id,
            index=index_name
        )

    def delete_index(self, index_name):
        return self.__client.indices.delete(
            index=index_name
        )

    def insert_document(self, index_name, document):
        try:
            insert_document = self.__client.index(
                index=index_name,
                body=document,
                refresh=True
            )
            return {i: insert_document[i] for i in {"_index", "_id", "result"}}
        except Exception:
            return False


if __name__ == "__main__":
    es = Manage_ES()
    # print(es.search_index('Shashanka', 'test_index', ["name"]))

    index_name = "from_code"
    id = 999
    document = {
        "key": 1234,
        "name": "Shashanka",
        "Phone": 12345678,
        "Job": "Useless"
    }
    number_of_shard = 4

    print("Create Index -->" + str(es.create_index_if_not_created(index_name, number_of_shard)), end="\n\n\n")
    # print("Create Index -->" + str(es.create_index(index_name, number_of_shard)), end="\n\n\n")
    print("Insert Document -->" + str(es.insert_document(index_name, document)), end="\n\n\n")
    print("Search Document -->" + str(es.search_index("Shashanka", index_name, ["name"])), end="\n\n\n")
    # print("Delete Document-->" + str(es.delete_document(999, index_name)), end="\n\n\n")
    # from time import sleep
    # sleep(10)
    # print("Search Document -->" + str(es.search_index("Shashanka", index_name, ["name"])), end="\n\n\n")
    print("Delete Index -->" + str(es.delete_index(index_name)), end="\n\n\n")
