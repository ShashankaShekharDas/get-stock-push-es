import os

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

from scripts import SECRETS_NAME
import subprocess


class Manage_Keyvault:
    """
        Manage Azure Keyvault
        Gets the Secrets from Azure Keyvault and returns the value of the secret
    """

    def __init__(self):
        self.keyVaultName = None
        self.credential = None
        self.client = None
        self.KVUri = f"https://{self.keyVaultName}.vault.azure.net"

        self.secret_names = {
            "ES_USERNAME": "ES-USERNAME",
            "ES_PASSWORD": "ES-PASSWORD",
            "ES_URL": "ES-DOMAIN-ENDPOINT",
            "aws_access_key_id": "aws-access-key-id",
            "aws_secret_access_key": "aws-secret-access-key"
        }

    def get_secret(self, secret_name):
        """
            Not Using due to Issue in Azure Virtual Machine, authenticating using  az login and bash script to create env variable
        """
        self.credential = DefaultAzureCredential()
        self.client = SecretClient(vault_url=self.KVUri, credential=self.credential)
        self.keyVaultName = os.environ["KEY_VAULT_NAME"]
        return self.client.get_secret(self.secret_names[secret_name]).value

    def refresh_environment_variable(self):
        rc = subprocess.call("environment_variable.sh")
        print(rc)


if __name__ == "__main__":
    Manage_Keyvault().refresh_environment_variable()
