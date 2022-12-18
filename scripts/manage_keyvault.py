import os
import subprocess

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


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
        self.bash_command = "az keyvault secret show --name {{secret-name}} --vault-name 'Data-Ingest' " \
                            "--query 'value' "

    def get_secret(self, secret_name):
        """
            Not Using due to Issue in Azure Virtual Machine, authenticating using  az login and bash script to create env variable
        """
        self.credential = DefaultAzureCredential()
        self.client = SecretClient(vault_url=self.KVUri, credential=self.credential)
        self.keyVaultName = os.environ["KEY_VAULT_NAME"]
        return self.client.get_secret(self.secret_names[secret_name]).value

    def get_secret_bash(self, secret_name):
        result = subprocess.run(
            self.bash_command.replace("{{secret-name}}", "'" + self.secret_names[secret_name] + "'"),
            stdout=subprocess.PIPE, shell=True)
        # Since output from bash, includes new line character and double quotes
        return result.stdout.decode("utf-8").replace("\n", "").replace('"', '').strip()


if __name__ == "__main__":
    Manage_Keyvault().get_secret_bash("aws_secret_access_key")
