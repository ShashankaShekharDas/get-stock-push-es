#!/bin/sh

export es_username=$(az keyvault secret show --name "ES-USERNAME" --vault-name "Data-Ingest" --query "value")
export es_password=$(az keyvault secret show --name "ES-PASSWORD" --vault-name "Data-Ingest" --query "value")
export es_url=$(az keyvault secret show --name "ES-DOMAIN-ENDPOINT" --vault-name "Data-Ingest" --query "value")
export aws_access_key_id=$(az keyvault secret show --name "aws-access-key-id" --vault-name "Data-Ingest" --query "value")
export aws_secret_access_key=$(az keyvault secret show --name "aws-secret-access-key" --vault-name "Data-Ingest" --query "value")