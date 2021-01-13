from azure.storage.blob import BlobServiceClient
import pandas as pd
import StringIO
import settings


def initiateBlobServiceClient():
    url = settings.url
    storageKey = settings.storageKey

    blob_service_client = BlobServiceClient(
        account_url=url,
        credential=storageKey
    )

    return  blob_service_client

def returnContainerClient(blob_service_client,writeContainer):
    return blob_service_client.get_container_client(writeContainer)

def getBlobAsDF(containerName,blobName,blob_service_client):
    container_client = blob_service_client.get_container_client(containerName)
    downloaded_blob = container_client.download_blob(blobName)
    df = pd.read_csv(StringIO(downloaded_blob.content_as_text()),dtype = str)
    return df