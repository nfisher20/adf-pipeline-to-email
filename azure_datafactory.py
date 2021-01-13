from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.datafactory import DataFactoryManagementClient
import settings

import time

def InitiateDatafactoryClient():

    #azure credentials from local-sp.json for nathans local laptop
    clientId = settings.clientId
    clientSecret = settings.clientSecret
    subscriptionId = settings.subscriptionId
    tenantId = settings.tenantId

    # Specify your Active Directory client ID, client secret, and tenant ID
    credentials = ServicePrincipalCredentials(client_id= clientId, secret= clientSecret, tenant= tenantId)

    # resource_client = ResourceManagementClient(credentials, subscriptionId)
    adf_client = DataFactoryManagementClient(credentials, subscriptionId)

    return adf_client

def RunPipeline(adf_client,pipelinename,delaystart,delayrunstatuscheck):

    time.sleep(delaystart)

    # This program creates this resource group. If it's an existing resource group, comment out the code that creates the resource group
    rg_name = 'AYTU'

    # The data factory name. It must be globally unique.
    datafactory_name = 'aytuadf'

    run_response = adf_client.pipelines.create_run(rg_name,datafactory_name,pipelinename)

    time.sleep(delayrunstatuscheck)

    pipeline_run = adf_client.pipeline_runs.get(rg_name, datafactory_name, run_response.run_id)

    return print("\n\tPipeline run status: {}".format(pipeline_run.status))