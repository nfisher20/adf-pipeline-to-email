
import pandas as pd
import datetime
import dateutil.relativedelta
from io import StringIO
import os
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from datetime import datetime, timedelta
import time

#azure credentials from local-sp.json for nathans local laptop
clientId = "xxx"
clientSecret = "xxx"
subscriptionId = "xxx"
tenantId = "xxx"

# This program creates this resource group. If it's an existing resource group, comment out the code that creates the resource group
rg_name = 'AYTU'

# The data factory name. It must be globally unique.
datafactory_name = 'aytuadf'

# Specify your Active Directory client ID, client secret, and tenant ID
credentials = ServicePrincipalCredentials(client_id= clientId, secret= clientSecret, tenant= tenantId)

resource_client = ResourceManagementClient(credentials, subscriptionId)
adf_client = DataFactoryManagementClient(credentials, subscriptionId)

df_params = {'location':'westus'}

datafactory = adf_client.factories.get(rg_name, datafactory_name)


run_response = adf_client.pipelines.create_run(rg_name,datafactory_name,"validusemailprep")

#pipeline takes 6:40 to run + 100 seconds for good measure, this can be improved
time.sleep(500)

pipeline_run = adf_client.pipeline_runs.get(rg_name, datafactory_name, run_response.run_id)
print("\n\tPipeline run status: {}".format(pipeline_run.status))

# Load libraries
from azure.storage.blob import BlobServiceClient
import pandas as pd
import datetime
import dateutil.relativedelta
from io import StringIO
import os

# Define parameters
storageAccountName = "blobname"
storageKey         = "xxx"
containerName      = "container"
url = "https://blob.blob.core.windows.net"

blob_service_client = BlobServiceClient(
                    account_url = url,
                    credential = storageKey
)

def getBlobAsDF(containerName,blobName):
    container_client = blob_service_client.get_container_client(containerName)
    downloaded_blob = container_client.download_blob(blobName)
    df = pd.read_csv(StringIO(downloaded_blob.content_as_text()),dtype = str)
    return df

from datetime import datetime

todaysdate = datetime.today().strftime('%Y-%m-%d')

actualrx = getBlobAsDF('container2',f'{todaysdate}ActualRx')
zoldec = getBlobAsDF('container2',f'{todaysdate}Decile')

with pd.ExcelWriter('Data.xlsx', engine = 'openpyxl') as writer:
    actualrx.to_excel(writer, sheet_name='Actual Rx',index=False)
    zoldec.to_excel(writer, sheet_name='Decile',index=False)

import smtplib
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from email import encoders


def send_mail(send_from, send_to, cc, subject, message, files=[],
              server="localhost", port=587, username='', password=''):
    """Compose and send email with provided info and attachments.

    Args:
        send_from (str): from name
        send_to (list[str]): to name(s)
        subject (str): message title
        message (str): message body
        files (list[str]): list of file paths to be attached to email
        server (str): mail server host name
        port (int): port number
        username (str): server auth username
        password (str): server auth password
        use_tls (bool): use TLS mode
    """
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Cc'] = cc
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject
    rcpt = cc.split(",") + [send_to]

    msg.attach(MIMEText(message))

    for path in files:
        part = MIMEBase('application', "octet-stream")
        with open(path, 'rb') as file:
            part.set_payload(file.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition',
                        'attachment; filename="{}"'.format(Path(path).name))
        msg.attach(part)

    smtp = smtplib.SMTP(server, port)
    smtp.ehlo()
    smtp.starttls()
    smtp.login(username, password)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.quit()

import datetime, dateutil
mydate = datetime.datetime.now()+dateutil.relativedelta.relativedelta(months=-1)
thismonth = mydate.strftime("%B")


filestosend = [r'C:\Users\Savefolder\Data.xlsx']
recipients = ['primaryrecipient@email.com']
copy = 'ccemails@email.com'
newmessage = f'Recipient, \n\nPlease see the attached info provided as requested: \n1)	{thismonth} monthly data \n2)	Targeting data \n\nPlease confirm receipt of this email.\n\nNathan'
newsubject = f'Monthly Rx data (24 months) - {thismonth}'

send_mail('email@aytubio.com',recipients,copy,newsubject,newmessage,filestosend,'smtp.office365.com',587,'data@email.com','xxx')

