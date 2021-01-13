
import azure_blob
import azure_datafactory
import settings
import email

import datetime, dateutil
import pandas as pd
import os

adf_client = azure_datafactory.InitiateDatafactoryClient()

pipeline_run = azure_datafactory.RunPipeline(adf_client,'Pipeline1',30,500)

print(pipeline_run)

todaysdate = datetime.today().strftime('%Y-%m-%d')

actualrx = azure_blob.getBlobAsDF('container2',f'{todaysdate}ActualRx')
dec = azure_blob.getBlobAsDF('container2',f'{todaysdate}Decile')

with pd.ExcelWriter('Data.xlsx', engine = 'openpyxl') as writer:
    actualrx.to_excel(writer, sheet_name='Actual Rx',index=False)
    dec.to_excel(writer, sheet_name='Decile',index=False)

mydate = datetime.datetime.now()+dateutil.relativedelta.relativedelta(months=-1)
thismonth = mydate.strftime("%B")


filestosend = [os.path.join(os.getcwd(),'Data.xlsx')]

recipients = settings.email_to
copy = settings.email_cc

newmessage = f'Recipient, \n\nPlease see the attached info provided as requested: \n1)	{thismonth} monthly data \n2)	Targeting data \n\nPlease confirm receipt of this email.\n\nNathan'
newsubject = f'Monthly Rx data (24 months) - {thismonth}'

email.send_mail(settings.email_from,recipients,copy,newsubject,newmessage,filestosend,'smtp.office365.com',587,settings.email_from,settings.email_password)

