from pydeephaven import Session
import pyarrow.parquet as pq
import numpy as np
import pandas as pd
import smtplib
from email.message import EmailMessage

#go to anaconda prompt
#install pydeephaven==0.16 : pip install pydeephaven==0.16
#check version : pip show pydeephaven
# and run : python C:\Office_Learning\Deephaven\Client\client_scripts_deephaven.py
def createAndBindTickingTable():
    print("Before Getting Session")
    session = Session() # assuming Deephaven Community Edition is running locally with the default configuration
    print("After Getting Session")
    table = session.time_table(period=1000000000).update(formulas=["Col1 = i % 2"])
    print("After Creating Table")
    session.bind_table(name="Clinet_table", table=table)
    print("After Binding Table")
    session.close()

def getExistingTable():
    print("Before Getting Session")
    session = Session() # assuming Deephaven Community Edition is running locally with the default configuration
    print("After Getting Session")
    print("Existing Tables are " + str(session.tables))
    bonds_refData = session.open_table("ref_data")
    print("Opened Table bonds_refData")
    query_obj = session.query(bonds_refData).where(["CUSIP icase in 'FA973SY0U'"])
    print("Query created on Table bonds_refData")
    #query_obj = session.query(bond_csv).where(["CUSIP icase in 'FA973SY0U'"])
    table2 = query_obj.exec();
    print("Query executed on Table bonds_refData")
    session.bind_table(name="python_client_table", table=table2)
    print("Binded Python client filtered Table so that it can be seen in DeepHaven")


#getExistingTable()


def send_email_to_desk(status="Success"):

    # =============================================================================
    # SET EMAIL LOGIN REQUIREMENTS
    # =============================================================================
    #gmail_user = 'hairsuri@gmail.com'
    #gmail_app_password = 'wpvnxgkowqzhofub'

    gmail_user = 'kunireddisri@gmail.com'
    gmail_app_password = 'ggbnzgajxruidrso'


    # =============================================================================
    # SET THE INFO ABOUT THE EMAIL
    # =============================================================================
    sent_from = gmail_user
    sent_to = ['sureshksv@gmail.com']
    sent_subject = "Reports "+status+" - Email from Client"
    sent_body = ("Hi, \n\n" 
                 "Reports are Successfully generated.\n\n"
                 "Please access them using following url..blah blah..\n"
                 "\n"
                 "Thanks,\n"
                 "SpDb Team\n")
    if(status != "Success"):
        sent_body = ("Hi, \n\n" 
                 "Reports genarion have some issues.\n\n"
                 "Please look into and get back to desk..blah blah..\n"
                 "\n"
                 "Thanks,\n"
                 "SpDb Team\n")

    
    msg = EmailMessage()
    msg.set_content(sent_body)
    msg['Subject'] = sent_subject
    msg['From'] = sent_from
    msg['To'] = ", ".join(sent_to)

    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.ehlo()
        server.login(gmail_user, gmail_app_password)
        server.send_message(msg)
        server.close()

        print('Email sent!')
    except Exception as exception:
        print("Error: %s!\n\n" % exception)



def check_send_email():
    try:
        session = Session() # assuming Deephaven Community Edition is running locally with the default configuration
        print("After Getting Session")
        print("Existing Tables are " + str(session.tables))
        process_status = session.open_table("process_status")
        print("Opened Table process_status")
        query_obj = session.query(process_status).where(["Status icase not in 'Success'"])
        print("Query created on Table process_status")
        #query_obj = session.query(bond_csv).where(["CUSIP icase in 'FA973SY0U'"])
        failed_processes = query_obj.exec();
        print(failed_processes.size)
        
        if(failed_processes.size == 0):
            send_email_to_desk();
        else:
            send_email_to_desk("Failed")
    except Exception as exception:
        print("Error: %s!\n\n" % exception)
        send_email_to_desk("Failed")
        


#check_send_email()
getExistingTable()






