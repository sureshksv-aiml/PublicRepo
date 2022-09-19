from deephaven.appmode import ApplicationState, get_app_state
from typing import Callable
from deephaven import new_table
from deephaven.column import string_col
import smtplib
from email.message import EmailMessage

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
    sent_subject = "Reports "+status+" - Email from Sod Process"
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

        print('Communicate to desk - Email sent!')
    except Exception as exception:
        print("Error: %s!\n\n" % exception)




#filter = new_table([string_col("Statuses", ["Success"])])
failed_processes = process_status.where_not_in(filter_table=new_table([string_col("Statuses", ["Success"])]), cols=["Status = Statuses"])
print("Communicate to desk - No of failed Processes:"+ str(failed_processes.size))
if(failed_processes.size == 0):
    send_email_to_desk()
else:
    send_email_to_desk("Failed")




def start(app: ApplicationState):    
    print("Communicate to desk - Start")

def initialize(func: Callable[[ApplicationState], None]):
    print("Communicate to desk - Init - Start")
    app = get_app_state()
    print("Communicate to desk - Init - Got App State")
    func(app)
    print("Communicate to desk - Init - End")
    print('***' * 20)
initialize(start)    