import sys, os
sys.path.append('/Users/johnokeefe/Boatsetter/analytics/airflow_workspace/airflow_home/vars')
sys.path.append('/Users/johnokeefe/Boatsetter/analytics/airflow_workspace/airflow_home/dags')
import pandas as pd
import csv
import jinja2
from credentials_vars import sendgrid_api_key
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import *
from datetime import datetime, timedelta, date

AIRFLOW_HOME = os.environ['AIRFLOW_HOME']

def render_template(template, **kwargs):
    ''' renders a Jinja template into HTML '''
    # check if template exists
    if not os.path.exists(template):
        print('No template file present: %s' % template)
        sys.exit()

    templateLoader = jinja2.FileSystemLoader(searchpath="/")
    templateEnv = jinja2.Environment(loader=templateLoader)
    templ = templateEnv.get_template(template)
    return templ.render(**kwargs)

def send_email(email, boat_list):

    header_month = (
        datetime.now() - timedelta(days=15)).strftime("%B")

    yr = date.today().year

    sg = SendGridAPIClient(sendgrid_api_key)
    subject = "Boatsetter Fleet Performance - %s, %s" % (header_month, yr)

    html = render_template(
           (AIRFLOW_HOME + "/email/ownerStatsEmail/render_fleet_stats.html")
           , header_month=header_month
           , boat_list=boat_list)

    to_email = Email(email)
    from_email = Email('em@boatsetter.com', 'Boatsetter Fleet Team')
    content = Content("text/html", html)
    mail = Mail(from_email=from_email, subject=subject
                , to_email=to_email, content=content)

    mail = mail.get()
    mail['categories'] = ['%s_fleets_stats' % header_month]

    tracking_settings = TrackingSettings()
    tracking_settings.click_tracking = ClickTracking(True, False)
    tracking_settings.open_tracking = OpenTracking(True)
    tracking_settings = tracking_settings.get()
    mail['tracking_settings'] = tracking_settings

    body = mail

    response = sg.client.mail.send.post(request_body=body)


if __name__ == "__main__":

    fleet_stats = []
    with open('/Users/johnokeefe/newjuly.csv', 'r') as f:

        reader = csv.reader(f)
        for row in reader:
            if row[0] not in [i[0] for i in fleet_stats]:
                fleet_stats.append(row)

    owners = []
    [owners.append(i[13]) for i in fleet_stats if i[13] not in owners]

    cumsum = 0
    for o in owners:

        boats = [i for i in fleet_stats if i[13] == o]

        try:
            # send_email(str(o), boats)

            cumsum += 1

            print cumsum, o

        except:

            pass
