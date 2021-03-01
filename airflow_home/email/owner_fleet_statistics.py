from boat_stats_query import sql
from database_conn import db_conn
import numpy as np
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import *
import os
import jinja2
from credentials_vars import sendgrid_api_key
import datetime
from scipy import stats
import webbrowser

class OwnerFleetStatistics(object):

    global header_month, yr
    header_month = (datetime.datetime.now()
        - datetime.timedelta(days=15)).strftime("%B")
    yr = datetime.date.today().year

    def __init__(self, lower_date, upper_date):

        self.lower_date=lower_date
        self.upper_date=upper_date

        global data
        conn, cursor = db_conn('redshift')

        cursor.execute(sql %((self.lower_date, self.upper_date)*4))
        data = map(lambda x: [x[0], x[1], x[2], x[3], x[4], "{:,}".format(x[5]),
            x[6], x[7], "${:,.2f}".format(x[8]), "${:,.2f}".format(x[9]), x[10],
            x[11], x[12]],cursor.fetchall())

        conn.close()

    def render_template(self, template, **kwargs):
        ''' renders a Jinja template into HTML '''
        # check if template exists
        if not os.path.exists(template):
            print('No template file present: %s' % template)
            sys.exit()

        templateLoader = jinja2.FileSystemLoader(searchpath="/")
        templateEnv = jinja2.Environment(loader=templateLoader)
        templ = templateEnv.get_template(template)
        return templ.render(**kwargs)

    def send_emails(self):

        sg = SendGridAPIClient(sendgrid_api_key)
        subject = "Boatsetter Fleet Performance - %s, %s" %(header_month,yr)

        owners = []

        for boat in data:
            if boat[0] not in owners:
                owners.append(boat[0])

        for owner in owners:

            boat_list = [i for i in data if owner == i[0]]
            html = self.render_template(("/home/ubuntu/analytics/"
            "airflow_workspace/airflow_home/email/render_boat_stats.html"),
            header_month=header_month, boat_list=boat_list)
            to_email = Email('%s' %owner)
            from_email = Email('em@boatsetter.com', 'Boatsetter Fleet Team')
            content = Content("text/html", html)
            mail = Mail(from_email=from_email, subject=subject,
                to_email=to_email, content=content)
            mail = mail.get()
            mail['categories'] = ['%s_fleets_stats' %header_month]

            tracking_settings = TrackingSettings()
            tracking_settings.click_tracking = ClickTracking(True, False)
            tracking_settings.open_tracking = OpenTracking(True)
            tracking_settings=tracking_settings.get()
            mail['tracking_settings'] = tracking_settings

            body = mail

            response = sg.client.mail.send.post(request_body=body)
