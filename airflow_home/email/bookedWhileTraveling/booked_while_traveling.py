import os, sys
sys.path.append('/home/ubuntu/analytics/airflow_workspace/airflow_home/email/'
                'bookedWhileTraveling')
from traveler_boat_suggestions import TravelerBoatSuggestions
from database_conn import db_conn
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import *
import jinja2
import os
from credentials_vars import sendgrid_api_key
import csv

sg = SendGridAPIClient(sendgrid_api_key)
log_route = ('/home/ubuntu/analytics/airflow_workspace/airflow_home/email'
             '/booked_while_traveling_logs.csv')

def renter_info(booking_id):

    conn, cursor = db_conn('bs4')
    cursor.execute(
        """
        SELECT u.first_name, u.email
        FROM bookings b
        JOIN users u
            ON u.id = b.renter_id
        WHERE b.id = %s
        """ % booking_id
    )
    info = cursor.fetchall()
    conn.close()
    return info[0]

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


conn, cursor = db_conn('redshift')
cursor.execute(
    """
    SELECT
    	b.id
    	, CASE
    		WHEN renter_address.state IS NULL THEN bill.state
    		ELSE renter_address.state
    	  END as home_state
    	, zip.zip_code
    FROM bs4.bookings b
    JOIN bs4.packages p
    	ON p.id = b.package_id
    JOIN bs4.boats bt
    	ON bt.id = p.boat_id
    JOIN bs4.addresses boat_address
    	ON boat_address.addressable_type = 'Boat'
    	AND boat_address.addressable_id = bt.id
    	AND boat_address.deleted_at IS NULL
    JOIN bs4.users u
    	ON u.id = b.renter_id
    LEFT JOIN bs4.addresses renter_address
    	ON renter_address.addressable_type = 'User'
    	AND renter_address.addressable_id = u.id
    	AND renter_address.deleted_at IS NULL
    LEFT JOIN bs4_aux.stripe_customer_billing_state bill
    	ON bill.user_id = u.id
    JOIN bs4.customer_accounts ca
    	ON ca.user_id = u.id
    LEFT JOIN bs4_aux.stripe_card_zip zip
    	ON zip.customer_id = ca.stripe_uuid
    WHERE trip_start::DATE = getdate()::DATE - '14 days'::INTERVAL
    AND b.state = 'concluded'
    AND home_state IS NOT NULL
    AND zip_code IS NOT NULL
    AND home_state != boat_address.state
    AND b.trip_type != 'multi_day'
    AND b.id NOT IN (236495, 236047, 223279)
    """
    )

data = cursor.fetchall()
conn.close()

for trip in data:

    name, email = renter_info(trip[0])
    boats = TravelerBoatSuggestions(trip[2], trip[1], trip[0]).boat_suggestions()

    if boats is not None:

        subject = "Boatsetter has boat rentals near you!"
        html = render_template(
               ("/home/ubuntu/analytics/airflow_workspace/airflow_home/email"
               "/bookedWhileTraveling/booked_while_traveling.html"),
               renter_name=name,
               boat_one_public_id = boats[0][0],
               boat_one_image = boats[0][1],
               boat_one_name = boats[0][2],
               boat_one_price = str(boats[0][3]),
               boat_one_location = boats[0][4],
               boat_two_public_id = boats[1][0],
               boat_two_image = boats[1][1],
               boat_two_name = boats[1][2],
               boat_two_price = str(boats[1][3]),
               boat_two_location = boats[1][4],
               boat_three_public_id = boats[2][0],
               boat_three_image = boats[2][1],
               boat_three_name = boats[2][2],
               boat_three_price = str(boats[2][3]),
               boat_three_location = boats[2][4])

        to_email = Email(email)
        from_email = Email('em@boatsetter.com', 'Boatsetter')
        content = Content("text/html", html)

        mail = Mail(from_email=from_email, subject=subject,
                    to_email=to_email, content=content)

        mail = mail.get()

        body = mail

        response = sg.client.mail.send.post(request_body=body)

        with open(log_route, 'a') as f:
            writer = csv.writer(f, delimiter=',')
            writer.writerow([email, boats[0][0], boats[1][0], boats[2][0]])

        print 'done'

    else:

        with open(log_route, 'a') as f:
            writer = csv.writer(f, delimiter=',')
            writer.writerow([email, None, None, None])
