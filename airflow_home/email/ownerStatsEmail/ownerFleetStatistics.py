import os, sys
AIRFLOW_HOME = os.environ['AIRFLOW_HOME']
sys.path.append(AIRFLOW_HOME + '/dags')
from database_conn import db_conn
import numpy as np
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import *
from math import radians, cos, sin, asin, sqrt
import numpy as np
from datetime import datetime, timedelta, date
from credentials_vars import sendgrid_api_key
import jinja2


class OwnerFleetStatistics(object):

    '''
    Produces information needed to send the owner stats email.
    '''

    def __init__(self, lower_date, upper_date, owner_id):

        self.lower_date = lower_date
        self.upper_date = upper_date
        self.owner_id = owner_id

    def live_boats(self):

        '''
        Produces a list of live boat owned by the owner passed to the owner_id
        parameter of the class.
        '''

        conn, cursor = db_conn('bs4')

        cursor.execute(
            """
            SELECT bt.id, bt.public_id, f.boat_image_large
            FROM boats bt
            JOIN fleets f
                ON f.boat_id = bt.id
            WHERE f.searchable = True
            AND bt.primary_manager_id = %s
            """ % self.owner_id
            )

        boats = [i for i in cursor.fetchall()]
        conn.close()
        return boats

    def get_nearby_boats(self, boat_id):

        '''
        Returns a list of boats within the distance defined by radius_miles
        variable from the boat being evaluated
        '''

        radius_miles = 20

        def haversine(lat1, lon1, lat2, lon2):

            R = 3959.87433

            dLat = radians(lat2 - lat1)
            dLon = radians(lon2 - lon1)
            lat1 = radians(lat1)
            lat2 = radians(lat2)

            a = sin(dLat/2)**2 + cos(lat1)*cos(lat2)*sin(dLon/2)**2
            c = 2*asin(sqrt(a))

            return R * c

        conn, cursor = db_conn('bs4')

        cursor.execute(
            """
            SELECT
                latitude, longitude, state
            FROM addresses
            WHERE addressable_type = 'Boat'
            AND addressable_id = %s
            AND deleted_at IS NULL
            """ % boat_id
            )

        coords = cursor.fetchall()[0]

        cursor.execute(
            """
            SELECT f.boat_id, latitude, longitude, bt.primary_manager_id
            FROM fleets f
            JOIN boats bt
            ON bt.id = f.boat_id
            JOIN addresses a
            ON a.addressable_type = 'Boat'
            AND a.addressable_id = bt.id
            AND a.deleted_at IS NULL
            WHERE bt.id != %s
            AND f.searchable = True
            AND latitude IS NOT NULL
            AND longitude IS NOT NULL
            AND a.state = '%s'
            """ % (boat_id, coords[2])
        )

        live_boats = cursor.fetchall()

        conn.close()

        return [[i[0], i[3]] for i in live_boats
                if haversine(coords[0], coords[1], i[1], i[2]) < radius_miles]

    def search_score_components(self, boat_id):

        '''
        Returns a dictionary with the components of the search score
        that are used in the search score email relative to the boat_id passed
        to the boat_id parameter.
        '''

        components = {}

        conn, cursor = db_conn('bs4')
        cursor.execute(
            """
            SELECT search_score_details
            FROM fleets
            WHERE boat_id = %s
            """ % boat_id
        )

        score_details = cursor.fetchall()[0][0]

        win_rate_last_60 = float(score_details.split(
                                 'formula"=>"')[1].split(
                                 '+', 10)[1].split('*', 10)[0])

        components['win_rate'] = win_rate_last_60

        cursor.execute(
            """
            SELECT
                COUNT(id)
            FROM fleets
            WHERE (captained_instant_book_enabled = True
            OR bareboat_instant_book_enabled = True)
            AND boat_id = %s
            """ % boat_id
        )

        insta_packages = cursor.fetchall()[0][0]

        if insta_packages > 0:

            components['insta_enabled'] = True

        else:

            components['insta_enabled'] = False

        cursor.execute(
            """
            WITH owner_id AS (
            SELECT bt.primary_manager_id
            FROM boats bt
            WHERE bt.id = %s
            GROUP BY bt.primary_manager_id
            )
            SELECT
            COUNT(DISTINCT be.booking_id)
            ,COUNT(distinct b.id)
            FROM bookings b
            JOIN packages p
            ON p.id = b.package_id
            JOIN boats bt
            ON bt.id = p.boat_id
            JOIN owner_id oid
            ON bt.primary_manager_id = oid.primary_manager_id
            LEFT JOIN booking_events be
            ON be.booking_id = b.id
            AND be.acting_user_type = 'owner'
            """ % boat_id
        )

        response_vars = cursor.fetchall()[0]

        if response_vars[1] > 0:

            components['owner_response_rate'] = (
                float(response_vars[0])/response_vars[1])

        else:

            components['owner_response_rate'] = 0

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM reviews
            WHERE reviewable_type = 'Boat'
            AND reviewable_id = %s
            AND star_rating = 5
            """ % boat_id
        )

        components['five_star_reviews'] = int(cursor.fetchall()[0][0])

        conn.close()

        return components

    def performance_stats(self, boat_id):

        '''
        Returns a dictionary with the performance stats
        used in the search score email.
        '''

        stats = {}

        conn, cursor = db_conn('redshift')

        # Boat pageviews during time period
        cursor.execute(
            """
            SELECT COUNT(DISTINCT v.id)
            FROM bs4.boats bt
            JOIN astronomer_production.viewed_boat_listing v
            ON SPLIT_PART(v.context_page_path, '/',3) = bt.public_id
            AND v.received_at::DATE BETWEEN '%s' AND '%s'
            WHERE bt.id = %s
            """ % (self.lower_date, self.upper_date, boat_id)
        )

        stats['views'] = int(cursor.fetchall()[0][0])

        conn.close()

        conn, cursor = db_conn('bs4')

        # Ops, Rentals, and Earnings for rentals during time period
        cursor.execute(
            """
            SELECT
                COUNT(DISTINCT b.opportunity_id)
                ,COUNT(DISTINCT CASE
                    WHEN b.state IN
                    ('concluded', 'disputed', 'aboard', 'ashore', 'approved')
                    THEN b.id END)
                ,CASE WHEN SUM(CASE WHEN b.state IN
                    ('concluded', 'disputed', 'aboard', 'ashore', 'approved')
                    THEN b.owner_payout_cents END) IS NOT NULL
                    THEN CONCAT('$', ROUND(SUM(CASE WHEN b.state IN
                    ('concluded', 'disputed', 'aboard', 'ashore', 'approved')
                    THEN b.owner_payout_cents END)/100::DECIMAL,2)::TEXT)
                    ELSE '$0.00' END

            FROM bookings b
            JOIN packages p
            ON p.id = b.package_id
            JOIN boats bt
            ON bt.id = p.boat_id
            WHERE bt.id = %s
            AND b.trip_start::DATE BETWEEN '%s' AND '%s'
            """ % (boat_id, self.lower_date, self.upper_date)
        )

        bookings = cursor.fetchall()[0]
        stats['ops'] = int(bookings[0])
        stats['rentals'] = int(bookings[1])

        if bookings[2] is not None:

            stats['earnings'] = bookings[2]

        else:

            stats['earnings'] = '$0.00'

        # Earnings lost to other owners during time period
        cursor.execute(
            """
            WITH boat_ops as (
            SELECT b.opportunity_id
            FROM bookings b
            JOIN packages p
            ON p.id = b.package_id
            WHERE p.boat_id = %s
            AND b.trip_start BETWEEN '%s' AND '%s'
            )
            SELECT CONCAT('$',
            CASE WHEN SUM((b.owner_payout_cents)/100::DECIMAL)::TEXT IS NULL
            THEN 0.00::TEXT
            ELSE ROUND(SUM((b.owner_payout_cents)/100::DECIMAL),2)::TEXT END)

            FROM bookings b
            JOIN packages p
            ON p.id = b.package_id
            JOIN boats bt
            ON bt.id = p.boat_id
            JOIN boat_ops bo
            ON bo.opportunity_id = b.opportunity_id
            WHERE bt.primary_manager_id != %s
            AND b.state IN
                ('concluded', 'disputed', 'aboard', 'ashore', 'approved')
            """ % (boat_id, self.lower_date, self.upper_date, self.owner_id)
            )

        earnings_lost = cursor.fetchall()[0][0]

        if earnings_lost is not None:

            stats['earnings_lost'] = earnings_lost

        else:

            stats['earnings_lost'] = '$0.00'

        conn.close()

        return stats

    def compare_search_components(self, boat_id):

        '''
        Compares the search score components of the boat_id passed to the boat_id
        parameter to the same components of nearby live boats. Returns a
        dictionary with each component as a key as 0, 1, or 2 as the value.
        0 indicates the search component is < the 33rd percentile compared
        to other boats generated by the get_nearby_boats method. 1 indicates the
        component is within the 33rd - 66th percentile and 2 represents the
        value is > the 66th percentile.
        '''

        search_score_components = self.search_score_components(boat_id)

        nearby_components = [self.search_score_components(i[0])
                             for i in self.get_nearby_boats(boat_id)]

        def compare_component(component, nearby_components):

            nearby = [i[component] for i in nearby_components]

            if len(nearby) > 0:

                low = np.percentile(nearby, 33.33)
                high = np.percentile(nearby, 66.66)

                if search_score_components[component] <= low:

                    return 0

                elif (search_score_components[component] <= high and
                      search_score_components[component] > low):

                    return 1

                else:

                    return 2

            else:

                return 1

        comparison = {}

        comparison['win_rate'] = compare_component('win_rate'
                                                   , nearby_components)

        comparison['owner_response_rate'] = compare_component(
                                            'owner_response_rate'
                                            , nearby_components)

        comparison['five_star_reviews'] = compare_component('five_star_reviews'
                                                            , nearby_components)

        if search_score_components['insta_enabled'] is True:

            comparison['insta_enabled'] = 2

        else:

            comparison['insta_enabled'] = 0

        return comparison

    def build_kitchen_sink(self):

        sink = []

        for bt in self.live_boats():

            stats = self.performance_stats(bt[0])
            search_report = self.compare_search_components(bt[0])

            row = [bt[0], stats['views'], stats['ops'], stats['rentals']
                   , stats['earnings'], stats['earnings_lost']
                   , search_report['win_rate'], search_report['insta_enabled']
                   , search_report['five_star_reviews']
                   , search_report['owner_response_rate'], bt[1], bt[2]]

            sink.append(row)

        return sink

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

    def send_email(self, email):

        header_month = (
            datetime.now() - timedelta(days=15)).strftime("%B")

        yr = date.today().year

        sg = SendGridAPIClient(sendgrid_api_key)
        subject = "Boatsetter Fleet Performance - %s, %s" % (header_month, yr)

        boat_list = self.build_kitchen_sink()

        html = self.render_template(
               ("/home/ubuntu/analytics/airflow_workspace/"
                "airflow_home/email/render_fleet_stats.html")
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
