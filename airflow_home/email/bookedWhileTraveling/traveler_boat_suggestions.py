import os, sys
sys.path.append('/home/ubuntu/analytics/airflow_workspace/airflow_home/dags')
from uszipcode import SearchEngine
from geopy import distance
from database_conn import db_conn


class TravelerBoatSuggestions(object):

	'''
	@param booking_id is a booking id where renter is not in home state
    @param home_zip is the home zip of the renter
	@param home_state is the home state of the renter
	'''

	def __init__(self, home_zip, home_state, booking_id):

		self.home_zip = home_zip
		self.home_state = home_state
		self.booking_id = booking_id

	def geo_coors(self):

		search = SearchEngine(simple_zipcode=True)
		zipcode = search.by_zipcode(str(self.home_zip))
		values = zipcode.to_dict()
		return values['lat'], values['lng']

	def booking_details(self):

		conn, cursor = db_conn('bs4')
		cursor.execute(
			"""
			SELECT
				bt.id
				, bt.boat_type
				, bt.boat_category
				, bt.length
				, b.price_with_service_fee_cents
					- b.sales_tax_cents
					- b.service_fee_cents
				, p.bareboat
				, b.trip_type
				, a.state
				, a.latitude
				, a.longitude
			FROM bookings b
			JOIN packages p
				ON p.id = b.package_id
			JOIN boats bt
				ON bt.id = p.boat_id
			JOIN addresses a
				ON a.addressable_type = 'Boat'
				AND a.addressable_id = bt.id
				AND a.deleted_at IS NULL
			WHERE b.id = %s
			AND b.trip_type IN ('half_day', 'full_day')
			AND (a.latitude IS NOT NULL AND a.longitude IS NOT NULL)
			""" % self.booking_id
			)
		bk_details = cursor.fetchall()
		conn.close()
		return bk_details[0]

	def min_package_price(self, bt_public_id):

		conn, cursor = db_conn('bs4')
		cursor.execute(
			"""
			SELECT
			CASE WHEN MIN(p.half_day_cents) > 0 THEN
				ROUND(MIN(p.half_day_cents)/100::DECIMAL,2)
			ELSE ROUND(MIN(p.all_day_cents)/100::DECIMAL,2)
			END
			FROM packages p
			JOIN boats bt
				ON bt.id = p.boat_id
			WHERE bt.public_id = '%s'
			AND p.available = True
			""" % bt_public_id
		)
		min_price = cursor.fetchall()[0][0]
		conn.close()
		return min_price

	def boat_suggestions(self):

		bk_details = self.booking_details()
		home_coors = self.geo_coors()

		conn, cursor = db_conn('bs4')
		cursor.execute(
			"""
			SELECT
				bt.public_id
				, f.boat_image_large
				, bt.boat_type
				, bt.boat_category
				, bt.length
				, CASE
					WHEN p.include_captain_price = True
						AND bt.length <= 29 THEN p.half_day_cents + 22000
					WHEN p.include_captain_price = True
						AND bt.length >= 30
						AND bt.length <= 45 THEN p.half_day_cents + 28000
					WHEN p.include_captain_price = True
						AND bt.length >= 46 THEN p.half_day_cents + 39000
					ELSE half_day_cents
				END as half_day_cents
				, CASE
					WHEN p.include_captain_price = True
						AND bt.length <= 29 THEN p.all_day_cents + 33000
					WHEN p.include_captain_price = True
						AND bt.length >= 30
						AND bt.length <= 45 THEN p.all_day_cents + 40000
					WHEN p.include_captain_price = True
						AND bt.length >= 46 THEN p.all_day_cents + 67000
					ELSE all_day_cents
				END as all_day_cents
				, a.latitude
				, a.longitude
				, concat(bt.length::TEXT, 'ft '::TEXT, bt.make)
				, a.city
				, a.state
			FROM boats bt
			JOIN packages p
				ON p.boat_id = bt.id
			JOIN fleets f
				ON f.boat_id = bt.id
			JOIN addresses a
				ON a.addressable_type = 'Boat'
				AND a.addressable_id = bt.id
				AND a.deleted_at IS NULL
			WHERE bt.id != %s
			AND p.bareboat = %s
			AND f.searchable = True
			AND p.available = True
			AND (a.latitude IS NOT NULL AND a.longitude IS NOT NULL)
			""" % (bk_details[0], bk_details[5])
		)
		boat_list = cursor.fetchall()
		conn.close()

		def distance_filter(boat_list, max_distance):
			return filter(lambda x: distance.distance((x[7], x[8]),
			 			 home_coors).miles < max_distance, boat_list)

		if len(distance_filter(boat_list, max_distance = 10)) > 10:

			boat_list = distance_filter(boat_list, max_distance = 10)

		elif len(distance_filter(boat_list, max_distance = 30)) > 3:

			boat_list = distance_filter(boat_list, max_distance = 30)

		elif len(distance_filter(boat_list, max_distance = 50)) > 3:

			boat_list = distance_filter(boat_list, max_distance = 50)

		else:

			boat_list = None

		def score(boat_type, boat_category, boat_length, half_day_cents,
				  all_day_cents):

			score = 0

			if bk_details[1] == boat_type:
				score += 100

			if bk_details[2] == boat_category:
				score += 100

			score -= (100 *
					abs((boat_length - bk_details[3]) / float(bk_details[3])))

			if bk_details[6] == 'half_day':
				score -= (100 *
						 abs((half_day_cents - bk_details[4])
						 /
						 float(bk_details[4])))

			elif bk_details[6] == 'full_day':

				score -= (100 *
						 abs((all_day_cents - bk_details[4])
						 /
						 float(bk_details[4])))

			return score

		scored_boats = []

		if boat_list is not None:

			for boat in boat_list:

				location = boat[10].capitalize() + ', ' + boat[11]

				scored_boats.append(
				(boat[0], boat[1], score(boat[2], boat[3], boat[4], boat[5],
				boat[6]), boat[9], self.min_package_price(boat[0]), location))

			sorted_boats = sorted(scored_boats, key = lambda x: x[2],
			 					  reverse = True)[:3]

			return [(i[0], i[1], i[3], i[4], i[5]) for i in sorted_boats]

		else:

			return None
