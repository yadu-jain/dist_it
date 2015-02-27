#author: sWaRtHi
#date: January 06, 2015
#description: Provider function modules

from helpers import provider_exceptions,db
import httplib2,json,urllib,xmltodict,re,time,os  
from suds.client import Client
from datetime import datetime

CONFIG_FILE="ashoka_vl_config.ini"
DEFAULT_SECTION="prod"

class AshokaVL_API(object):
	"""docstring for ValueLabs_API"""
	def __init__(self, section):
		super(AshokaVL_API, self).__init__()
		try:
			import ConfigParser
			global CONFIG_FILE
			self.section = section
			self.config=ConfigParser.ConfigParser()
			path =  os.path.join(os.path.dirname(os.path.abspath(__file__)),CONFIG_FILE)
			self.config.read(path)
			if not (section in self.config.sections()):
				raise Exception("Section "+section+" not Found !")
			self.loaded=True
		except Exception as ex:
			self.loaded=False
			self.loading_error=str(ex)

	def __get_pulldb_config__(self):
		try:
			server 		= self.get_config("pulldb_server")
			db_name 	= self.get_config("pulldb_db")
			user 		= self.get_config("pulldb_user")
			password 	= self.get_config("pulldb_password")
			return (server,db_name,user,password)
		except Exception, ex:
			raise Exception("Invalid PullDB Conffiguration !|"+str(ex))

	def get_config(self,key):
		return self.config.get(self.section,key)

	def __get_client__(self):
		main_url		= self.get_config("main_url")
		if not hasattr(self,'client') or self.client == None:
			self.client=Client(main_url,faults=False)
		#fetching from API
		return self.client.service

	def pull_from_cities(self):
		"""
			author 		: sWaRtHi
			Date 		: January 07, 2015
			Description : Pull From Cities from api 
		"""
		# fetching configurations from config file
		main_url = self.get_config("main_url")
		from_cities_api = self.get_config("from_cities_api")
		login_id = self.get_config("login_id")
		password = self.get_config("password")

		# Calling Web API method
		(response_code,raw_response)=self.__get_client__().GetSourceStations(loginID=login_id,password=password)
		# if response success than return else rais exception
		if response_code==200:
			return raw_response
		else:
			raise Exception("response code="+str(response_code))

	def process_from_cities(self,process_id,str_response):
		"""
			author 		: sWaRtHi
			Date 		: October 10, 2014
			Description : Push From Cities to pulldb
		"""
		try:
			response = xmltodict.parse(str_response)
		except Exception, e:
			raise Exception("Invalid XML response!")
		
		try:
			if len(response["SourceStationList"]["Table"]) > 0:
				# Prepare list to pushed in pulldb
				list_prov_cities = response["SourceStationList"]["Table"]
				list_cities=[ (process_id, prov_city["SourceStationID"], prov_city["SourceStationName"]) for prov_city in list_prov_cities ]				

				##Insert into db
				pulldb_config = self.__get_pulldb_config__()
				pulldb = db.DB( *pulldb_config )
				
				pulldb.execute_dml_bulk("""
						insert into SourceStations (
							process_id
							, SourceStationID
							, SourceStationName
						) values (%d,%s,%s)
					""",list_cities)
				return True
		except Exception as ex:
			print pulldb.db, pulldb.server

			print str(ex)
			raise Exception("Invalid XML response("+str(ex)+")")

	def pull_to_cities(self, from_city_id):
		"""
			author 		: sWaRtHi
			Date 		: October 10, 2014
			Description : Pull to_cities from api and return as string
		"""
		# fetching configurations from config file
		main_url = self.get_config("main_url")
		to_cities_api = self.get_config("to_cities_api")
		login_id = self.get_config("login_id")
		password = self.get_config("password")

		# Calling Web API method
		(response_code,raw_response)=self.__get_client__().GetDestinationStations(loginID=login_id,password=password,sourceStationID=from_city_id)
		
		if response_code==200:
			return raw_response

	def process_to_cities(self,process_id,from_city_id,from_city_name,str_response):
		"""
			author 		: sWaRtHi
			Date 		: October 10, 2014
			Description : Push To Cities to pulldb
		"""
		try:
			response = xmltodict.parse(str_response)
		except Exception, e:
			raise Exception("Invalid XML response!")
		
		# return response
		try:
			if "DestinationStationID" in response["DestinationStationList"]["Table"]:
				prov_city = response["DestinationStationList"]["Table"]
				list_cities=[ (process_id, from_city_id, from_city_name, prov_city["DestinationStationID"], prov_city["DestinationStationName"]) ]

				# return list_cities
				##Insert into db
				pulldb_config = self.__get_pulldb_config__()
				pulldb = db.DB( *pulldb_config )
				pulldb.execute_dml_bulk("""
						insert into DestinationStations (
							process_id
							, SourceStationID
							, SourceStationName
							, DestinationStationID
							, DestinationStationName
						) values (%d,%s,%s,%s,%s)
					""",list_cities)
				return True
			else:
				if len(response["DestinationStationList"]["Table"]) > 0:
					# Prepare list to pushed in pulldb
					list_prov_cities = response["DestinationStationList"]["Table"]
					list_cities=[ (process_id, from_city_id, from_city_name, prov_city["DestinationStationID"], prov_city["DestinationStationName"]) for prov_city in list_prov_cities ]				

					##Insert into db
					pulldb_config = self.__get_pulldb_config__()
					pulldb = db.DB( *pulldb_config )
					pulldb.execute_dml_bulk("""
							insert into DestinationStations (
								process_id
								, SourceStationID
								, SourceStationName
								, DestinationStationID
								, DestinationStationName
							) values (%d,%s,%s,%s,%s)
						""",list_cities)
					return True
		except Exception as ex:
			raise Exception("Invalid XML response("+str(ex)+")")

	def pull_routes(self,from_city_id,to_city_id,journey_date):
		"""
			author 		: sWaRtHi
			Date 		: January 07, 2015
			Description : Pull routes from api 
		"""
		# fetching configurations from config file
		main_url = self.get_config("main_url")
		route_api = self.get_config("route_api")
		login_id = self.get_config("login_id")
		password = self.get_config("password")

		# Calling Web API method
		(response_code,raw_response)=self.__get_client__().GetServices(loginID=login_id,password=password,sourceStationID=from_city_id,destinationStationID=to_city_id,onwardJourneyDate=journey_date,seatType=0)
		# if response success than return else rais exception
		# print response_code
		if response_code==200:
			return raw_response
		else:
			raise Exception("response code="+str(response_code))

	def process_routes(self,process_id,str_response):
		"""
			author 		: sWaRtHi
			Date 		: October 10, 2014
			Description : Push To Cities to pulldb
		"""
		try:
			# check route availability
			if "no service" not in str_response.lower():
				response = xmltodict.parse(str_response)
			else:
				raise provider_exceptions.Pull_Exc(str(str_response))

			# return response
			# return response["OnwardServiceList"]["Table"]
			# return len(response["OnwardServiceList"]["Table"])
			if "SourceStationID" in response["OnwardServiceList"]["Table"]:
				prov_route = response["OnwardServiceList"]["Table"]
				list_routes = []
				if int(prov_route["SeatAvailability"]) > 0:
					from_city_id 	= prov_route["SourceStationID"]
					from_city_name 	= prov_route["SourceStationName"]
					to_city_id 		= prov_route["DestinationStationID"]
					to_city_name 	= prov_route["DestinationStationName"]
					journey_date 	= prov_route["OnwardJourneyDate"]
					travel_partner 	= prov_route["TravelPartner"]
					service_id 		= prov_route["ServiceID"]
					service_no 		= prov_route["ServiceNumber"]
					departure_time 	= prov_route["DepartureTime"]
					service_contact = prov_route["ServiceContactNo"]
					coach_type 		= prov_route["CoachType"]
					coach_desc 		= prov_route["CoachTypeDescription"]
					coach_capacity 	= prov_route["CoachCapacity"]
					availability 	= prov_route["SeatAvailability"]
					ticket_fare 	= prov_route["TicketFare"]
					duration 		= prov_route["ApproxJourneyTime"]

					route_code = service_id\
								+'~'+service_no\
								+'~'+coach_type\
								+'~'+from_city_id\
								+'~'+to_city_id\
								+'~'+journey_date
						
					route=(
						process_id
						, route_code
						, from_city_id
						, from_city_name
						, to_city_id
						, to_city_name
						, journey_date
						, travel_partner
						, service_id
						, service_no
						, departure_time
						, service_contact
						, coach_type
						, coach_desc
						, coach_capacity
						, availability
						, ticket_fare
						, duration
						)
					list_routes.append(route)

				pulldb_config = self.__get_pulldb_config__()
				pulldb = db.DB( *pulldb_config )

				if len(list_routes) > 0:
					try:
						pulldb.execute_dml_bulk("""
							insert into OnwardServices ( 
								process_id
								, route_code
								, SourceStationID
								, SourceStationName
								, DestinationStationID
								, DestinationStationName
								, OnwardJourneyDate
								, TravelPartner
								, ServiceID
								, ServiceNumber
								, DepartureTime
								, ServiceContactNo
								, CoachType
								, CoachTypeDescription
								, CoachCapacity
								, SeatAvailability
								, TicketFare
								, ApproxJourneyTime
							) values (%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
							""",list_routes)
					except Exception as ex:
						raise Exception("Exception while pushing routes in db: "+str(ex))
					return True
				else:
					return False
			else:
				# Prepare list to pushed in pulldb
				prov_routes = response["OnwardServiceList"]["Table"]
				list_routes = []
				# list_cities=[ (process_id, int(list_prov_routes["DestinationStationID"]), int(prov_city["DestinationStationID"]), prov_city["DestinationStationName"]) for prov_city in list_prov_cities ]
				for prov_route in prov_routes:
					if int(prov_route["SeatAvailability"]) > 0:
						
						from_city_id 	= prov_route["SourceStationID"]
						from_city_name 	= prov_route["SourceStationName"]
						to_city_id 		= prov_route["DestinationStationID"]
						to_city_name 	= prov_route["DestinationStationName"]
						journey_date 	= prov_route["OnwardJourneyDate"]
						travel_partner 	= prov_route["TravelPartner"]
						service_id 		= prov_route["ServiceID"]
						service_no 		= prov_route["ServiceNumber"]
						departure_time 	= prov_route["DepartureTime"]
						service_contact = prov_route["ServiceContactNo"]
						coach_type 		= prov_route["CoachType"]
						coach_desc 		= prov_route["CoachTypeDescription"]
						coach_capacity 	= prov_route["CoachCapacity"]
						availability 	= prov_route["SeatAvailability"]
						ticket_fare 	= prov_route["TicketFare"]
						duration 		= prov_route["ApproxJourneyTime"]

						route_code = service_id\
									+'~'+service_no\
									+'~'+coach_type\
									+'~'+from_city_id\
									+'~'+to_city_id\
									+'~'+journey_date
						
						route=(
							process_id
							, route_code
							, from_city_id
							, from_city_name
							, to_city_id
							, to_city_name
							, journey_date
							, travel_partner
							, service_id
							, service_no
							, departure_time
							, service_contact
							, coach_type
							, coach_desc
							, coach_capacity
							, availability
							, ticket_fare
							, duration
							)
						list_routes.append(route)

				# return list_routes
				pulldb_config = self.__get_pulldb_config__()
				pulldb = db.DB( *pulldb_config )

				if len(list_routes) > 0:
					try:
						pulldb.execute_dml_bulk("""
							insert into OnwardServices ( 
								process_id
								, route_code
								, SourceStationID
								, SourceStationName
								, DestinationStationID
								, DestinationStationName
								, OnwardJourneyDate
								, TravelPartner
								, ServiceID
								, ServiceNumber
								, DepartureTime
								, ServiceContactNo
								, CoachType
								, CoachTypeDescription
								, CoachCapacity
								, SeatAvailability
								, TicketFare
								, ApproxJourneyTime
							) values (%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
							""",list_routes)
					except Exception as ex:
						raise Exception("Exception while pushing routes in db: "+str(ex))
					return True
				else:
					return False

		except Exception as ex:
			raise Exception("Invalid XML response("+str(ex)+")")

	def pull_pickups(self,route_code):
		"""
			author 		: sWaRtHi
			Date 		: January 12, 2015
			Description : Pull routewise pickups from API
		"""
		# fetching configurations from config file
		main_url = self.get_config("main_url")
		pickup_api = self.get_config("pickup_api")
		login_id = self.get_config("login_id")
		password = self.get_config("password")

		# getting param for pickup api from route code
		# route_code = service_id+'~'+service_no+'~'+coach_type+'~'+from_city_id+'~'+to_city_id+'~'+journey_date
		pickup_param = route_code.split("~")
		# Calling Web API method
		(response_code,raw_response)=self.__get_client__().GetBoardingPoints(loginID=login_id,password=password,journeyDate=pickup_param[5],serviceID=pickup_param[0],sourceStationID=pickup_param[3],destinationStationID=pickup_param[4])
		# if response success than return else rais exception
		# print response_code
		if response_code==200:
			return raw_response
		else:
			raise Exception("response code="+str(response_code))

	def process_pickups(self,process_id,route_code,str_response):
		"""
			author 		: sWaRtHi
			Date 		: October 10, 2014
			Description : Push Service's pickups to pulldb
		"""
		# check pickup availability
		if "not exist" not in str_response.lower():
			response = xmltodict.parse(str_response)
		else:
			raise provider_exceptions.Pull_Exc(str(str_response))

		try:
			# return response["BoardingPointsList"]["Table"]
			# return response["BoardingPointsList"]["Table"][0]["ArrivalTime"].replace(response["BoardingPointsList"]["Table"][0]["BoardingPointName"],'').strip()
			# return "BoardingPointID" in response["BoardingPointsList"]["Table"]
			if "BoardingPointID" in response["BoardingPointsList"]["Table"]:
				prov_pickup = response["BoardingPointsList"]["Table"]
				list_pickups = []
				
				service_id 		= prov_pickup["ServiceID"]
				service_no 		= prov_pickup["ServiceNumber"]
				pickup_id 		= prov_pickup["BoardingPointID"]
				pickup_name 	= prov_pickup["BoardingPointName"]
				pickup_address 	= prov_pickup["BoardingPointAddress"]
				pickup_landmark	= prov_pickup["BoardingPointLandmark"]
				pickup_contact 	= prov_pickup["BoardingPointContactNo"]
				pickup_time 	= prov_pickup["ArrivalTime"]#.replace(prov_pickup["BoardingPointName"],'').strip()
				pickup_date 	= prov_pickup["JourneyDate"]
				from_city_name 	= prov_pickup["SourceStationName"]
				
				pickup=(
					process_id
					, route_code
					, service_id
					, service_no
					, pickup_id
					, pickup_name
					, pickup_address
					, pickup_landmark
					, pickup_contact
					, pickup_time
					, pickup_date
					, from_city_name
				)
				list_pickups.append(pickup)

				pulldb_config = self.__get_pulldb_config__()
				pulldb = db.DB( *pulldb_config )

				if len(list_pickups) > 0:
					try:
						pulldb.execute_dml_bulk("""
							insert into BoardingPoints ( 
								process_id
								, route_code
								, ServiceID
								, ServiceNumber
								, BoardingPointID
								, BoardingPointName
								, BoardingPointAddress
								, BoardingPointLandmark
								, BoardingPointContactNo
								, ArrivalTime
								, JourneyDate
								, SourceStationName
							) values (%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
							""",list_pickups)
					except Exception as ex:
						raise Exception("Exception while pushing pickup in db: "+str(ex))
					# return True
				else:
					raise Exception("Unable to push to cities for "+str(from_city_id)+"|"+str(from_city_name))
					# return False
			else:
				# Prepare list to pushed in pulldb
				prov_pickups = response["BoardingPointsList"]["Table"]
				list_pickups = []
				# list_cities=[ (process_id, int(list_prov_routes["DestinationStationID"]), int(prov_city["DestinationStationID"]), prov_city["DestinationStationName"]) for prov_city in list_prov_cities ]
				for prov_pickup in prov_pickups:
					# gettng field from response to push into list
					service_id 		= prov_pickup["ServiceID"]
					service_no 		= prov_pickup["ServiceNumber"]
					pickup_id 		= prov_pickup["BoardingPointID"]
					pickup_name 	= prov_pickup["BoardingPointName"]
					pickup_address 	= prov_pickup["BoardingPointAddress"]
					pickup_landmark	= prov_pickup["BoardingPointLandmark"]
					pickup_contact 	= prov_pickup["BoardingPointContactNo"]
					pickup_time 	= prov_pickup["ArrivalTime"]#.strip()#.replace(prov_pickup["BoardingPointName"],'').strip()
					pickup_date 	= prov_pickup["JourneyDate"]
					from_city_name 	= prov_pickup["SourceStationName"]

					# adding items in list
					pickup=(
						process_id
						, route_code
						, service_id
						, service_no
						, pickup_id
						, pickup_name
						, pickup_address
						, pickup_landmark
						, pickup_contact
						, pickup_time
						, pickup_date
						, from_city_name
					)
					list_pickups.append(pickup)

				# return list_pickups
				pulldb_config = self.__get_pulldb_config__()
				pulldb = db.DB( *pulldb_config )

				if len(list_pickups) > 0:
					try:
						pulldb.execute_dml_bulk("""
							insert into BoardingPoints ( 
								process_id
								, route_code
								, ServiceID
								, ServiceNumber
								, BoardingPointID
								, BoardingPointName
								, BoardingPointAddress
								, BoardingPointLandmark
								, BoardingPointContactNo
								, ArrivalTime
								, JourneyDate
								, SourceStationName
							) values (%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
							""",list_pickups)
					except Exception as ex:
						raise Exception("Exception while pushing pickups in db: "+str(ex))
					return True
				else:
					raise Exception("Unable to push to cities for "+str(from_city_id)+"|"+str(from_city_name))
					# return False
		except Exception as ex:
			raise Exception("Error : ("+str(ex)+")")

	def pull_dropoffs(self,route_code):
		"""
			author 		: sWaRtHi
			Date 		: January 12, 2015
			Description : Pull routewise dropoffs from API
		"""
		# fetching configurations from config file
		main_url = self.get_config("main_url")
		dropoff_api = self.get_config("dropoff_api")
		login_id = self.get_config("login_id")
		password = self.get_config("password")

		# getting param for dropoff API method from route code
		# route_code = service_id+'~'+service_no+'~'+coach_type+'~'+from_city_id+'~'+to_city_id+'~'+journey_date
		dropoff_param = route_code.split("~")
		# Calling Web API method
		(response_code,raw_response)=self.__get_client__().GetDroppingPoints(loginID=login_id,password=password,journeyDate=dropoff_param[5],serviceID=dropoff_param[0],sourceStationID=dropoff_param[3],destinationStationID=dropoff_param[4])
		# if response success than return else rais exception
		# print response_code
		if response_code==200:
			return raw_response
		else:
			raise Exception("response code="+str(response_code))

	def process_dropoffs(self,process_id,route_code,str_response):
		"""
			author 		: sWaRtHi
			Date 		: October 10, 2014
			Description : Push Service's dropoffs to pulldb
		"""
		# check pickup availability
		if "not exist" not in str_response.lower():
			response = xmltodict.parse(str_response)
		else:
			raise provider_exceptions.Pull_Exc(str(str_response))

		try:
			# return response["BoardingPointsList"]["Table"][0]["ArrivalTime"].replace(response["BoardingPointsList"]["Table"][0]["BoardingPointName"],'').strip()
			# return response["DroppingPointsList"]["Table"]
			if "DroppingPointID" in response["DroppingPointsList"]["Table"]:
				prov_dropoff = response["DroppingPointsList"]["Table"]
				list_dropoffs = []
				
				service_id 		= prov_dropoff["ServiceID"]
				service_no 		= prov_dropoff["ServiceNumber"]
				dropoff_id 		= prov_dropoff["DroppingPointID"]
				dropoff_name 	= prov_dropoff["DroppingPointName"]
				dropoff_address = prov_dropoff["DroppingPointAddress"]
				dropoff_landmark= prov_dropoff["DroppingPointLandmark"]
				dropoff_date 	= prov_dropoff["JourneyDate"]
				to_city_name 	= prov_dropoff["DestinationStationName"]

				dropoff=(
					process_id
					, route_code
					, service_id
					, service_no
					, dropoff_id
					, dropoff_name
					, dropoff_address
					, dropoff_landmark
					, dropoff_date
					, to_city_name
				)
				list_dropoffs.append(dropoff)

				pulldb_config = self.__get_pulldb_config__()
				pulldb = db.DB( *pulldb_config )

				if len(list_dropoffs) > 0:
					try:
						pulldb.execute_dml_bulk("""
							insert into DroppingPoints ( 
								process_id
								, route_code
								, ServiceID
								, ServiceNumber
								, DroppingPointID
								, DroppingPointName
								, DroppingPointAddress
								, DroppingPointLandmark
								, JourneyDate
								, DestinationStationName
							) values (%d,%s,%s,%s,%s,%s,%s,%s,%s,%s)
							""",list_dropoffs)
					except Exception as ex:
						raise Exception("Exception while pushing dropoff in db: "+str(ex))
					return True
				else:
					return False
			else:
				# return "multi"
				# Prepare list to pushed in pulldb
				prov_dropoffs = response["DroppingPointsList"]["Table"]
				list_dropoffs = []
				# list_cities=[ (process_id, int(list_prov_routes["DestinationStationID"]), int(prov_city["DestinationStationID"]), prov_city["DestinationStationName"]) for prov_city in list_prov_cities ]
				for prov_dropoff in prov_dropoffs:
					# gettng field from response to push into list
					service_id 		= prov_dropoff["ServiceID"]
					service_no 		= prov_dropoff["ServiceNumber"]
					dropoff_id 		= prov_dropoff["DroppingPointID"]
					dropoff_name 	= prov_dropoff["DroppingPointName"]
					dropoff_address = prov_dropoff["DroppingPointAddress"]
					dropoff_landmark= prov_dropoff["DroppingPointLandmark"]
					dropoff_date 	= prov_dropoff["JourneyDate"]
					to_city_name 	= prov_dropoff["DestinationStationName"]

					# adding items in list
					dropoff=(
						process_id
						, route_code
						, service_id
						, service_no
						, dropoff_id
						, dropoff_name
						, dropoff_address
						, dropoff_landmark
						, dropoff_date
						, to_city_name
					)
					list_dropoffs.append(dropoff)

				# return list_pickups
				pulldb_config = self.__get_pulldb_config__()
				pulldb = db.DB( *pulldb_config )

				if len(list_dropoffs) > 0:
					try:
						pulldb.execute_dml_bulk("""
							insert into DroppingPoints ( 
								process_id
								, route_code
								, ServiceID
								, ServiceNumber
								, DroppingPointID
								, DroppingPointName
								, DroppingPointAddress
								, DroppingPointLandmark
								, JourneyDate
								, DestinationStationName
							) values (%d,%s,%s,%s,%s,%s,%s,%s,%s,%s)
							""",list_dropoffs)
					except Exception as ex:
						raise Exception("Exception while pushing dropoffs in db: "+str(ex))
					return True
				else:
					return False

		except Exception as ex:
			raise Exception("Invalid XML response("+str(ex)+")")

	def process_data(self,process_id):
		process_init = self.get_config("process_init")
		pulldb_config = self.__get_pulldb_config__()
		pulldb=db.DB( *pulldb_config )
		return pulldb.execute_sp(process_init,(process_id,),commit=True)

##--------------------------Class ValueLabs_API Ends------------------------

def get_from_cities(process_id):
	global DEFAULT_SECTION
	api=AshokaVL_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)
	# Pull from citeis from API
	try:
		response=api.pull_from_cities()
	except Exception, ex:
		raise provider_exceptions.Pull_Exc(str(ex))

	# Push pulled citeis into pulldb
	try:
		api.process_from_cities(process_id,response)
	except Exception, ex:
		raise provider_exceptions.Process_Exc(str(ex))

	return xmltodict.parse(response)

def get_to_cities(process_id,from_city_response):
	global DEFAULT_SECTION
	api=AshokaVL_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)
	# Get from city list from response to process to city for each from city_pairs
	# return len(from_city_response["SourceStationList"]["Table"])
	if len(from_city_response["SourceStationList"]["Table"])<=0:
		raise Exception("no result from get from cities")
	# from_city_list=[(station["SourceStationID"],station["SourceStationName"]) for station in from_city_response["SourceStationList"]["Table"]]
	try:
		for station in from_city_response["SourceStationList"]["Table"]:#from_city_list:
			# Pulling To Cities from API
			# print "pulling to cities for : "+str(station)
			try:
				response=api.pull_to_cities(station["SourceStationID"])
			except Exception, ex:
				raise provider_exceptions.Pull_Exc(str(ex))
			# Push pulled to citeis into pulldb
			# print "pushing to cities for : "+str(station)
			try:
				api.process_to_cities(process_id,station["SourceStationID"],station["SourceStationName"],response)
				# return api.process_to_cities(process_id,station["SourceStationID"],station["SourceStationName"],response)
			except Exception, ex:
				raise provider_exceptions.Process_Exc(str(ex))
		return True
	except Exception, ex:
		raise Exception("Unable to get to cities : "+str(ex))

def get_city_pairs(process_id):
	global DEFAULT_SECTION
	api=AshokaVL_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)
	pulldb_config = api.__get_pulldb_config__()
	pulldb = db.DB( *pulldb_config )
	return pulldb.execute_query("""
		select distinct cp.SourceStationID as from_city_id
			, cp.DestinationStationID as to_city_id
		from DestinationStations cp with(nolock)
		where cp.process_id = %d
		""" %process_id)

def get_routes(process_id,from_city_id,to_city_id,journey_date):
	global DEFAULT_SECTION
	api=AshokaVL_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)
	
	jd = datetime.strptime(journey_date,"%Y-%m-%d")
	str_journey_date=datetime.strftime(jd,"%m/%d/%Y")

	# PUlling routes
	try:
		response=api.pull_routes(from_city_id,to_city_id,str_journey_date)
	except Exception, ex:
		raise provider_exceptions.Pull_Exc(str(ex))
	# return xmltodict.parse(response)
	# Process routes
	try:
		return api.process_routes(process_id,response)
	except Exception, ex:
		raise provider_exceptions.Process_Exc(str(ex))

def get_route_codes(process_id):
	global DEFAULT_SECTION
	api=AshokaVL_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)
	pulldb_config = api.__get_pulldb_config__()
	pulldb = db.DB( *pulldb_config )
	return pulldb.execute_query("""
		select distinct route_code as route_code
		from OnwardServices os with(nolock)
		where os.process_id = %d
		""" %process_id)

def get_pickups(process_id,route_code):
	global DEFAULT_SECTION
	api=AshokaVL_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)

	# Pulling pickups
	try:
		response=api.pull_pickups(route_code)
	except Exception, ex:
		raise provider_exceptions.Pull_Exc(str(ex))
	# Processing pickups
	try:
		return api.process_pickups(process_id,route_code,response)
	except Exception, ex:
		raise provider_exceptions.Process_Exc(str(ex))

def get_dropoffs(process_id,route_code):
	global DEFAULT_SECTION
	api=AshokaVL_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)

	# Pulling dropoffs
	try:
		response=api.pull_dropoffs(route_code)
	except Exception, ex:
		raise provider_exceptions.Pull_Exc(str(ex))
	# Processing pickups
	try:
		return api.process_dropoffs(process_id,route_code,response)
	except Exception, ex:
		raise provider_exceptions.Process_Exc(str(ex))

def process_data(process_id):
	global DEFAULT_SECTION
	api=AshokaVL_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)
	try:
		return api.process_data(process_id)
	except Exception as e:
		raise provider_exceptions.Process_Exc(str(e))	

def get_response(str_api_name,*args,**kwrds):
	global DEFAULT_SECTION
	api=AshokaVL_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)

	if hasattr(api,str_api_name):
		try:
			api_method=getattr(api,str_api_name)
			response=api_method(*args,**kwrds)
			return response
		except Exception as ex:
			raise provider_exceptions.Pull_Exc(str(ex))
	else:
		raise Exception("No Such API Method !")

if __name__== "__main__":
	vl = AshokaVL_API(DEFAULT_SECTION)
	print json.dumps(xmltodict.parse(vl.pull_from_cities()),indent=4)
	# print json.dumps(vl.process_from_cities(0,vl.pull_from_cities()),indent=4)
	# print json.dumps(xmltodict.parse(vl.pull_to_cities("1")),indent=4)
	# print json.dumps(vl.process_to_cities(0,"11","Calicut",vl.pull_to_cities("11")),indent=4)
	# print json.dumps(vl.process_to_cities(0,"1","Bangalore",vl.pull_to_cities("1")),indent=4)
	# print json.dumps(xmltodict.parse(vl.pull_routes("1","2","02/19/2015")),indent=4)
	# print vl.pull_routes("10","1","01/20/2015")
	# print json.dumps(vl.process_routes(0,vl.pull_routes("1","2","01/25/2015")),indent=4)
	# print json.dumps(vl.process_routes(0,vl.pull_routes("1","11","01/25/2015")),indent=4)
	# print json.dumps(xmltodict.parse(vl.pull_pickups("7~BNG-KNR007~2~1~9~2/6/2015")),indent=4)
	# print json.dumps(xmltodict.parse(vl.pull_pickups("7~BNG-KNR007~1~2~1/25/2015")),indent=4)
	# print json.dumps(xmltodict.parse(vl.pull_pickups("7~BNG-KNR007~1~9~1/24/2015")),indent=4)
	# print json.dumps(vl.process_pickups(0,"7~BNG-KNR007~1~2~1/25/2015",vl.pull_pickups("7~BNG-KNR007~1~2~1/25/2015")),indent=4)
	# print json.dumps(vl.process_pickups(0,"7~BNG-KNR007~1~9~1/24/2015",vl.pull_pickups("7~BNG-KNR007~1~9~1/24/2015")),indent=4)
	# print vl.pull_dropoffs("1~BNG-KNR001~1~2~1/21/2015")
	# print json.dumps(vl.pull_dropoffs("1~BNG-KNR001~1~2~1/23/2015"),indent=4)
	# print json.dumps(xmltodict.parse(vl.pull_dropoffs("2~BNG-KNR002~1~3~2/8/2015")),indent=4)
	# print json.dumps(vl.process_dropoffs(0,"1~BNG-KNR001~1~2~1/21/2015",vl.pull_dropoffs("1~BNG-KNR001~1~2~1/21/2015")),indent=4)
	
	# print json.dumps(get_from_cities(1),indent=4)
	# print json.dumps(get_to_cities(01,"10","Thalassery"),indent=4)
	# print json.dumps(get_to_cities(5,get_from_cities(5)),indent=4)
	# print json.dumps(get_city_pairs(1),indent=4)
	# print json.dumps(get_routes(0,"10","1","2015-01-20"),indent=4)
	# print json.dumps(get_route_codes(1),indent=4)
	# print json.dumps(get_pickups(53327030,"7~BNG-KNR007~2~1~9~2/6/2015"),indent=4)
	# print json.dumps(get_pickups(1,"7~BNG-KNR007~1~3~1/21/2015"),indent=4)
	# print json.dumps(get_dropoffs(1,"1~BNG-KNR001~1~2~1/21/2015"),indent=4)

	# select distinct * from city_pairs WITH (NOLOCK) where process_id=%d
	# select distinct route_code as route_code from OnwardServices os with(nolock) where os.process_id = 0
	pass