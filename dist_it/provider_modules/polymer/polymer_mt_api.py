# author: sWaRtHi
# date: February 06, 2015
# description: Provider function modules

from helpers import db,provider_exceptions
import os,httplib2,xmltodict#,json,urllib,re,time
from suds.client import Client
# from datetime import datetime

CONFIG_FILE="polymer_mt_config.ini"
DEFAULT_SECTION="prod"

class PolymerMT_API(object):
	"""docstring for PolymerMT_API"""
	def __init__(self, section):
		super(PolymerMT_API, self).__init__()
		try:
			import ConfigParser
			global CONFIG_FILE
			self.section = section
			self.config=ConfigParser.ConfigParser()
			path = os.path.join(os.path.dirname(os.path.abspath(__file__)),CONFIG_FILE)
			self.config.read(path)
			if not (section in self.config.sections()):
				raise Exception("Section "+section+" not Found !")
			self.loaded=True
		except Exception as ex:
			self.loaded=False
			self.loading_error=str(ex)

	def get_config(self,key):
		return self.config.get(self.section,key)

	def __get_pulldb_config__(self):
		try:
			server 		= self.get_config("pulldb_server")
			db_name 	= self.get_config("pulldb_db")
			user 		= self.get_config("pulldb_user")
			password 	= self.get_config("pulldb_password")
			return (server,db_name,user,password)
		except Exception, ex:
			raise Exception("Invalid PullDB Conffiguration !|"+str(ex))

	def __get_client__(self):
		main_url = self.get_config("main_url")

		if not hasattr(self,'client') or self.client == None:
			self.client=Client(main_url,faults=False)
		#fetching from API
		return self.client.service

	def pull_from_cities(self):
		"""
			author 		: sWaRtHi
			Date 		: February 06, 2015
			Description : Pull From Cities from api 
		"""
		# fetching configurations from config file
		main_url 	= self.get_config("main_url")
		user_name	= self.get_config("user_name")
		password 	= self.get_config("password")
		# from_cities_api = self.get_config("from_cities_api")

		# Calling Web API method
		(response_code,raw_response)=self.__get_client__().GetSources(strUserName=user_name,strPassword=password)
		# if response success than return else rais exception
		
		if response_code==200:
			return raw_response
		else:
			raise Exception("response code="+str(response_code))

	def process_from_cities(self,process_id,str_response):
		"""
			author 		: sWaRtHi
			Date 		: February 09, 2015
			Description : Push From Cities to pulldb
		"""
		try:
			response = xmltodict.parse(str_response)
		except Exception, e:
			raise Exception("Invalid XML response!")
		# return response
		try:
			if len(response["NewDataSet"]["Table"]) > 0:
				# Prepare list to pushed in pulldb
				list_prov_cities = response["NewDataSet"]["Table"]
				list_cities=[ (process_id, prov_city["SourceId"], prov_city["Source"]) for prov_city in list_prov_cities ]				

				##Insert into db
				pulldb_config = self.__get_pulldb_config__()
				pulldb = db.DB( *pulldb_config )
				
				pulldb.execute_dml_bulk("""
						insert into Sources (
							process_id
							, SourceId
							, Source
						) values (%d,%s,%s)
					""",list_cities)
				return True
		except Exception as ex:
			raise Exception("Invalid XML response("+str(ex)+")")

	def pull_to_cities(self, from_city_id):
		"""
			author 		: sWaRtHi
			Date 		: February 09, 2015
			Description : Pull to_cities from api and return as string
		"""
		# fetching configurations from config file
		main_url  = self.get_config("main_url")
		user_name = self.get_config("user_name")
		password  = self.get_config("password")
		# to_cities_api = self.get_config("to_cities_api")

		# Calling Web API method
		(response_code,raw_response)=self.__get_client__().GetDestinations(strUserName=user_name,strPassword=password,intSourceId=from_city_id)

		if response_code==200:
			return raw_response

	def process_to_cities(self,process_id,from_city_id,from_city_name,str_response):
		"""
			author 		: sWaRtHi
			Date 		: February 09, 2015
			Description : Push To Cities to pulldb
		"""
		try:
			response = xmltodict.parse(str_response)
		except Exception, e:
			raise Exception("Invalid XML response!")

		try:
			# print response["NewDataSet"]["Table"]
			if response["NewDataSet"] is not None:
				# raise provider_exceptions.Pull_Exc("No to  cities: for from_city{"+from_city_id+","+from_city_name+"} "+str(str_response))
			# else:
				if "DestinationId" in response["NewDataSet"]["Table"]:

					prov_city = response["NewDataSet"]["Table"]
					list_cities=[ (process_id, from_city_id, from_city_name, prov_city["DestinationId"], prov_city["Destination"]) ]

					# return list_cities
					##Insert into db
					pulldb_config = self.__get_pulldb_config__()
					pulldb = db.DB( *pulldb_config )
					pulldb.execute_dml_bulk("""
							insert into Destinations (
								process_id
								, SourceId
								, Source
								, DestinationId
								, Destination
							) values (%d,%s,%s,%s,%s)
						""",list_cities)
					return True
				else:
					if len(response["NewDataSet"]["Table"]) > 0:
						# Prepare list to pushed in pulldb
						list_prov_cities = response["NewDataSet"]["Table"]
						list_cities=[ (process_id, from_city_id, from_city_name, prov_city["DestinationId"], prov_city["Destination"]) for prov_city in list_prov_cities ]				

						##Insert into db
						pulldb_config = self.__get_pulldb_config__()
						pulldb = db.DB( *pulldb_config )
						pulldb.execute_dml_bulk("""
								insert into Destinations (
									process_id
									, SourceId
									, Source
									, DestinationId
									, Destination
								) values (%d,%s,%s,%s,%s)
							""",list_cities)
						return True
		except Exception as ex:
			raise Exception("Invalid XML response("+str(ex)+")")

	def pull_routes(self,from_city_id,to_city_id,journey_date):
		"""
			author 		: sWaRtHi
			Date 		: February 09, 2015
			Description : Pull routes from api 
		"""
		# fetching configurations from config file
		main_url  = self.get_config("main_url")
		user_name = self.get_config("user_name")
		password  = self.get_config("password")
		# route_api = self.get_config("route_api")

		# Calling Web API method
		(response_code,raw_response)=self.__get_client__().GetRoutes(strUserName=user_name,strPassword=password,intSourceId=from_city_id,intDestinationId=to_city_id,dtTravelDate=journey_date)
		# if response success than return else rais exception
		# print response_code
		if response_code==200:
			return raw_response
		else:
			raise Exception("response code="+str(response_code))

	def process_routes(self,process_id,from_city_name,to_city_name,journey_date,str_response):
		"""
			author 		: sWaRtHi
			Date 		: February 10, 2015
			Description : Push routes to pulldb
		"""
		try:
			# check route availability
			# if response["NewDataSet"] is not None:
			response = xmltodict.parse(str_response)
			if response["NewDataSet"] is not None:
				# check for single or multiple results 
				if "SourceID" in response["NewDataSet"]["Table"]:
					prov_route = response["NewDataSet"]["Table"]
					list_routes = []
					if int(prov_route["AvailableSeats"]) > 0:
						from_city_id 	= prov_route["SourceID"]
						to_city_id 		= prov_route["DestinationID"]
						route_id 		= prov_route["RouteID"]
						prov_route_code	= prov_route["RouteCode"]
						route_name		= prov_route["Route"]
						bus_type 		= prov_route["BusType"]
						departure_time	= prov_route["DepartureTime"]
						arrival_time 	= prov_route["ArrivalTime"]
						fare 			= prov_route["Fare"]
						seats 			= prov_route["NoOfSeats"]
						available_seats	= prov_route["AvailableSeats"]
						rows			= prov_route["NoOfRows"]
						columns			= prov_route["NoOfColumns"]
						layers			= prov_route["NoOfLayers"]

						route_code = route_id\
								+'~'+from_city_id\
								+'~'+to_city_id\
								+'~'+journey_date

						route=(
							process_id			# param
							, route_code
							, from_city_name	# param
							, to_city_name		# param
							, journey_date		# param
							, from_city_id
							, to_city_id
							, route_id
							, prov_route_code
							, route_name
							, bus_type
							, departure_time
							, arrival_time
							, fare
							, seats
							, available_seats
							, rows
							, columns
							, layers
							)
						list_routes.append(route)

					pulldb_config = self.__get_pulldb_config__()
					pulldb = db.DB( *pulldb_config )

					if len(list_routes) > 0:
						try:
							pulldb.execute_dml_bulk("""
								insert into Routes ( 
									process_id
									, route_code
									, Source
									, Destination
									, dtTravelDate
									, SourceID
									, DestinationID
									, RouteID
									, RouteCode
									, Route
									, BusType
									, DepartureTime
									, ArrivalTime
									, Fare
									, NoOfSeats
									, AvailableSeats
									, NoOfRows
									, NoOfColumns
									, NoOfLayers
								) values (%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
								""",list_routes)
						except Exception as ex:
							raise Exception("Exception while pushing routes in db: "+str(ex))
						return True
				else:
					prov_routes = response["NewDataSet"]["Table"]
					list_routes = []
					for prov_route in prov_routes:
						if int(prov_route["AvailableSeats"]) > 0:
							from_city_id 	= prov_route["SourceID"]
							to_city_id 		= prov_route["DestinationID"]
							route_id 		= prov_route["RouteID"]
							prov_route_code	= prov_route["RouteCode"]
							route_name		= prov_route["Route"]
							bus_type 		= prov_route["BusType"]
							departure_time	= prov_route["DepartureTime"]
							arrival_time 	= prov_route["ArrivalTime"]
							fare 			= prov_route["Fare"]
							seats 			= prov_route["NoOfSeats"]
							available_seats	= prov_route["AvailableSeats"]
							rows			= prov_route["NoOfRows"]
							columns			= prov_route["NoOfColumns"]
							layers			= prov_route["NoOfLayers"]

							route_code = route_id\
									+'~'+from_city_id\
									+'~'+to_city_id\
									+'~'+journey_date

							route=(
								process_id			# param
								, route_code
								, from_city_name	# param
								, to_city_name		# param
								, journey_date		# param
								, from_city_id
								, to_city_id
								, route_id
								, prov_route_code
								, route_name
								, bus_type
								, departure_time
								, arrival_time
								, fare
								, seats
								, available_seats
								, rows
								, columns
								, layers
								)
							list_routes.append(route)

					pulldb_config = self.__get_pulldb_config__()
					pulldb = db.DB( *pulldb_config )

					if len(list_routes) > 0:
						try:
							pulldb.execute_dml_bulk("""
								insert into Routes ( 
									process_id
									, route_code
									, Source
									, Destination
									, dtTravelDate
									, SourceId
									, DestinationID
									, RouteID
									, RouteCode
									, Route
									, BusType
									, DepartureTime
									, ArrivalTime
									, Fare
									, NoOfSeats
									, AvailableSeats
									, NoOfRows
									, NoOfColumns
									, NoOfLayers
								) values (%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
								""",list_routes)
						except Exception as ex:
							raise Exception("Exception while pushing routes in db: "+str(ex))
						return True
					else:
						return False
			else:
				raise provider_exceptions.Pull_Exc("No route from "+from_city_name+" to "+to_city_name+" on "+journey_date+"} "+str(str_response))
		except Exception, ex:
			raise Exception("Invalid XML response("+str(ex)+")")

	def pull_pickups(self,route_code):
		"""
			author 		: sWaRtHi
			Date 		: February 10, 2015
			Description : Pull routewise pickups from API
		"""
		# fetching configurations from config file
		main_url  = self.get_config("main_url")
		user_name = self.get_config("user_name")
		password  = self.get_config("password")
		# pickup_api = self.get_config("pickup_api")

		# getting param for pickup api from route code
		# route_code = route_id+'~'+from_city_id+'~'+to_city_id+'~'+journey_date
		pickup_param = route_code.split("~")
		# Calling Web API method
		(response_code,raw_response)=self.__get_client__().GetBoardingPlaces(strUserName=user_name,strPassword=password,intRouteId=pickup_param[0],intPlaceId=pickup_param[1])
		# if response success than return else rais exception
		# print response_code
		if response_code==200:
			return raw_response
		else:
			raise Exception("response code="+str(response_code))

	def process_pickups(self,process_id,route_code,str_response):
		"""
			author 		: sWaRtHi
			Date 		: February 10, 2015
			Description : Push Route's pickups to pulldb
		"""
		# check pickup availability
		try:
			response = xmltodict.parse(str_response)
			if response["NewDataSet"] is not None:
				if "BoardingPlaceID" in response["NewDataSet"]["Table"]:
					prov_pickup = response["NewDataSet"]["Table"]
					list_pickups = []
					
					pickup_id 			= prov_pickup["BoardingPlaceID"]
					pickup_name 		= prov_pickup["BoardingPlace"]
					pickup_report_time 	= prov_pickup["ReportingTime"]
					pickup_depart_time 	= prov_pickup["DepartureTime"]#.replace(prov_pickup["BoardingPointName"],'').strip()
					pickup_address 		= prov_pickup["Address"]
					pickup_landmark		= prov_pickup["Landmark"]
					pickup_contact 		= prov_pickup["PhoneNumber"]
					pickup_order 		= prov_pickup["SortOrder"]
					
					pickup=(
						process_id
						, route_code
						, pickup_id
						, pickup_name
						, pickup_report_time
						, pickup_depart_time
						, pickup_address
						, pickup_landmark
						, pickup_contact
						, pickup_order
					)
					list_pickups.append(pickup)

					pulldb_config = self.__get_pulldb_config__()
					pulldb = db.DB( *pulldb_config )

					if len(list_pickups) > 0:
						try:
							pulldb.execute_dml_bulk("""
								insert into BoardingPlaces ( 
									process_id
									, route_code
									, BoardingPlaceID
									, BoardingPlace
									, ReportingTime
									, DepartureTime
									, Address
									, Landmark
									, PhoneNumber
									, SortOrder
								) values (%d,%s,%s,%s,%s,%s,%s,%s,%s,%s)
								""",list_pickups)
						except Exception as ex:
							raise Exception("Exception while pushing pickup in db: "+str(ex))
						return True
					else:
						return False
				else:
					# Prepare list to pushed in pulldb
					prov_pickups = response["NewDataSet"]["Table"]
					list_pickups = []
					# list_cities=[ (process_id, int(list_prov_routes["DestinationStationID"]), int(prov_city["DestinationStationID"]), prov_city["DestinationStationName"]) for prov_city in list_prov_cities ]
					for prov_pickup in prov_pickups:
						# gettng field from response to push into list
						pickup_id 			= prov_pickup["BoardingPlaceID"]
						pickup_name 		= prov_pickup["BoardingPlace"]
						pickup_report_time 	= prov_pickup["ReportingTime"]
						pickup_depart_time 	= prov_pickup["DepartureTime"]#.replace(prov_pickup["BoardingPointName"],'').strip()
						pickup_address 		= prov_pickup["Address"]
						pickup_landmark		= prov_pickup["Landmark"]
						pickup_contact 		= prov_pickup["PhoneNumber"]
						pickup_order 		= prov_pickup["SortOrder"]

						# adding items in list
						pickup=(
							process_id
							, route_code
							, pickup_id
							, pickup_name
							, pickup_report_time
							, pickup_depart_time
							, pickup_address
							, pickup_landmark
							, pickup_contact
							, pickup_order
						)
						list_pickups.append(pickup)

						# return list_pickups
						pulldb_config = self.__get_pulldb_config__()
						pulldb = db.DB( *pulldb_config )

						if len(list_pickups) > 0:
							try:
								pulldb.execute_dml_bulk("""
									insert into BoardingPlaces ( 
										process_id
										, route_code
										, BoardingPlaceID
										, BoardingPlace
										, ReportingTime
										, DepartureTime
										, Address
										, Landmark
										, PhoneNumber
										, SortOrder
									) values (%d,%s,%s,%s,%s,%s,%s,%s,%s,%s)
									""",list_pickups)
							except Exception as ex:
								raise Exception("Exception while pushing pickup in db: "+str(ex))
							return True
						else:
							return False
			else:
				raise provider_exceptions.Pull_Exc("No pickups for rc("+route_code+"): "+str(str_response))
		except Exception as ex:
			raise Exception("Error : ("+str(ex)+")")

	def pull_dropoffs(self,route_code):
		"""
			author 		: sWaRtHi
			Date 		: February 10, 2015
			Description : Pull routewise pickups from API
		"""
		# fetching configurations from config file
		main_url  = self.get_config("main_url")
		user_name = self.get_config("user_name")
		password  = self.get_config("password")
		# dropoff_api = self.get_config("dropoff_api")

		# getting param for pickup api from route code
		# route_code = route_id+'~'+from_city_id+'~'+to_city_id+'~'+journey_date
		pickup_param = route_code.split("~")
		# Calling Web API method
		(response_code,raw_response)=self.__get_client__().\
					GetDroppingPlaces(\
						strUserName=user_name,\
						strPassword=password,\
						intRouteId=pickup_param[0],\
						intPlaceId=pickup_param[2]\
					)
		# if response success than return else rais exception
		# print response_code
		if response_code==200:
			return raw_response
		else:
			raise Exception("response code="+str(response_code))

	def process_dropoffs(self,process_id,route_code,str_response):
		"""
			author 		: sWaRtHi
			Date 		: February 10, 2015
			Description : Push Route's dropoffs to pulldb
		"""
		# check pickup availability
		try:
			response = xmltodict.parse(str_response)
			if response["NewDataSet"] is not None:
				if "BoardingPlaceID" in response["NewDataSet"]["Table"]:
					prov_pickup = response["NewDataSet"]["Table"]
					list_pickups = []
					
					pickup_id 			= prov_pickup["BoardingPlaceID"]
					pickup_name 		= prov_pickup["BoardingPlace"]
					pickup_report_time 	= prov_pickup["ReportingTime"]
					pickup_depart_time 	= prov_pickup["DepartureTime"]#.replace(prov_pickup["BoardingPointName"],'').strip()
					pickup_address 		= prov_pickup["Address"]
					pickup_landmark		= prov_pickup["Landmark"]
					pickup_contact 		= prov_pickup["PhoneNumber"]
					pickup_order 		= prov_pickup["SortOrder"]
					
					pickup=(
						process_id
						, route_code
						, pickup_id
						, pickup_name
						, pickup_report_time
						, pickup_depart_time
						, pickup_address
						, pickup_landmark
						, pickup_contact
						, pickup_order
					)
					list_pickups.append(pickup)

					pulldb_config = self.__get_pulldb_config__()
					pulldb = db.DB( *pulldb_config )

					if len(list_pickups) > 0:
						try:
							pulldb.execute_dml_bulk("""
								insert into BoardingPlaces ( 
									process_id
									, route_code
									, BoardingPlaceID
									, BoardingPlace
									, ReportingTime
									, DepartureTime
									, Address
									, Landmark
									, PhoneNumber
									, SortOrder
								) values (%d,%s,%s,%s,%s,%s,%s,%s,%s,%s)
								""",list_pickups)
						except Exception as ex:
							raise Exception("Exception while pushing pickup in db: "+str(ex))
						return True
					else:
						return False
				else:
					# Prepare list to pushed in pulldb
					prov_pickups = response["NewDataSet"]["Table"]
					list_pickups = []
					# list_cities=[ (process_id, int(list_prov_routes["DestinationStationID"]), int(prov_city["DestinationStationID"]), prov_city["DestinationStationName"]) for prov_city in list_prov_cities ]
					for prov_pickup in prov_pickups:
						# gettng field from response to push into list
						pickup_id 			= prov_pickup["BoardingPlaceID"]
						pickup_name 		= prov_pickup["BoardingPlace"]
						pickup_report_time 	= prov_pickup["ReportingTime"]
						pickup_depart_time 	= prov_pickup["DepartureTime"]#.replace(prov_pickup["BoardingPointName"],'').strip()
						pickup_address 		= prov_pickup["Address"]
						pickup_landmark		= prov_pickup["Landmark"]
						pickup_contact 		= prov_pickup["PhoneNumber"]
						pickup_order 		= prov_pickup["SortOrder"]

						# adding items in list
						pickup=(
							process_id
							, route_code
							, pickup_id
							, pickup_name
							, pickup_report_time
							, pickup_depart_time
							, pickup_address
							, pickup_landmark
							, pickup_contact
							, pickup_order
						)
						list_pickups.append(pickup)

						# return list_pickups
						pulldb_config = self.__get_pulldb_config__()
						pulldb = db.DB( *pulldb_config )

						if len(list_pickups) > 0:
							try:
								pulldb.execute_dml_bulk("""
									insert into BoardingPlaces ( 
										process_id
										, route_code
										, BoardingPlaceID
										, BoardingPlace
										, ReportingTime
										, DepartureTime
										, Address
										, Landmark
										, PhoneNumber
										, SortOrder
									) values (%d,%s,%s,%s,%s,%s,%s,%s,%s,%s)
									""",list_pickups)
							except Exception as ex:
								raise Exception("Exception while pushing pickup in db: "+str(ex))
							return True
						else:
							return False
			else:
				raise provider_exceptions.Pull_Exc("No pickups for rc("+route_code+"): "+str(str_response))
		except Exception as ex:
			raise Exception("Error : ("+str(ex)+")")

	def process_data(self,process_id):
		process_init = self.get_config("process_init")
		pulldb_config = self.__get_pulldb_config__()
		pulldb=db.DB( *pulldb_config )
		return pulldb.execute_sp(process_init,(process_id,),commit=True)

##--------------------------Class ValueLabs_API Ends------------------------

def get_from_cities(process_id):
	global DEFAULT_SECTION
	api=PolymerMT_API(DEFAULT_SECTION)
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
	api=PolymerMT_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)

	# Get from city list from response to process to city for each from city_pairs
	if from_city_response["NewDataSet"]["Table"] is None:
		raise Exception("No result from get from cities")

	try:
		for city in from_city_response["NewDataSet"]["Table"]:#from_city_list:
			# Pulling To Cities from API
			try:
				response=api.pull_to_cities(city["SourceId"])
			except Exception, ex:
				raise provider_exceptions.Pull_Exc(str(ex))
			
			# Push pulled to citeis into pulldb
			try:
				api.process_to_cities(process_id,city["SourceId"],city["Source"],response)
			except Exception, ex:
				raise provider_exceptions.Process_Exc(str(ex))
		return True
	except Exception, ex:
		raise Exception("Unable to get to cities : "+str(ex))

def get_city_pairs(process_id):
	global DEFAULT_SECTION
	api=PolymerMT_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)
	pulldb_config = api.__get_pulldb_config__()
	pulldb = db.DB( *pulldb_config )
	return pulldb.execute_query("""
		select distinct cp.SourceId as from_city_id
			, cp.Source as from_city_name
			, cp.DestinationId as to_city_id
			, cp.Destination as to_city_name
		from Destinations cp with(nolock)
		where cp.process_id = %d
		""" %process_id)

def get_routes(process_id,from_city_id,from_city_name,to_city_id,to_city_name,journey_date):
	global DEFAULT_SECTION
	api=PolymerMT_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)

	# PUlling routes
	try:
		response=api.pull_routes(from_city_id,to_city_id,journey_date)
	except Exception, ex:
		raise provider_exceptions.Pull_Exc(str(ex))
	# Process routes
	print "got response"
	try:
		return api.process_routes(process_id,from_city_name,to_city_name,journey_date,response)
	except Exception, ex:
		raise provider_exceptions.Process_Exc(str(ex))

def get_route_codes(process_id):
	global DEFAULT_SECTION
	api=PolymerMT_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)
	pulldb_config = api.__get_pulldb_config__()
	pulldb = db.DB( *pulldb_config )
	return pulldb.execute_query("""
		select distinct r.route_code as route_code
		from Routes r with(nolock)
		where r.process_id = %d
		""" %process_id)

def get_pickups(process_id,route_code):
	global DEFAULT_SECTION
	api=PolymerMT_API(DEFAULT_SECTION)
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
	api=PolymerMT_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)

	# Pulling dropoffs
	try:
		response=api.pull_dropoffs(route_code)
	except Exception, ex:
		raise provider_exceptions.Pull_Exc(str(ex))
	return response
	# Processing pickups
	try:
		return api.process_dropoffs(process_id,route_code,response)
	except Exception, ex:
		raise provider_exceptions.Process_Exc(str(ex))

def process_data(process_id):
	global DEFAULT_SECTION
	api=PolymerMT_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)

	try:
		return api.process_data(process_id)
	except Exception as e:
		raise provider_exceptions.Process_Exc(str(e))	

def get_response(str_api_name,*args,**kwrds):
	global DEFAULT_SECTION
	api=PolymerMT_API(DEFAULT_SECTION)
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
	import json
	api = PolymerMT_API(DEFAULT_SECTION)
	# print api.pull_from_cities()
	# print xmltodict.parse(api.pull_from_cities())
	# print json.dumps(xmltodict.parse(api.pull_from_cities()),indent=4)
	# print json.dumps(api.process_from_cities(0,api.pull_from_cities()),indent=4)
	# print json.dumps(xmltodict.parse(api.pull_to_cities("167")),indent=4)
	# print json.dumps(api.process_to_cities(0,"56","Bangalore",api.pull_to_cities("56")),indent=4)
	# print json.dumps(api.process_to_cities(0,"167","VIRUTHACHALAM",api.pull_to_cities("167")),indent=4)
	# print json.dumps(api.process_to_cities(0,"1","asd",api.pull_to_cities("1")),indent=4)
	# print api.pull_routes("12","11","2015-04-05")
	print json.dumps(xmltodict.parse(api.pull_routes("11","12","2015-05-13")),indent=4)
	# print json.dumps(api.process_routes(0,"Coimbatore","Bangalore","2015-04-05",api.pull_routes("12","11","2015-04-05")),indent=4)
	# print json.dumps(api.process_routes(0,"Bangalore","Trivandram","2015-02-15",api.pull_routes("56","172","2015-02-15")),indent=4)
	# print json.dumps(api.process_routes(0,"Bangalore","Chennai","2015-02-15",api.pull_routes("56","44","2015-02-15")),indent=4)
	# print json.dumps(xmltodict.parse(api.pull_pickups("167~11~12~2015-05-03")),indent=4)
	# print json.dumps(api.process_pickups(0,"136~56~172~2015-02-15",api.pull_pickups("136~56~172~2015-02-15")),indent=4)
	# print json.dumps(api.process_pickups(0,"99~56~44~2015-02-15",api.pull_pickups("99~56~44~2015-02-15")),indent=4)
	
	# print api.pull_dropoffs("136~56~172~2015-02-15")
	# print json.dumps(api.pull_dropoffs("1~BNG-KNR001~1~2~1/23/2015"),indent=4)
	# print json.dumps(xmltodict.parse(api.pull_dropoffs("2~BNG-KNR002~1~3~2/8/2015")),indent=4)
	# print json.dumps(api.process_dropoffs(0,"1~BNG-KNR001~1~2~1/21/2015",api.pull_dropoffs("1~BNG-KNR001~1~2~1/21/2015")),indent=4)
	
	# print json.dumps(get_from_cities(1),indent=4)
	# print json.dumps(get_to_cities(1,"179"),indent=4)
	# print json.dumps(get_to_cities(1,get_from_cities(1)),indent=4)
	# print json.dumps(get_city_pairs(1),indent=4)
	# print json.dumps(get_routes(1,"45","Trichy","24","Villupuram","2015-02-26"),indent=4)
	# print json.dumps(get_route_codes(1),indent=4)
	# print json.dumps(get_pickups(1,"138~177~30~2015-02-28"),indent=4)
	# print json.dumps(get_dropoffs(1,"137~44~56~2015-02-20"),indent=4)

	# select distinct * from city_pairs WITH (NOLOCK) where process_id=%d
	# select distinct route_code as route_code from OnwardServices os with(nolock) where os.process_id = 0
	pass