#Author: sWaRtHi
#Date: 2014-10-10
#Description: Provider function modules

CONFIG_FILE="ticketengine_config.ini"
DEFAULT_SECTION="dev"

import httplib2
from helpers import provider_exceptions,db
import urllib
import socks
import json
from datetime import datetime

class TicketEngine_API(object):
	"""docstring for TicketEngine_API"""
	def __init__(self, section):
		super(TicketEngine_API, self).__init__()
		try:
			import ConfigParser
			global CONFIG_FILE
			self.section = section
			self.config=ConfigParser.ConfigParser()
			self.config.read(CONFIG_FILE)
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

	def pull_cities(self):
		"""
			author 		: sWaRtHi
			Date 		: October 10, 2014
			Description : Pull Cities from api getCities.json
		"""
		use_proxy = int(self.get_config("use_proxy"))
		proxy_ip = self.get_config("proxy_ip")
		porxy_port = int(self.get_config("porxy_port"))
		main_url = self.get_config("main_url")
		#email = self.get_config("email")
		#password = self.get_config("password")
		#stations_path = self.get_config("stations_path")
		cities_api = self.get_config("cities_api")
		api_key = self.get_config("api_key")

		if (use_proxy==1):
			h = httplib2.Http(proxy_info = httplib2.ProxyInfo(socks.PROXY_TYPE_HTTP, proxy_ip,proxy_port))
		else:
			h = httplib2.Http()

		path = main_url \
				+cities_api \
				+"?&api_key="+api_key 

		(resp_header,content)=h.request(path,"GET")
		if resp_header["status"]=="200":
			return content
		else:
			raise Exception("Server Error:"+str(resp_header))

	def process_cities(self,process_id,str_response):
		"""
			author:			sWaRtHi
			Date:			October 13, 2014
			Description:	Process citess data from api, params=str_response
		"""
		try:
			response = json.loads(str_response)
		except Exception as ex:
			raise Exception("Invalid JSON response!")

		try:
			cities = response["cities"]
			list_cities	= [(process_id,city["city"]["id"],city["city"]["name"]) for city in cities]
			
			##Insert into db			
			pulldb_config 	= self.__get_pulldb_config__()
			pulldb=db.DB( *pulldb_config )
			pulldb.execute_dml_bulk("""
					insert into cities (
						process_id,
						city_id,
						city_name
					) values (%d,%d,%s)
				""",list_cities)
			return True
		except Exception as ex:
			raise

	def pull_routes(self,process_id,from_city_id,to_city_id,journey_date):
		use_proxy = int(self.get_config("use_proxy"))
		proxy_ip = self.get_config("proxy_ip")
		porxy_port = int(self.get_config("porxy_port"))
		main_url = self.get_config("main_url")
		api_key = self.get_config("api_key")
		route_api = self.get_config("route_api")

		if (use_proxy==1):
			h = httplib2.Http(proxy_info = httplib2.ProxyInfo(socks.PROXY_TYPE_HTTP, proxy_ip,proxy_port))
		else:
			h = httplib2.Http()

		path = main_url \
				+route_api \
				+"?&api_key="+api_key \
				+"&from_id="+from_city_id \
				+"&to_id="+to_city_id \
				+"&date="+journey_date
		#print path

		(resp_header,content)=h.request(path,"GET")
		if resp_header["status"]=="200":
			return content
		else:
			raise Exception("Server Error:"+str(resp_header))

	#def process_routes(self,process_id,from_city_id,to_city_id,dt_journey_date,str_response):
	def process_routes(self,process_id,str_response):
		try:
			response = json.loads(str_response)
		except Exception, ex:
			raise Exception("Invalid JSON response!")

		try:
			routes = response["service_details"]
			list_services=[]
			list_pickups=[]
			list_dropoffs=[]
			list_cancellation=[]
			#journey_date=datetime.strftime(dt_journey_date,"%Y-%m-%d")
			for route in routes:
				if route["status"] == "Available":
					seat_fare = route["seat_fare"]
					lb_fare = route["lb_fare"]
					ub_fare = route["ub_fare"]
					
					from_id = route["from_id"]
					from_name = route["from_name"]
					to_id = route["to_id"]
					to_name = route["to_name"]
					journey_date = route["journey_date"]
					
					travel_id = route["travel_id"]
					travel_name = route["travel_name"]
					
					dep_time = route["dep_time"]
					journey_time = route["journey_time"]
					arrival_time = route["arrival_time"]

					available_seats = route["available_seats"]
					total_seats = route["total_seats"]
					
					bus_type = route["bus_type"]
					bus_model = route["bus_model"]
					service_number = route["service_number"]

					route_code = str(from_id)+'~'+str(to_id)+'~'+journey_date+'~'+service_number

					service=(process_id
						, route_code
						, service_number
						, travel_id
						, travel_name
						, from_id
						, from_name
						, to_id
						, to_name
						, journey_date
						, bus_type
						, bus_model
						, seat_fare
						, lb_fare
						, ub_fare
						, dep_time
						, journey_time
						, arrival_time
						, available_seats
						, total_seats
						)
					list_services.append(service)
					
					for boarding in route["boarding_points"]:
						list_pickups.append((process_id
							, route_code
							, boarding["boarding_point"]["bpid"]
							, boarding["boarding_point"]["pickup_point"]
							, boarding["boarding_point"]["landmark"]
							, boarding["boarding_point"]["city_id"]
							, boarding["boarding_point"]["city_name"]
							, boarding["boarding_point"]["time"]
							, boarding["boarding_point"]["van_pickup"]
							, boarding["boarding_point"]["type"]
							))
					
					for cancellation in route["cancellation_policies"]:
						list_cancellation.append((process_id
							, route_code
							, cancellation["cancellation_policy"]["charges"]
							, cancellation["cancellation_policy"]["from_time"]
							, cancellation["cancellation_policy"]["to_time"]
							))

			pulldb_config 	= self.__get_pulldb_config__()
			pulldb=db.DB( *pulldb_config )

			if len(list_services) > 0:
				try:
					pulldb.execute_dml_bulk("""
						insert into services ( 
							process_id
							, route_code
							, service_number
							, travel_id
							, travel_name
							, from_id
							, from_name
							, to_id
							, to_name
							, journey_date
							, bus_type
							, bus_model
							, seat_fare
							, lb_fare
							, ub_fare
							, dep_time
							, journey_time
							, arrival_time
							, available_seats
							, total_seats
						) values (%d,%s,%s,%d,%s,%d,%s,%d,%s,%s,%s,%s,%d,%d,%d,%s,%s,%s,%d,%d)
						""",list_services)
				except Exception as ex:
					raise Exception("Exception while pushing routes in db: "+str(ex))
				
				try:
					pulldb.execute_dml_bulk("""
						insert into boarding_points (
							process_id
							, route_code
							, bpid
							, pickup_point
							, landmark
							, city_id
							, city_name
							, time
							, van_pickup
							, type
							) values(%d,%s,%s,%s,%s,%d,%s,%s,%s,%s)
						""",list_pickups)
				except Exception as ex:
					raise Exception("Exception while pushing boardingPoints in db: "+str(ex))

				try:
					pulldb.execute_dml_bulk("""
						insert into cancellation_policies (
							process_id
							, route_code
							, charges
					 		, from_time
					 		, to_time
							) values (%d,%s,%s,%s,%s)
						""",list_cancellation)
				except Exception as ex:
					raise Exception("Exception while pushing cancellation policy in db: "+str(ex))
				return True
			else:
				return False

		except Exception, ex:
			raise ex

	def get_all_city_list(self,process_id):
		pulldb_config 	= self.__get_pulldb_config__()
		pulldb=db.DB( *pulldb_config )
		return pulldb.execute_query("""
			select distinct city_id from cities ct with(nolock) where ct.process_id=%d
			""" %process_id)

##--------------------------Class TicketEngine_API Ends------------------------


def get_cities(process_id):
	global DEFAULT_SECTION
	api=TicketEngine_API(DEFAULT_SECTION)
	print "Pulling stations"
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)
	##Pulling Data
	try:
		response=api.pull_cities()
	except Exception as e:
		raise provider_exceptions.Pull_Exc(str(e))
	##Processing Data
	try:
		return api.process_cities(process_id,response)
	except Exception as e:
		# print e
		raise provider_exceptions.Process_Exc(str(e))
	# return True	
	# return json.loads(response)

def get_city_pairs_to_pull(process_id):
	global DEFAULT_SECTION
	print "Get city_pairs from db for process_id="+str(process_id)
	api=TicketEngine_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)

	##Getting data from db
	try:
		cities=api.get_all_city_list(process_id)
		list_city_pair = []
		for from_city in cities:
			for to_city in cities:
				if from_city != to_city:
					city_pair = {}
					city_pair["from_city_id"]=from_city["city_id"]
					city_pair["to_city_id"]=to_city["city_id"]
					list_city_pair.append(city_pair)

		return list_city_pair
	except Exception as e:
		raise provider_exceptions.Pull_Exc(str(e))	

def get_routes(process_id,from_city_id,to_city_id,journey_date):
	global DEFAULT_SECTION
	api=TicketEngine_API(DEFAULT_SECTION)

	#str_journey_date=datetime.strftime(journey_date,"%d/%m/%Y")
	jd = datetime.strptime(journey_date,"%Y-%m-%d")
	str_journey_date=datetime.strftime(jd,"%Y-%m-%d")

	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)	
	
	##Pulling Data
	try:
		response=api.pull_routes(process_id, from_city_id,to_city_id,str_journey_date)		
	except Exception as e:
		raise provider_exceptions.Pull_Exc(str(e))
	
	##Processing Data	
	try:
		return api.process_routes(process_id,response)
	except Exception as e:		
		raise provider_exceptions.Process_Exc(str(e))		

if __name__== "__main__":
	# x = TicketEngine_API(DEFAULT_SECTION)
	# obj=json.loads(x.pull_cities())
	# print json.dumps(obj,indent=4)
	
	# cities = x.process_cities(0,x.pull_cities())
	# obj = json.loads(x.pull_routes(1,"402","36","2014-10-30"))
	
	# obj = x.pull_routes(0,"402","36","2014-10-15")
	# print json.dumps(json.loads(obj),indent=4)
	
	# obj = x.process_routes(10,x.pull_routes(0,"402","36","2014-10-16"))
	# print json.dumps(obj,indent=4)

	# print get_cities(2)
	
	# print json.dumps(get_city_pairs_to_pull(3),indent=4)

	# get_routes(10,"402","36","2014-10-16")
	pass