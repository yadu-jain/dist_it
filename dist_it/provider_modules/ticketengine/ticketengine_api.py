#Author: sWaRtHi
#Date: 2014-10-10
#Description: Provider function modules

import httplib2
from helpers import provider_exceptions,db
import urllib
import socks
import json
from datetime import datetime
import os

CONFIG_FILE="ticketengine_config.ini"
DEFAULT_SECTION="prod"

class TicketEngine_API(object):
	"""docstring for TicketEngine_API"""
	def __init__(self, section):
		super(TicketEngine_API, self).__init__()
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
			raise ex

	def pull_city_pairs(self):
		use_proxy = int(self.get_config("use_proxy"))
		proxy_ip = self.get_config("proxy_ip")
		porxy_port = int(self.get_config("porxy_port"))
		main_url = self.get_config("main_url")
		city_pair_api = self.get_config("city_pair_api")
		api_key = self.get_config("api_key")
		
		if (use_proxy==1):
			h = httplib2.Http(proxy_info = httplib2.ProxyInfo(socks.PROXY_TYPE_HTTP, proxy_ip,proxy_port))
		else:
			h = httplib2.Http()

		path = main_url \
				+ city_pair_api \
				+ "?&api_key="+api_key

		(resp_header,content)=h.request(path,"GET")
		if resp_header["status"]=="200":
			return content
		else:
			raise Exception("Server Error:"+str(resp_header))

	def process_city_pairs(self,process_id,str_response):
		try:
			response = json.loads(str_response)
		except Exception as ex:
			raise Exception("Invalid JSON response!")

		try:
			city_pairs = response["citiespair"]
			list_city_pair	= [(process_id,city_pair["citypair"]["SourceId"],city_pair["citypair"]["SourceName"],city_pair["citypair"]["DestId"],city_pair["citypair"]["DestName"]) for city_pair in city_pairs]
			
			pulldb_config 	= self.__get_pulldb_config__()
			pulldb=db.DB( *pulldb_config )
			pulldb.execute_dml_bulk("""
					insert into city_pairs (
						process_id,
						source_id,
                		source_name,
		                dest_id,
		                dest_name
					) values (%d,%d,%s,%d,%s)
				""",list_city_pair)
			return True
		except Exception as ex:
			raise ex

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
				+"&from_id="+str(from_city_id) \
				+"&to_id="+str(to_city_id) \
				+"&date="+journey_date
		#print path

		(resp_header,content)=h.request(path,"GET")
		if resp_header["status"]=="200":
			return content
		else:
			# @todo log response
			raise Exception("Server Error:"+str(resp_header))

	def process_routes(self,process_id,str_response):
		try:
			response = json.loads(str_response)
		except Exception, ex:
			raise Exception("Invalid JSON response!")

		try:
			if "service_details" in response.keys():
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

						route_code = str(travel_id)+'~'+service_number+'~'+str(from_id)+'~'+str(to_id)+'~'+journey_date

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
								, boarding["boarding_point"].get("van_pickup","no")
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
			else:
				return False

		except Exception, ex:
			raise ex

	def process_data(self,process_id):
		pulldb_config = self.__get_pulldb_config__()
		pulldb=db.DB( *pulldb_config )
		return pulldb.execute_sp("TE_Process_Data_NoChart",(process_id,),commit=True)

	# def pull_companies(self,process_id):
	# 	pulldb_config 	= self.__get_pulldb_config__()
	# 	pulldb=db.DB( *pulldb_config )
	# 	return pulldb.execute_query("""
	# 		select distinct s.travel_id, s.travel_name from services s with(nolock) where s.process_id=%d
	# 		""" %process_id)

	# def process_companies(self,process_id,str_response):
	# 	companies = str_response
	# 	try:
	# 		list_company = [(process_id,company["travel_id"],company["travel_name"]) for company in companies]
	# 		pulldb_config 	= self.__get_pulldb_config__()
	# 		pulldb=db.DB( *pulldb_config )
	# 		pulldb.execute_dml_bulk("""
	# 				insert into companies (
	# 					process_id,
	# 					travel_id,
	# 					travel_name
	# 				) values (%d,%d,%s)
	# 			""",list_company)
	# 		return True
	# 	except Exception, e:
	# 		raise e

	# def get_all_city_list(self,process_id):
	# 	pulldb_config 	= self.__get_pulldb_config__()
	# 	pulldb=db.DB( *pulldb_config )
	# 	return pulldb.execute_query("""
	# 		select distinct city_id from cities ct with(nolock) where ct.process_id=%d
	# 		""" %process_id)

##--------------------------Class TicketEngine_API Ends------------------------

def get_cities(process_id):
	global DEFAULT_SECTION
	api=TicketEngine_API(DEFAULT_SECTION)
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

def get_city_pairs(process_id):
	global DEFAULT_SECTION
	api=TicketEngine_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)

	try:
		response=api.pull_city_pairs()
	except Exception as e:
		raise provider_exceptions.Pull_Exc(str(e))
	##Processing Data
	try:
		return api.process_city_pairs(process_id,response)
	except Exception as e:
		raise provider_exceptions.Process_Exc(str(e))

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

def get_city_pairs_to_pull(process_id):
	global DEFAULT_SECTION
	api=TicketEngine_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)

	##Getting data from db
	try:
		pulldb_config = api.__get_pulldb_config__()
		pulldb=db.DB( *pulldb_config )
		# select distinct city_id from cities ct with(nolock) where ct.process_id=%d
		city_pairs = pulldb.execute_query("""
			select distinct source_id,dest_id from city_pairs cp with(nolock) where cp.process_id=%d
			""" %process_id)
						
		# cities=api.get_all_city_list(process_id)
		list_city_pair = []
		for citypair in city_pairs:
			city_pair = {}
			city_pair["from_city_id"]=citypair["source_id"]
			city_pair["to_city_id"]=citypair["dest_id"]
			list_city_pair.append(city_pair)
		# for from_city in cities:
		# 	for to_city in cities:
		# 		if from_city != to_city:
		# 			city_pair = {}
		# 			city_pair["from_city_id"]=from_city["city_id"]
		# 			city_pair["to_city_id"]=to_city["city_id"]
		# 			list_city_pair.append(city_pair)
		return list_city_pair
	except Exception as e:
		raise provider_exceptions.Pull_Exc(str(e))

def process_data(process_id):
	global DEFAULT_SECTION
	api=TicketEngine_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)
	try:
		return api.process_data(process_id)
	except Exception as e:
		raise provider_exceptions.Process_Exc(str(e))	

def get_response(str_api_name,*args,**kwrds):
	global DEFAULT_SECTION
	api=TicketEngine_API(DEFAULT_SECTION)
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

# def get_companies(process_id):
# 	global DEFAULT_SECTION
# 	api=TicketEngine_API(DEFAULT_SECTION)
# 	if api.loaded==False:
# 		raise provider_exceptions.Config_Load_Exc(api.loading_error)

# 	try:
# 		response = api.get_all_companies(process_id)
# 	except Exception, ex:
# 		raise provider_exceptions.Pull_Exc(str(ex))

# 	try:
# 		return api.process_companies(process_id,response)
# 	except Exception as e:
# 		raise provider_exceptions.Process_Exc(str(e))

if __name__== "__main__":
	# te = TicketEngine_API(DEFAULT_SECTION)
	# print json.dumps(json.loads(te.pull_cities()),indent=4)
	# print te.process_cities(0,te.pull_cities())
	# print json.dumps(json.loads(te.pull_city_pairs()),indent=4)
	# print te.process_city_pairs(0,te.pull_city_pairs())
	# print json.dumps(json.loads(te.pull_routes(0,"508","121","2014-12-01")),indent=4)
	# print json.dumps(te.process_routes(0,te.pull_routes(1,"1413","1414","2014-12-09")),indent=4)
	# print json.dumps(te.pull_companies(0),indent=4)
	# print te.process_companies(0,te.pull_companies(0))

	# print get_cities(1)
	# print get_routes(1,"402","286","2014-11-15")
	# print get_city_pairs(1)

	# print json.dumps(get_city_pairs_to_pull(1),indent=4)

	# print json.dumps(get_city_pairs_to_pull(0))
	# obj = x.pull_routes(0,"402","36","2014-10-15")
	# print json.dumps(json.loads(obj),indent=4)
	# obj = x.process_routes(10,x.pull_routes(0,"402","36","2014-10-16"))
	# print json.dumps(obj,indent=4)
	# print get_cities(50)
	# print json.dumps(get_city_pairs_to_pull(50),indent=4)
	# print get_routes(21,117,355,"2014-10-16")
	# print get_routes(36,402,36,"2014-10-16")
	# print json.dumps(x.push_all_companies(0),indent=4)
	# print json.dumps(get_companies(0),indent=4)
	pass