#Author: Heera
#Date: 2014-08-26
#Description: Provider function modules

CONFIG_FILE="paulo.ini"
DEFAULT_SECTION="dev"


def squar_it(ip):
	return ip*ip

from helpers import provider_exceptions,db,socks
import httplib2,json,urllib,xmltodict,re,time,os  
from datetime import datetime
from suds.client import Client

class Paulo_API:
	def __init__(self,section):
		try:
			import ConfigParser
			global CONFIG_FILE			
			self.section=section
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
		except Exception as ex:
			raise Exception("Invalid Pulldb Configuration !|"+str(ex))

	def __get_client__(self):
		main_url		= self.get_config("main_url")		

		if not hasattr(self,'client') or self.client == None:
			self.client=Client(main_url,faults=False)
			
		#fetching from api#
		return self.client.service			

	def get_config(self,key):
	    return self.config.get(self.section,key)      

	## Station list
	def pull_cities(self):
		"""
			Author: Heera
			Date: 2014-09-15
			Description: Pull cities from apia and return as it is
		"""		
		response_code,raw_response=self.__get_client__().GetCitiesXML()
		if response_code==200:
			return raw_response
		else:
			raise Exception("response code="+str(response_code))

	def pull_chart(self,route_code,journey_date):
		"""
			Author: Heera
			Date: 2014-09-15
			Description: Pull cities from apia and return as it is
		"""		
		response_code,raw_response=self.__get_client__().GetSeatingTemplateXML(ScheduleID=route_code)
		if response_code==200:
			return raw_response
		else:
			raise Exception("response code="+str(response_code))
		
	def process_cities(self,process_id,str_response):
		"""
			Author: Heera
			Date: 2014-08-27
			Description: Not Implemented
		"""
		pass
		try:			
			response = xmltodict.parse(str_response)			
			
			if response["CityXml"]["ResponseCode"]=='0':

				##Prepare list to pushed in pulldb
				list_prov_cities=response["CityXml"]["Cities"]["City"]
				
				list_cities=[ (prov_city["CityCode"], prov_city["Name"],re.search('\w+(\w+)', prov_city["Name"]).group(0), process_id) for prov_city in list_prov_cities ]				
				
				##Insert into db
				pulldb_config 	= self.__get_pulldb_config__()
				pulldb=db.DB( *pulldb_config )
				#print pulldb_config
				pulldb.execute_dml_bulk("""
					INSERT INTO ALL_CITIES 
									(
										city_code
										,city_name
										,just_name										
										,process_id
									)
						VALUES	(%s,%s,%s,%d)
					""",list_cities)
				return True			
			else:
				raise Exception("Failed response("+response["CityXml"]["ResponseMessage"]+")")	
		except Exception as ex:
			raise Exception("Invalid XML response("+str(ex)+")")

	## To Cities	
	def pull_city_pairs(self):
		"""
			Author: Heera
			Date: 2014-09-15
			Description: Pull city pairs from api and return as it is
		"""		
		#use_proxy		= self.get_config("use_proxy")
		#proxy_ip		= self.get_config("proxy_ip")
		#proxy_port		= int(self.get_config("proxy_port"))
		response_code,raw_response=self.__get_client__().GetAllSourceDestinationPairsXML()
		if response_code==200:
			return raw_response
		else:
			raise Exception("response code="+str(response_code))
		


	def pull_to_cities(self,process_id, from_city_id):
		"""
			Author: Heera
			Date: 2014-08-27
			Description: Not Implemented
		"""
		pass

	def get_city_pairs_to_pull(self,process_id):
		"""
			Author: Heera
			Date: 2014-08-27
			Description: Return city pairs pulled, to pull routes
		"""		
		pulldb_config 	= self.__get_pulldb_config__()
		pulldb=db.DB( *pulldb_config )
		return pulldb.execute_query("""
			select distinct * from city_pairs WITH (NOLOCK) where process_id=%d
			""" %process_id)

	def process_city_pairs(self,process_id,xml_response):
		"""
			Author: Heera
			Date: 2014-09-15
			Description: Process source and destination as city_pair in db, param=process_id,response
		"""
		try:			
			response 	= xmltodict.parse(xml_response)			
			
			if response["SourceDestinationXml"]["ResponseCode"]=='0':

				##Prepare list to pushed in pulldb
				list_source_dest=response["SourceDestinationXml"]["SourceDestinations"]["SourceDestination"]
				
				list_city_pairs=[]
				for source_dest in list_source_dest:
					from_city_id 	= source_dest["SourceCityCode"]
					from_city_name 	= source_dest["SourceCityName"]
					to_city_id 		= source_dest["DestinationCityCode"]
					to_city_name 	= source_dest["DestinationName"]
					list_city_pairs.append((from_city_id,from_city_name,to_city_id,to_city_name,process_id))
				
				##Insert into db
				pulldb_config 	= self.__get_pulldb_config__()
				pulldb=db.DB( *pulldb_config )
				#print pulldb_config
				pulldb.execute_dml_bulk("""
					INSERT INTO CITY_PAIRS 
									(
										from_city_id
										,from_city_name
										,to_city_id
										,to_city_name
										,process_id
									)
						VALUES	(%s,%s,%s,%s,%d)
					""",list_city_pairs)
				return True			
		except Exception as ex:
			raise Exception("Invalid XML response("+str(ex)+")")
		

	##Routes Data
	def pull_routes(self,process_id,from_city_id,to_city_id,journey_date,retry=True):
		"""
			Author: Heera
			Date: 2014-08-27
			Description: Pull routes from api and return as string
			Params:
			process_id= int
			from_city_id= int
			to_city_id= int
			journey_date= string (dd-mm-yyyy)
		"""
		response_code,raw_response=self.__get_client__().GetSchedulesXML(JourneyDate=journey_date,Origin=from_city_id,Destination=to_city_id)
		#print self.__get_client__().GetFareXMLDI(ScheduleID="2615929BLR1GOA",SeatNos="1")
		if response_code==200:
			return raw_response
		else:
			raise Exception("response code="+str(response_code))


	def process_routes(self,process_id,from_city_id,to_city_id,dt_journey_date,str_response):
		"""
			Author: Heera
			Date: 2014-08-27
			Description: Process stations data from api, param=str_response
		"""		
		try:
			response = xmltodict.parse(str_response)
			#print json.dumps(response,indent=4)
		except Exception as ex:
			raise Exception("Invalid XML response")
			
		if response!=None and response["ScheduleXml"]["ResponseCode"]=='0':
			try:
				##Prepare list to pushed in pulldb				
				list_schedules=response["ScheduleXml"]["Schedules"]["Schedule"]
				if type(list_schedules)!=list:
					list_schedules=[list_schedules]
				
				list_routes=[]
				journey_date=datetime.strftime(dt_journey_date,"%Y-%m-%d")
				for schedule in list_schedules:
					schedule_id 	= schedule["ScheduleID"]
					bus_type 		= schedule["BusType"]
					departure_time 	= schedule["DepartureTime"]
					arrival_time 	= schedule["ArrivalTime"]
					per_seat_fare 	= schedule["PerSeatFare"]
					travels_name 	= schedule["TravelsName"]
					bus_code 		= schedule["BusCode"]
					list_routes.append(
						(	schedule_id,
							bus_type,
							departure_time,
							arrival_time,
							per_seat_fare,
							travels_name,
							bus_code,
							process_id,
							from_city_id,
							to_city_id,
							journey_date
						)
					)			
					##Insert into db
					#print list_routes
				pulldb_config 	= self.__get_pulldb_config__()
				pulldb=db.DB( *pulldb_config )				
				pulldb.execute_dml_bulk("""
					INSERT INTO 
						SEARCH_RESULTS 
								(
									schedule_id
									,bus_type
									,departure_time
									,arrival_time
									,per_seat_fare
									,travels_name
									,bus_code
									,process_id
									,from_city_id
									,to_city_id
									,journey_date
								)
						VALUES	(%s,%s,%s,%s,%s,%s,%s,%d,%s,%s,%s)
					""",list_routes)
				return True		
			except Exception as e:	
				raise Exception("Invalid response("+str_response+")")			
		else:
			raise Exception("Failed response("+str(response["ScheduleXml"]["ResponseMessage"])+")")		

	def pull_pickups_and_dropoffs(self,route_code):
		"""
			Author: Heera
			Date: 2014-09-15
			Description: Pull cities from apia and return as it is
		"""		
		response_code,raw_response=self.__get_client__().GetPickupDropPointDetailsXML(ScheduleID=route_code)
		if response_code==200:
			return raw_response
		else:
			raise Exception("response code="+str(response_code))

	def process_data(self,process_id):
		pulldb_config = self.__get_pulldb_config__()
		pulldb=db.DB( *pulldb_config )
		return pulldb.execute_sp("PROCESS_DATA",(process_id,),commit=True)

##--------------------------Class Paulo_API Ends------------------------
		
def get_response(str_api_name,*args,**kwrds):
	global DEFAULT_SECTION
	
	api=Paulo_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)

	##Getting data from db
	if hasattr(api,str_api_name):
		try:
			api_method=getattr(api,str_api_name)
			response=api_method(*args,**kwrds)		
			return response		
		except Exception as e:
			raise provider_exceptions.Pull_Exc(str(e))			
	else:
		raise Exception("No Such API Method !")

	

def get_city_pairs(process_id):
	global DEFAULT_SECTION
	
	api=Paulo_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)

	##Getting data from db
	response=None
	try:
		response=api.pull_city_pairs()		
	except Exception as e:
		raise provider_exceptions.Pull_Exc(str(e))	

	try:
		if response!=None:
			return api.process_city_pairs(process_id,response)
	except Exception as e:
		raise provider_exceptions.Process_Exc(str(e))

def get_cities(process_id):
	global DEFAULT_SECTION
	
	api=Paulo_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)

	##Getting data from db
	response=None
	try:
		response=api.pull_cities()		
	except Exception as e:
		raise provider_exceptions.Pull_Exc(str(e))	
	#print response
	try:
		if response!=None:
			return api.process_cities(process_id,response)
	except Exception as e:
		raise provider_exceptions.Process_Exc(str(e))		

def get_city_pairs_to_pull(process_id):
	global DEFAULT_SECTION
	
	api=Paulo_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)

	##Getting data from db
	try:
		response=api.get_city_pairs_to_pull(process_id)		
		return response
	except Exception as e:
		raise provider_exceptions.Pull_Exc(str(e))	

def get_routes(process_id,from_city_id,to_city_id,journey_date):
	##TEMP
	time.sleep(0.01)	
	##
	global DEFAULT_SECTION
	api=Paulo_API(DEFAULT_SECTION)

	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)	

	jd = datetime.strptime(journey_date,"%Y-%m-%d")
	str_journey_date=datetime.strftime(jd,"%d-%m-%Y")
	print str_journey_date
	##Pulling Data
	try:
		response=api.pull_routes(process_id, from_city_id,to_city_id,str_journey_date)		
		print json.dumps(response,indent=4)
	except Exception as e:
		raise provider_exceptions.Pull_Exc(str(e))
		
	##Processing Data		
	try:
		return api.process_routes(process_id,from_city_id,to_city_id,jd,response)
	except Exception as e:		
		raise provider_exceptions.Process_Exc(str(e))		

def process_data(process_id):
	global DEFAULT_SECTION
	print "Processing data=%d" % (process_id,)
	api=Paulo_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)

	##calling stored procedure to process data
	try:		
		return api.process_data(process_id)
	except Exception as e:
		raise provider_exceptions.Process_Exc(str(e))	

if __name__== "__main__":	
	#print get_city_pairs(1)
	#print json.dumps(get_city_pairs_to_pull(1),indent=4)
	#AGR	GOA	2014-09-17
	#get_routes(5,"MUM","KUD","2014-09-20")
	get_cities(44548425)
	
	#print json.dumps(temp,indent=4)
	#get_to_cities(2,"Bangalore",42)
	#dt=datetime.strptime("2014-08-30","%Y-%m-%d")
	#print get_routes(16,43,170,"2014-09-06")	
	
	#get_to_cities(14,"Bangalore",42)
	#MUM	KUD
	#t=get_response("pull_pickups_and_dropoffs","2584759GOA1PNQ")
	#t=xmltodict.parse(t)
	#print json.dumps(t,indent=4)
