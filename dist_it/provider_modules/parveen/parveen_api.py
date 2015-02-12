#Author: Heera
#Date: 2014-08-26
#Description: Provider function modules

#CONFIG_FILE="D:/repo/dist_it_env/env/dist_it/dist_it/provider_modules/parveen/parveen_config.ini"
CONFIG_FILE="parveen_config.ini"
DEFAULT_SECTION="dev"


def squar_it(ip):
	return ip*ip

import httplib2

from helpers import provider_exceptions,db,socks
import json
import urllib
from datetime import datetime
import os
class Parveen_API:
	def __init__(self,section):
		try:
			import ConfigParser
			global CONFIG_FILE			
			self.section=section
			self.config=ConfigParser.ConfigParser()
			

			#print os.path.dirname(os.path.abspath(__file__))
			#print os.getcwd()
			#print "sections",self.config.sections()			
			path =  os.path.join(os.path.dirname(os.path.abspath(__file__)),CONFIG_FILE)
			self.config.read(path)
			if not (section in self.config.sections()):
				raise Exception("Section "+section+" not Found !")
			self.loaded=True			
		except Exception as ex:
			print ex
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


	def get_config(self,key):
	    return self.config.get(self.section,key)      

	## Station list
	def pull_stations(self):
		"""
			Author: Heera
			Date: 2014-08-27
			Description: Pull stations from api and return as string
		"""
		use_proxy		= self.get_config("use_proxy")
		proxy_ip		= self.get_config("proxy_ip")
		proxy_port		= int(self.get_config("proxy_port"))
		main_url		= self.get_config("main_url")
		email			= self.get_config("email")
		password		= self.get_config("password")
		stations_path	= self.get_config("stations_path")

		h=None
		if int(use_proxy)==1:
			h = httplib2.Http(proxy_info = httplib2.ProxyInfo(socks.PROXY_TYPE_HTTP, proxy_ip, proxy_port))
		else:
			h = httplib2.Http()
		path = main_url \
				+ stations_path \
				+ '&email=' + email \
				+ '&password=' + password

		(resp_headers, content) = h.request(path, "POST")
		if resp_headers["status"]=="200":
			return content

		
	def process_stations(self,process_id,str_response):
		"""
			Author: Heera
			Date: 2014-08-27
			Description: Process stations data from api, param=str_response
		"""
		try:
			response 	= json.loads(str_response)
		except Exception as ex:
			raise Exception("Invalid JSON response")

		if response["success"]==True and response["data"]["statusCode"]=="200":			
			stations 	= response["data"]["stations"]

			##Prepare list to insert into pull db
			list_stations 	= [(station["stationName"], station["stationId"],process_id) for station in stations]

			##Insert into db			
			pulldb_config 	= self.__get_pulldb_config__()
			pulldb=db.DB( *pulldb_config )
			
			pulldb.execute_dml_bulk("""
				INSERT INTO STATIONS 
								(
									stationName
									,stationId
									,process_id
								)
					VALUES	(%s,%d,%d)
				""",list_stations)
			return True
		else:
			raise Exception("Invalid response")

	## To Cities		
	def pull_to_cities(self,process_id, from_city_id):
		"""
			Author: Heera
			Date: 2014-08-27
			Description: Pull to_cities from api and return as string
		"""
		use_proxy		= self.get_config("use_proxy")
		proxy_ip		= self.get_config("proxy_ip")
		proxy_port		= int(self.get_config("proxy_port"))
		main_url		= self.get_config("main_url")
		email			= self.get_config("email")
		password		= self.get_config("password")
		to_cities_path	= self.get_config("to_cities_path")

		h=None
		if int(use_proxy)==1:
			h = httplib2.Http(proxy_info = httplib2.ProxyInfo(socks.PROXY_TYPE_HTTP, proxy_ip, proxy_port))
		else:
			h = httplib2.Http()
		
		path = main_url \
				+ str(from_city_id) + "/" \
				+ to_cities_path \
				+ '&email=' + email \
				+ '&password=' + password
		
		(resp_headers, content) = h.request(path, "POST")
		if resp_headers["status"]=="200":
			return content

	def get_city_pairs_to_pull(self,process_id):
		pulldb_config 	= self.__get_pulldb_config__()
		pulldb=db.DB( *pulldb_config )
		return pulldb.execute_query("""
			select distinct * from city_pairs WITH (NOLOCK) where process_id=%d
			""" %process_id)
	def process_to_cities(self,process_id,from_city_name,from_city_id,str_response):
		"""
			Author: Heera
			Date: 2014-08-27
			Description: Process to_cities  as city_pairs in db, param=process_id,response
		"""
		try:
			response 	= json.loads(str_response)
		except Exception as ex:
			raise Exception("Invalid JSON response")

		if response["success"]==True and response["data"]["statusCode"]=="200":
			to_cities = response["data"]["stations"]
			
			##Prepare list to pushed in pulldb
			list_city_pairs = [ (from_city_id, from_city_name, to_city["stationId"], to_city["stationName"], process_id ) for to_city in to_cities]

			##Insert into db
			pulldb_config 	= self.__get_pulldb_config__()
			pulldb=db.DB( *pulldb_config )
			
			pulldb.execute_dml_bulk("""
				INSERT INTO CITY_PAIRS 
								(
									from_city_id
									,from_city_name
									,to_city_id
									,to_city_name
									,process_id
								)
					VALUES	(%d,%s,%d,%s,%d)
				""",list_city_pairs)
			return True
		elif response["success"]==True and response["data"]["statusCode"]=="800":
			return False
		else:
			raise Exception("Invalid response")

	##Routes Data
	def pull_routes(self,process_id,from_city_id,to_city_id,journey_date):
		"""
			Author: Heera
			Date: 2014-08-27
			Description: Pull routes from api and return as string
			Params:
			process_id= int
			from_city_id= int
			to_city_id= int
			journey_date= string (dd/mm/yyyy)
		"""
		use_proxy		= self.get_config("use_proxy")
		proxy_ip		= self.get_config("proxy_ip")
		proxy_port		= int(self.get_config("proxy_port"))
		main_url		= self.get_config("main_url")
		email			= self.get_config("email")
		password		= self.get_config("password")
		routes_path		= self.get_config("routes_path")

		h=None
		if int(use_proxy)==1:
			h = httplib2.Http(proxy_info = httplib2.ProxyInfo(socks.PROXY_TYPE_HTTP, proxy_ip, proxy_port))
		else:
			h = httplib2.Http()
		
		path = main_url \
				+ routes_path \
				+ '&email=' + email \
				+ '&password=' + password			
		
		body = {"fromStation" : str(from_city_id), "toStation" : str(to_city_id), "travelDate" : journey_date}
		
		headers = {'Content-type':'application/json'}
		(resp_headers, content) = h.request(path, "POST",headers=headers, body=json.dumps(body) )
		
		if resp_headers["status"]=="200":
			return content


	def process_routes(self,process_id,from_city_id,to_city_id,dt_journey_date,str_response):
		"""
			Author: Heera
			Date: 2014-08-27
			Description: Process stations data from api, param=str_response
		"""
		try:
			response 	= json.loads(str_response)
		except Exception as ex:
			raise Exception("Invalid JSON response")
		if response["success"] == True and response["data"]["statusCode"]=="200":			

			##Prepare routes Data to insert into db
			schedules 		= response["data"]["schedules"]
			list_routes=[]
			list_boardingPoints=[]
			list_droppingPoints=[]
			journey_date=datetime.strftime(dt_journey_date,"%Y-%m-%d")
			for schedule in schedules:

				fare_ss=0
				fare_st=0
				fare_sl=0
				service_tax=0
				service_tax_amt=0
				list_service_tax=[]
				for fare_type in schedule["fares"]:					
					service_tax_amt = int(fare_type["serviceTax"])
					if fare_type["seatType"] 	== "SS":
						fare_ss = int(fare_type["fare"])
						if fare_ss>0:
							list_service_tax.append(service_tax_amt*100.0/fare_ss)
							fare_ss+=service_tax_amt

					elif fare_type["seatType"] 	== "ST":
						fare_st = int(fare_type["fare"])
						if fare_st>0:
							list_service_tax.append(service_tax_amt*100.0/fare_st)
							fare_st+=service_tax_amt

					if fare_type["seatType"] 	== "SL" or fare_type["seatType"]=="LSL" or  fare_type["seatType"]=="USL":
						fare_sl = int(fare_type["fare"])
						if fare_sl>0:
							list_service_tax.append(service_tax_amt*100.0/fare_sl)
							fare_sl+=service_tax_amt
										
				if len(list_service_tax)>0:
					service_tax=max(list_service_tax)
					service_tax=round(service_tax,2)

				route_code=str(from_city_id)+'~'+str(to_city_id)+'~'+journey_date+'~'+str(schedule["scheduleId"]) #TODO
				route=(from_city_id
					, to_city_id
					, journey_date
					, process_id
					, schedule["arrivalTime"]
					, schedule["departureTime"]
					, schedule["totalSeats"]
					, schedule["busType"]
					, schedule["routeName"]
					, schedule["operatorDisplayName"]
					, schedule["serviceNumber"]
					, schedule["scheduleId"]
					, str(fare_sl)
					, str(fare_ss)
					, str(fare_st)	
					, str(service_tax)				
					,route_code
					)

				

				for boarding in schedule["boardingPoints"]:
					list_boardingPoints.append((
						route_code
						, process_id
						, boarding["mobileNumber"]
						, boarding["contactPersonName"]
						, boarding["stationPointName"]
						, boarding["stationPointId"]
						, boarding["latitude"]
						, boarding["longitude"]
						, boarding["addressLine1"]
						, boarding["addressLine2"]
						, boarding["addressLandMark"]
						, boarding["addressPinCode"]
						, boarding["stationPointTime"]
						, boarding["day"]
						))

				for dropping in schedule["droppingPoints"]:
					list_droppingPoints.append((
						route_code
						, process_id
						, dropping["mobileNumber"]
						, dropping["contactPersonName"]
						, dropping["stationPointName"]
						, dropping["stationPointId"]
						, dropping["latitude"]
						, dropping["longitude"]
						, dropping["addressLine1"]
						, dropping["addressLine2"]
						, dropping["addressLandMark"]
						, dropping["addressPinCode"]
						, dropping["stationPointTime"]
						, dropping["day"]
						))


				list_routes.append(route)

			##Insert into db
			if len(list_routes) > 0:
				pulldb_config 	= self.__get_pulldb_config__()
				pulldb=db.DB( *pulldb_config )

				##Routes
				try:
					pulldb.execute_dml_bulk("""
						INSERT INTO SEARCH_RESULTS
										(
											from_city_id
											,to_city_id									
											,journey_date
											,process_id
											,arrivalTime
											,departureTime
											,totalSeats
											,busType
											,routeName
											,operatorDisplayName
											,serviceNumber
											,scheduleId
											,fare_sl
											,fare_ss
											,fare_st
											,service_tax
											,route_code
										)
							VALUES	(%d,%d,%s,%d,%s,%s,%d,%s,%s,%s,%s,%d,%s,%s,%s,%s,%s)
						""",list_routes)
				except Exception as ex:
					raise Exception("Exception while pushing routes in db: "+str(ex))

				##Boarding Points
				try:
					pulldb.execute_dml_bulk("""
						INSERT INTO boardingPoints
										(
											route_code
											,process_id									
											,mobileNumber
											,contactPersonName
											,stationPointName
											,stationPointId
											,latitude
											,longitude
											,addressLine1
											,addressLine2
											,addressLandMark
											,addressPinCode
											,stationPointTime
											,day
										)
							VALUES	(%s,%d,%s,%s,%s,%s,%d,%d,%s,%s,%s,%s,%s,%s)
						""",list_boardingPoints)

				except Exception as ex:
					raise Exception("Exception while pushing boardingPoints in db: "+str(ex))

				##Dropping Points
				try:
					pulldb.execute_dml_bulk("""
						INSERT INTO droppingPoints
										(
											route_code
											,process_id									
											,mobileNumber
											,contactPersonName
											,stationPointName
											,stationPointId
											,latitude
											,longitude
											,addressLine1
											,addressLine2
											,addressLandMark
											,addressPinCode
											,stationPointTime
											,day
										)
							VALUES	(%s,%d,%s,%s,%s,%s,%d,%d,%s,%s,%s,%s,%s,%s)
						""",list_droppingPoints)
				except Exception as ex:
					raise Exception("Exception while pushing droppingPoints in db: "+str(ex))
				return True
			else:
				return False
		else:
			raise Exception("Invalid response")			

	def process_data(self,process_id):
		pulldb_config = self.__get_pulldb_config__()
		pulldb=db.DB( *pulldb_config )
		return pulldb.execute_sp("PROCESS_DATA",(process_id,),commit=True)
##--------------------------Class Parveen_API Ends------------------------


def get_stations(process_id):
	global DEFAULT_SECTION
	api=Parveen_API(DEFAULT_SECTION)
	print "Pulling stations"
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)
	##Pulling Data
	try:
		response=api.pull_stations()
	except Exception as e:
		raise provider_exceptions.Pull_Exc(str(e))
	##Processing Data
	# try:
	# 	return api.process_stations(process_id,response)
	# except Exception as e:
	# 	print e
	# 	raise provider_exceptions.Process_Exc(str(e))
	# return True	
	return json.loads(response)

def get_to_cities(process_id, from_city_name ,from_city_id):
	global DEFAULT_SECTION
	print "Pulling to_cites from ",from_city_name
	api=Parveen_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)

	##Pulling Data
	try:
		response=api.pull_to_cities(process_id, from_city_id)		
	except Exception as e:
		raise provider_exceptions.Pull_Exc(str(e))

	##Processing Data	
	try:
		return api.process_to_cities(process_id,from_city_name,from_city_id,response)
	except Exception as e:		
		raise provider_exceptions.Process_Exc(str(e))		

def get_city_pairs_to_pull(process_id):
	global DEFAULT_SECTION
	print "Get city_pairs from db for process_id="+str(process_id)
	api=Parveen_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)

	##Getting data from db
	try:
		response=api.get_city_pairs_to_pull(process_id)		
		return response
	except Exception as e:
		raise provider_exceptions.Pull_Exc(str(e))	

def get_routes(process_id,from_city_id,to_city_id,journey_date):
	global DEFAULT_SECTION
	api=Parveen_API(DEFAULT_SECTION)

	#str_journey_date=datetime.strftime(journey_date,"%d/%m/%Y")
	jd = datetime.strptime(journey_date,"%Y-%m-%d")
	str_journey_date=datetime.strftime(jd,"%d/%m/%Y")


	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)	
	
	##Pulling Data
	try:
		response=api.pull_routes(process_id, from_city_id,to_city_id,str_journey_date)		
	except Exception as e:
		raise provider_exceptions.Pull_Exc(str(e))
	
	#print response
	##Processing Data	
	try:
		return api.process_routes(process_id,from_city_id,to_city_id,jd,response)
	except Exception as e:		
		raise provider_exceptions.Process_Exc(str(e))		

def process_data(process_id):
	global DEFAULT_SECTION
	print "Processing data=%d" % (process_id,)
	api=Parveen_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)

	##calling stored procedure to process data
	try:		
		return api.process_data(process_id)
	except Exception as e:
		raise provider_exceptions.Process_Exc(str(e))	

def test_me():
	a=Parveen_API("dev")
	print __file__	
	print "Done"

if __name__== "__main__":
	#print 
	#print get_stations(3)	
	#print get_city_pairs_to_pull(12)
	#get_to_cities(2,"Bangalore",42)
	#dt=datetime.strptime("2014-08-30","%Y-%m-%d")
	#print get_routes(16,43,170,"2014-09-06")
	# get_routes(20,43,45,"2014-09-05")
	# get_routes(20,93,43,"2014-09-05")
	# get_routes(20,43,59,"2014-09-05")
	# get_routes(20,43,59,"2014-09-06")
	# get_routes(20,43,45,"2014-09-06")
	# get_routes(20,43,46,"2014-09-05")
	# get_routes(20,93,43,"2014-09-06")
	# get_routes(20,43,93,"2014-09-05")
	# get_routes(20,43,93,"2014-09-06")
	# get_routes(20,58,43,"2014-09-05")
	# get_routes(20,58,43,"2014-09-06")
	# get_routes(20,59,43,"2014-09-05")
	# get_routes(20,59,43,"2014-09-06")
	# get_routes(20,43,46,"2014-09-06")
	# get_routes(20,45,43,"2014-09-05")
	# get_routes(20,45,43,"2014-09-06")
	# get_routes(20,46,43,"2014-09-05")
	# get_routes(20,46,43,"2014-09-06")

	#get_to_cities(14,"Bangalore",42)
	#a=Parveen_API("dev")
	print process_data(46249871)
	#print __file__
	pass
