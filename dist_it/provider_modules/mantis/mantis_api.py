#author: Heera
#date: 2015-03-23
#description: Provider function modules

from helpers import provider_exceptions,db,jsonx,util_fun
import httplib2,json,urllib,xmltodict,re,time,os  
from suds.client import Client
from datetime import datetime
from collections import OrderedDict
import decimal

CONFIG_FILE="mantis_config.ini"
DEFAULT_SECTION="live"

class Mantis_API(object):
	"""
		Mantis (CRS API

		Author: Heera
		Date: 2015-03-23
		Description: Class to contain provider's different functions
	"""
	def __init__(self, section):
		super(Mantis_API, self).__init__()
		try:
			import ConfigParser
			global CONFIG_FILE
			self.section = section
			self.config=ConfigParser.ConfigParser()
			path =  os.path.join(os.path.dirname(os.path.abspath(__file__)),CONFIG_FILE)
			self.config.read(path)	
			self.__table_schemas__ = {}		
			if not (section in self.config.sections()):
				raise Exception("Section "+section+" not Found !")
			self.loaded=True
			#TODO:
			#self.generic_sp_api=self.__load_generic_sp_api__()
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
			raise Exception("Invalid PullDB Configuration !|"+str(ex))

	def __get_crsdb_config__(self):
		try:
			server 		= self.get_config("crsdb_server")
			db_name 	= self.get_config("crsdb_db")
			user 		= self.get_config("crsdb_user")
			password 	= self.get_config("crsdb_password")
			return (server,user,password,db_name)
		except Exception, ex:
			raise Exception("Invalid crsdb Configuration !|"+str(ex))

	def __load_generic_sp_api__(self):
		import json
		path =  os.path.join(os.path.dirname(os.path.abspath(__file__)),"mantis_api.json")
		api_json=None
		try:
			with open(path,"rb") as f:
				api_json=json.loads(f.read())["crs_sp_specs"]
		except Exception as e:
			raise Exception("Failed to load generic sp api ! |"+str(e))
		return api_json

	def __get_table_schema__(self,table_name):
		if not table_name in self.__table_schemas__:
			pulldb_config 	= self.__get_pulldb_config__()
			pulldb=db.DB( *pulldb_config )
			
			rows_table_schema = pulldb.execute_query("""
				SELECT 
					lower(column_name) column_name,
					is_nullable,
					data_type
				FROM   INFORMATION_SCHEMA.Columns WITH (NOLOCK)
				WHERE  table_name = '%s'
				""" % table_name )			
			dict_table_schema=OrderedDict()
			for field_item in rows_table_schema:
				dict_table_schema[field_item["column_name"]]=field_item
			self.__table_schemas__[table_name]=dict_table_schema
		return self.__table_schemas__[table_name]

	def quick_insert(self,table_name,data,extra={}):
		"""
			Match dict fields to table fields name and insert accordingly
		"""
		if len(data)==0:
			raise Exception("no rows found !")
		table_schema = self.__get_table_schema__(table_name)
		#print jsonx.dumps(table_schema,indent=4)
		fields=OrderedDict()
		types=[]

		if len(data)>0:							
			#print jsonx.dumps(data[0],indent=4)
			for key in data[0].keys():			
				if key.lower() in table_schema.keys():
					fields[key.lower()]=key
					#print data[0][key]
					#print type(data[0][key])
					if type(data[0][key])==int or type(data[0][key])==long:							
						types.append("%d")
					elif type(data[0][key])==float or type(data[0][key])==decimal.Decimal:
						types.append("%s")					
					else:
						types.append("%s")
		
		if len(fields)==0:
			raise Exception("No matching fields are found in db !")
		for key in extra:
			if key.lower() in table_schema.keys():
				fields[key.lower()]=key
				if type(extra[key])==int or type(extra[key])==long:							
					types.append("%d")
				elif type(extra[key])==float or type(extra[key])==decimal.Decimal:
					types.append("%s")
				else:
					types.append("%s")

		#create insert statement
		str_sql = """
			INSERT INTO %s(%s) VALUES(%s)

		""" % (table_name, ",".join(fields.keys()), ",".join(types))

		#create data rows to be inserted
		list_rows=[]
		for item in data:
			tuple_item=()
			for key in fields:
				if key in extra:
					tuple_item+=(extra[fields[key]],)
				else:
					tuple_item+=(item[fields[key]],)
			list_rows.append(tuple_item)
		#print str_sql
		pulldb_config 	= self.__get_pulldb_config__()
		pulldb=db.DB( *pulldb_config )		
		pulldb.execute_dml_bulk(str_sql,list_rows)	

	def call_crs_sp(self,sp_type,list_params,commit=False):	
		"""
			Get sp name correspnding to sp Type
			execute the sp passing params
			Returns data_set (list of tables) as result from sp
		"""
#		import _mysql
		import MySQLdb
		import MySQLdb.cursors	
		#print MySQLdb.__dict__		


		sp_name=None
		try:
			sp_name = self.get_config(sp_type)
		except Exception as e:
			raise Exception("SP Type not found ! | "+str(e))

		crsdb_config 	= self.__get_crsdb_config__()		
		data_set=None
		conn = MySQLdb.connect(*crsdb_config,cursorclass=MySQLdb.cursors.DictCursor,conv={MySQLdb.constants.FIELD_TYPE.BIT:bool})
		conn.autocommit(commit)
		#print conn
		cursor = conn.cursor()
		cursor.callproc(sp_name,list_params)
		result=cursor.fetchall()
		data_set=[result]		
		try:
			while cursor.nextset()==True:						
				result=cursor.fetchall()	
				data_set.append(result)		
		except Exception as e:
			pass
		#cursor.close()		
		conn.close()
		return data_set
	
	def call_crs_db_via_api(self,sp_type,list_params,commit=False):
		"""
			Get sp name correspnding to sp Type
			call crs generic api to execute the sp passing params
			Returns data_set (list of tables) as result from sp
		"""
		
		sp_name=None
		try:
			sp_name = self.get_config(sp_type)
		except Exception as e:
			raise Exception("SP Type not found ! | "+str(e))
		url=generic_sp_url+"?key="+api_key,
		sp_details=self.generic_sp_api[sp_name]
		#TODO:

	def __create_url_params__(self,sp_name,list_params):
		pass
		#TODO:
	def __call_crs_api__(self,url,method="GET",data=None):
		h=Http()    
		print api_url
		print data    
		if not data is None:
			method="POST"
		res=None
		content=None
		if method=="POST":
			res,content=h.request(url,method,urlencode(data),headers={"content-type":"application/x-www-form-urlencoded"})       
		else:
			res,content=h.request(url)       
		if res["status"]=='200':
			response = json.JSONDecoder(object_pairs_hook=OrderedDict).decode(content)         
			if response.has_key("success") and response["success"]==True:
				return response["data"]
			elif response.has_key("success") and response["success"]==False:
				raise Exception(response["msg"])
			else:
				raise Exception("GDS Invalid Response !")

	def get_config(self,key):
		return self.config.get(self.section,key)

	def process_trip_data(self,process_id):
		pulldb_config 	= self.__get_pulldb_config__()
		pulldb=db.DB( *pulldb_config )		
		pulldb.execute_sp("process_trigger_data",[process_id],commit=True)

	def process_trip_journey_status(self,process_id):
		pulldb_config 	= self.__get_pulldb_config__()
		pulldb=db.DB( *pulldb_config )		
		pulldb.execute_sp("process_trip_journey_all",[process_id],commit=True)
	
	def process_pickup_details(self,process_id):
		pulldb_config 	= self.__get_pulldb_config__()
		pulldb=db.DB( *pulldb_config )		
		pulldb.execute_sp("process_pickups_master",[process_id],commit=True)	

##--------------------------Class Mantis_API Ends------------------------

def is_trigger_enabled():
	global DEFAULT_SECTION
	api=Mantis_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)
	if int(api.get_config("trigger.enabled"))==1:
		return True
	else:
		return False
def get_trip_journey_status(process_id,trip_id,journey_date):
	"""
		Get only trip journey status
	"""
	global DEFAULT_SECTION
	api=Mantis_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)
	
	jd = datetime.strptime(journey_date,"%Y-%m-%d")
	str_journey_date=datetime.strftime(jd,"%Y-%m-%d")

	# Pulling trip details on journey date
	response=None
	try:
		response = api.call_crs_sp("trigger.pull_trip_journey_status",[str_journey_date,trip_id])									
		if len(response)==0:
			raise provider_exceptions.Process_Exc("No routes found !")
	except Exception, ex:
		raise provider_exceptions.Pull_Exc(str(ex))	
	# Process Trip details on journey date into pulldb 
	print jsonx.dumps(response,indent=4)
	try:
		#print jsonx.dumps(response[0],indent=4)
		data_to_insert=[]
		for item in response[0]:
		 	data_to_insert.append({"journey_date":item["JourneyDate"],"trip_id":item["TripID"], "is_active":item["IsActive"]})
		pass
		api.quick_insert("TRIP_JOURNEYS",data_to_insert,extra={"process_id":process_id})
	except Exception, ex:
		raise provider_exceptions.Process_Exc(str(ex))
	

def get_trip_journey(process_id,trip_id,journey_date):
	"""
		Get trip journey details on a journey date
	"""
	global DEFAULT_SECTION
	api=Mantis_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)
	
	jd = datetime.strptime(journey_date,"%Y-%m-%d")
	str_journey_date=datetime.strftime(jd,"%Y-%m-%d")

	# Pulling trip details on journey date
	response=None
	try:
		response = api.call_crs_sp("trigger.pull_trip_journey",[trip_id,str_journey_date])									
		if len(response)==0:
			raise provider_exceptions.Process_Exc("No routes found !")
	except Exception, ex:
		raise provider_exceptions.Pull_Exc(str(ex))

	# Process Trip details on journey date into pulldb 
	#print jsonx.dumps(response,indent=4)
	try:
		#print jsonx.dumps(response[0],indent=4)
		api.quick_insert("trip_journey_details",response[0],extra={"process_id":process_id})
	except Exception, ex:
		raise provider_exceptions.Process_Exc(str(ex))

	# Process company details into pulldb
	try:
		if len(response)<2:
			raise Exception("company details are not found in response!")
		companies=response[1]
		companies_rows=[]
		for company in companies:		
			#if company["IsActive"]=="\u0001":
			companies_rows.append({"company_id":company["CompanyID"],"company_name":company["CompanyName"],"city_id":company["CityID"],"address":company["Address"],"state_id":company["StateID"]})	
		api.quick_insert("companies",companies_rows,extra={"process_id":process_id})
	except Exception,ex:
		raise provider_exceptions.Process_Exc(str(ex))

def get_max_trip_journey_date(trip_id):
	global DEFAULT_SECTION
	api=Mantis_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)

	# Pulling pickup details
	response=None
	try:
		response = api.call_crs_sp("trigger.get_max_trip_journey_date",[trip_id])									
		if len(response)==0:
			raise provider_exceptions.Process_Exc("No routes found !")
	except Exception, ex:
		raise provider_exceptions.Pull_Exc(str(ex))
	return response

def process_trip_data(process_id):
	global DEFAULT_SECTION
	api=Mantis_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)

	# Process all trip data into gds 
	try:
		api.process_trip_data(process_id)
	except Exception, ex:
		raise provider_exceptions.Process_Exc(str(ex))
	
def process_trip_journey_status(process_id):
	global DEFAULT_SECTION
	api=Mantis_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)

	# Process all trip data into gds 
	try:
		api.process_trip_journey_status(process_id)
	except Exception, ex:
		raise provider_exceptions.Process_Exc(str(ex))

def get_pickup_details(process_id,pickup_id):
	global DEFAULT_SECTION
	api=Mantis_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)

	# Pulling pickup details
	response=None
	try:
		response = api.call_crs_sp("trigger.pull_pickup_details",[pickup_id])									
		if len(response)==0:
			raise provider_exceptions.Process_Exc("No routes found !")
	except Exception, ex:
		raise provider_exceptions.Pull_Exc(str(ex))

	# Process pickups into pulldb 
	#print json.dumps(response,indent=4)
	try:
		pass
		#print jsonx.dumps(response[0],indent=4)
		#TODO:use existing pickup_master table or create table for pickup details
		if len(response)<1:
			raise Exception("no result found from crs !")
		pickups=response[0]
		pickup_rows=[]		
		for pickup in pickups:								
			pickup_rows.append({"pickup_id":pickup["PickupID"],"pickup_name":pickup["PickupName"],"city_id":pickup["CityID"],"pickup_address":pickup["PickupAddress"],"landmark":pickup["Landmark"],"contact_numbers":pickup["ContactNumbers"]})	
		if len(pickup_rows)>0:
			api.quick_insert("pickups",pickup_rows,extra={"process_id":process_id})
		else:
			raise Exception("No Pickup found !")		
	except Exception, ex:
		raise provider_exceptions.Process_Exc(str(ex))	

def process_pickup_details(process_id):
	global DEFAULT_SECTION
	api=Mantis_API(DEFAULT_SECTION)
	if api.loaded==False:
		raise provider_exceptions.Config_Load_Exc(api.loading_error)

	# Process pickup
	try:
		response = api.process_pickup_details(process_id)
	except Exception, ex:
		raise provider_exceptions.Process_Exc(str(ex))

def get_response(str_api_name,*args,**kwrds):
	global DEFAULT_SECTION
	api=Mantis_API(DEFAULT_SECTION)
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
	process_id=util_fun.get_process_id(15)
	print process_id
	get_trip_journey(12345,"23866","2015-04-30")
	#get_trip_journey_status(process_id,"13021","2015-05-13")
	#process_trip_data(process_id)
	#process_trip_journey_status(process_id)
	#get_trip_journey(667,10140,"2015-04-03")
	# trip_id=10140
	# journey_date="2015-04-02"
	# jd = datetime.strptime(journey_date,"%Y-%m-%d")
	# str_journey_date=datetime.strftime(jd,"%Y-%m-%d")
	# api = Mantis_API(DEFAULT_SECTION)
	# #response = api.call_crs_sp("trigger.pull_trip_details",[trip_id])
	# #print response	
	# response = api.call_crs_sp("trigger.pull_trip_journey",[trip_id,str_journey_date])
	# str_data = jsonx.dumps(response,indent=4)
	# #print str_data
	# with open("dump.txt","wb") as f:
	# 	f.write(str_data)
	# 	f.flush()		
	# companies=response[1]
	# companies_rows=[]
	# for company in companies:
	# 	print company
	# 	#if company["IsActive"]=="\u0001":
	# 	companies_rows.append({"company_id":company["CompanyID"],"company_name":company["CompanyName"],"city_id":company["CityID"],"address":company["Address"],"state_id":company["StateID"]})
	
	# api.quick_insert("companies",companies_rows,extra={"process_id":123,"blaj":"23424"})

	#print json.dumps(response,indent=4)	

	pass