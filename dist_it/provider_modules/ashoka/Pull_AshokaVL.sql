-- createing schema's table
if exists (select * from INFORMATION_SCHEMA.TABLES ifs where ifs.TABLE_NAME = 'SourceStations')
    drop table SourceStations;
go
create table SourceStations(
	process_id int,
	SourceStationID varchar(5),
	SourceStationName varchar(100)
)
go

if exists (select * from INFORMATION_SCHEMA.TABLES ifs where ifs.TABLE_NAME = 'DestinationStations')
    drop table DestinationStations;
go
create table DestinationStations(
	process_id int,
	SourceStationID varchar(5),
	SourceStationName varchar(100),
	DestinationStationID varchar(5),
	DestinationStationName varchar(100)
)
go

if exists (select * from INFORMATION_SCHEMA.TABLES ifs where ifs.TABLE_NAME = 'OnwardServices')
    drop table OnwardServices;
go
create table OnwardServices ( 
	process_id int,
	route_code varchar(100),
	SourceStationID varchar(5),
	SourceStationName varchar(100),
	DestinationStationID varchar(5),
	DestinationStationName varchar(100),
	OnwardJourneyDate varchar(30),
	TravelPartner varchar(100),
	ServiceID varchar(5),
	ServiceNumber varchar(50),
	DepartureTime varchar(20),
	ServiceContactNo varchar(20),
	CoachType varchar(5),
	CoachTypeDescription varchar(50),
	CoachCapacity varchar(5),
	SeatAvailability varchar(5),
	TicketFare varchar(20),
	ApproxJourneyTime varchar(10)
)
go

if exists (select * from INFORMATION_SCHEMA.TABLES ifs where ifs.TABLE_NAME = 'BoardingPoints')
    drop table BoardingPoints;
go
create table BoardingPoints (
	process_id int,
	route_code varchar(100),
	ServiceID varchar(5),
	ServiceNumber varchar(50),
	BoardingPointID varchar(5),
	BoardingPointName varchar(50),
	BoardingPointAddress varchar(200),
	BoardingPointLandmark varchar(50),
	BoardingPointContactNo varchar(20),
	ArrivalTime varchar(50),
	JourneyDate varchar(30),
	SourceStationName varchar(100)
)
go

if exists (select * from INFORMATION_SCHEMA.TABLES ifs where ifs.TABLE_NAME = 'DroppingPoints')
    drop table DroppingPoints;
go
create table DroppingPoints ( 
	process_id int,
	route_code varchar(100),
	ServiceID varchar(5),
	ServiceNumber varchar(50),
	DroppingPointID varchar(5),
	DroppingPointName varchar(50),
	DroppingPointAddress varchar(200),
	DroppingPointLandmark varchar(50),
	JourneyDate varchar(30),
	DestinationStationName varchar(100)
)
go

if exists (select * from INFORMATION_SCHEMA.TABLES ifs where ifs.TABLE_NAME = 'sp_error_logs')
    drop table sp_error_logs;
go
create table sp_error_logs(
	process_id int,
	sp_name varchar(200),
	error_no int,
	error_line int,
	error_msg varchar(1000),
	log_date datetime,
	error_data varchar(1000)
)
go

if exists (select * from INFORMATION_SCHEMA.TABLES ifs where ifs.TABLE_NAME = 'sp_status_log')
    drop table sp_status_log;
go
create table sp_status_log (
	process_id int,
	spid smallint,
	sp_name varchar(500),
	proc_status varchar(30),
	log_date datetime,
	status char(1)
)
go

-- creating schema's store procedures
if object_id('ATA_Process_Cities') IS NULL
    exec('create procedure ATA_Process_Cities as select 1')
go
-- =============================================
-- Author:		sWaRtHi
-- Create date: January 22, 2015
-- Description: Process cities
-- =============================================
-- Exec SP_HELPTEXT ATA_Process_Cities
-- =============================================
alter procedure ATA_Process_Cities
	@ProcessId int,
	@IsProcess smallint = 1
as
begin
	set nocount on;

    insert into sp_status_log(
    	sp_name,
		process_id,
		spid,
		proc_status,
		log_date,
		status
	) select 'Pull_AshokaVL.dbo.ATA_Process_Cities'
		, @ProcessId
		, sp.spid
		, sp.status
		, dateadd(MI,690,current_timestamp)
		, 's'
	from master..sysprocesses sp where sp.spid = @@spid

	declare
		@ProviderId int,
		@ProviderName varchar(50) = 'Ashoka Travel Agency',
		@ProcessProviderId int

	select @ProviderId = p.provider_id from gdsdb.GDS.dbo.providers p with(nolock) where p.provider_name = @ProviderName
	select @ProcessProviderId = p.provider_id from gdsdb.GDS.dbo.processes p with(nolock) where p.process_id = @ProcessId
	if @ProviderId <> @ProcessProviderId
	begin
		select 'PROVIDER MIS-MATCH', @ProviderId, @ProcessProviderId
		return
	end

	/*
	IF @ProcessId = 0
	begin
		select @ProcessId = coalesce(max(p.process_id),0)
		from gdsdb.GDS.dbo.processes p with(nolock)
		where p.provider_id = @ProviderId
	end
	--*/

	begin try
		delete from gdsdb.GDS.dbo._CITIES where PROCESS_ID = @ProcessId
		insert into gdsdb.GDS.dbo._CITIES (
			process_id,
			provider_id,
			provider_city_id,
			city_name,
			is_processed
		) select distinct
			@ProcessId
			, @ProviderId
			, cast(ct.SourceStationID as int)
			, ct.SourceStationName
			, 0
		from SourceStations ct with(nolock)
		where 0=0
			and ct.process_id = @ProcessId
	end try
	begin catch
		insert into sp_error_logs(
			process_id
			, sp_name
			, error_no
			, error_line
			, error_msg
			, log_date
			, error_data
		) select 
			@ProcessId
			, error_procedure()
			, error_number()
			, error_line()
			, error_message()
			, dateadd(MI,690,current_timestamp)
			, '[01]ProcessId='+convert(varchar,@ProcessId)    
	end catch

	if @IsProcess = 1
	begin
		insert into sp_status_log(sp_name
			, process_id
			, spid
			, proc_status
			, log_date
			, status
		)
		select 'Pull_AshokaVL.dbo.ATA_Process_Cities'
			, @ProcessId
			, sp.spid
			, sp.status
			, dateadd(MI,690,current_timestamp)
			, 'd'
		from master..sysprocesses sp where sp.spid = @@spid
		exec gdsdb.GDS.dbo._PROCESS_CITIES @ProcessId
	end

	insert into sp_status_log(sp_name
		, process_id
		, spid
		, proc_status
		, log_date
		, status
	) select 'Pull_AshokaVL.dbo.ATA_Process_Cities'
		, @ProcessId
		, sp.spid
		, sp.status
		, dateadd(MI,690,current_timestamp)
		, 'c'
	from master..sysprocesses sp where sp.spid = @@spid
end

if object_id('ATA_Process_Citiy_Pairs') IS NULL
    exec('create procedure ATA_Process_Citiy_Pairs as select 1')
go
-- =============================================
-- Author:		sWaRtHi
-- Create date: January 22, 2015
-- Description: Process city pairs
-- =============================================
-- Exec SP_HELPTEXT ATA_Process_Citiy_Pairs
-- =============================================
alter procedure ATA_Process_Citiy_Pairs
	@ProcessId int,
	@IsProcess smallint = 1
as
begin
	set nocount on;

    insert into sp_status_log(sp_name
		, process_id
		, spid
		, proc_status
		, log_date
		, status
	) select 'Pull_AshokaVL.dbo.ATA_Process_Citiy_Pairs'
		, @ProcessId
		, sp.spid
		, sp.status
		, dateadd(MI,690,current_timestamp)
		, 's'
	from master..sysprocesses sp with(nolock) where sp.spid = @@spid

	--Get Provider Id
	declare
		@ProviderId int,
		@ProviderName varchar(50) = 'Ashoka Travel Agency',
		@ProcessProviderId int

	select @ProviderId = p.provider_id from gdsdb.GDS.dbo.providers p with(nolock) where p.provider_name = @ProviderName
	select @ProcessProviderId = p.provider_id from gdsdb.GDS.dbo.processes p with(nolock) where p.process_id = @ProcessId
	if @ProviderId <> @ProcessProviderId
	begin
		select 'PROVIDER MIS-MATCH', @ProviderId, @ProcessProviderId
		return
	end

	/*
	IF @ProcessId = 0
	begin
		select @ProcessId = coalesce(max(p.process_id),0)
		from gdsdb.GDS.dbo.processes p with(nolock)
		where p.provider_id = @ProviderId
	end
	--*/

	begin try
		delete from gdsdb.GDS.dbo._CITY_PAIRS where PROCESS_ID = @ProcessId
		insert into gdsdb.GDS.dbo._CITY_PAIRS(
			process_id,
			provider_id,
			provider_from_city_id,
			provider_to_city_id,
			is_processed,
			entry_date,
			error_msg
		) select distinct
			@ProcessId
			, @ProviderId
			, cast(cp.SourceStationID as int)
			, cast(cp.DestinationStationID as int)
			, 0
			, dateadd(MI,690,current_timestamp)
			, ''
		from DestinationStations cp with(nolock)
		where cp.process_id = @ProcessId
	end try
	begin catch
		insert into sp_error_logs(
			process_id
			, sp_name
			, error_no
			, error_line
			, error_msg
			, log_date
			, error_data
		) select 
			@ProcessId
			, error_procedure()
			, error_number()
			, error_line()
			, error_message()
			, dateadd(MI,690,current_timestamp)
			, 'ProcessId='+convert(varchar,@ProcessId)    
	end catch

	if @IsProcess = 1
	begin
		insert into sp_status_log(sp_name
			, process_id
			, spid
			, proc_status
			, log_date
			, status
		) select 'Pull_AshokaVL.dbo.ATA_Process_Citiy_Pairs'
			, @ProcessId
			, sp.spid
			, sp.status
			, dateadd(MI,690,current_timestamp)
			, 'd'
		from master..sysprocesses sp where sp.spid = @@spid
		exec gdsdb.GDS.dbo._PROCESS_CITY_PAIRS @ProcessId
	end

	insert into sp_status_log(sp_name
		, process_id
		, spid
		, proc_status
		, log_date
		, status
	) select 'Pull_AshokaVL.dbo.ATA_Process_Citiy_Pairs'
		, @ProcessId
		, sp.spid
		, sp.status
		, dateadd(MI,690,current_timestamp)
		, 'c'
	from master..sysprocesses sp where sp.spid = @@spid
end

if object_id('ATA_Process_Companies') IS NULL
    exec('create procedure ATA_Process_Companies as select 1')
go
-- =============================================
-- Author:		sWaRtHi
-- Create date: January 22, 2015
-- Description: Process companies
-- =============================================
-- Exec SP_HELPTEXT ATA_Process_Companies
-- =============================================
alter procedure ATA_Process_Companies
	@ProcessId int,
	@IsProcess smallint = 1
as
begin
	set nocount on;

    insert into sp_status_log(sp_name
		, process_id
		, spid
		, proc_status
		, log_date
		, status
	) select 'Pull_AshokaVL.dbo.ATA_Process_Companies'
		, @ProcessId
		, sp.spid
		, sp.status
		, dateadd(MI,690,current_timestamp)
		, 's'
	from master..sysprocesses sp with(nolock) where sp.spid = @@spid

	--Get Provider Id
	declare
		@ProviderId int,
		@ProviderName varchar(50) = 'Ashoka Travel Agency',
		@ProcessProviderId int

	select @ProviderId = p.provider_id from gdsdb.GDS.dbo.providers p with(nolock) where p.provider_name = @ProviderName
	select @ProcessProviderId = p.provider_id from gdsdb.GDS.dbo.processes p with(nolock) where p.process_id = @ProcessId
	if @ProviderId <> @ProcessProviderId
	begin
		select 'PROVIDER MIS-MATCH', @ProviderId, @ProcessProviderId
		return
	end

	/*
	IF @ProcessId = 0
	begin
		select @ProcessId = coalesce(max(p.process_id),0)
		from gdsdb.GDS.dbo.processes p with(nolock)
		where p.provider_id = @ProviderId
	end
	--*/
	begin try
		delete from gdsdb.GDS.dbo._COMPANIES where PROCESS_ID = @ProcessId
		insert into gdsdb.GDS.dbo._COMPANIES(
			PROCESS_ID,
			PROVIDER_ID,
			PROVIDER_COMPANY_ID,
			COMPANY_NAME,
			COMPANY_ADDRESS,
			COMPANY_PHONE_1,
			COMPANY_PHONE_2,
			IS_PROCESSED
		) select distinct
			@ProcessId
			, @ProviderId
			, 0	--os.travel_id
			, os.TravelPartner
			, ''
			, ''
			, ''
			, 0
		from OnwardServices os with(nolock)
		where os.process_id = @ProcessId
	end try
	begin catch
		insert into sp_error_logs(
			process_id
			, sp_name
			, error_no
			, error_line
			, error_msg
			, log_date
			, error_data
		) select 
			@ProcessId
			, error_procedure()
			, error_number()
			, error_line()
			, error_message()
			, dateadd(MI,690,current_timestamp)
			, 'ProcessId='+convert(varchar,@ProcessId)    
	end catch
	
	if @IsProcess = 1
	begin
		insert into sp_status_log(sp_name
			, process_id
			, spid
			, proc_status
			, log_date
			, status
		) select 'Pull_AshokaVL.dbo.ATA_Process_Companies'
			, @ProcessId
			, sp.spid
			, sp.status
			, dateadd(MI,690,current_timestamp)
			, 'd'
		from master..sysprocesses sp where sp.spid = @@spid
		exec gdsdb.GDS.dbo._PROCESS_COMPANIES @ProcessId
	end

	insert into sp_status_log(sp_name
		, process_id
		, spid
		, proc_status
		, log_date
		, status
	) select 'Pull_AshokaVL.dbo.ATA_Process_Companies'
		, @ProcessId
		, sp.spid
		, sp.status
		, dateadd(MI,690,current_timestamp)
		, 'c'
	from master..sysprocesses sp where sp.spid = @@spid
end

if object_id('ATA_Process_Routes') IS NULL
    exec('create procedure ATA_Process_Routes as select 1')
go
-- =============================================
-- Author:		sWaRtHi
-- Create date: January 22, 2015
-- Description: Process Routes
-- =============================================  
-- Exec SP_HELPTEXT ATA_Process_Routes
-- Exec ATA_Process_Routes 52610419, 0
-- =============================================  
alter procedure ATA_Process_Routes
	@ProcessId int,
	@IsProcess smallint = 1,
	@RouteCode varchar(100) = ''
as
begin
	set nocount on;

    insert into sp_status_log(sp_name
		, process_id
		, spid
		, proc_status
		, log_date
		, status
	) select 'Pull_AshokaVL.dbo.ATA_Process_Routes'
		, @ProcessId
		, sp.spid
		, sp.status
		, dateadd(MI,690,current_timestamp)
		, 's'
	from master..sysprocesses sp with(nolock) where sp.spid = @@spid

	--Get Provider Id
	declare
		@ProviderId int,
		@ProviderName varchar(50) = 'Ashoka Travel Agency',
		@ProcessProviderId int

	select @ProviderId = p.provider_id from gdsdb.GDS.dbo.providers p with(nolock) where p.provider_name = @ProviderName
	select @ProcessProviderId = p.provider_id from gdsdb.GDS.dbo.processes p with(nolock) where p.process_id = @ProcessId
	if @ProviderId <> @ProcessProviderId
	begin
		select 'PROVIDER MIS-MATCH', @ProviderId, @ProcessProviderId
		return
	end

	/*
	IF @ProcessId = 0
	begin
		select @ProcessId = coalesce(max(p.process_id),0)
		from gdsdb.GDS.dbo.processes p with(nolock)
		where p.provider_id = @ProviderId
	end
	--*/
	begin try
		declare @tblFareParam as table ( id int identity, route_fare varchar(20) )
		
		declare CurTR cursor local for
		select distinct
			os.route_code
			, os.SourceStationID
			, os.SourceStationName
			, os.DestinationStationID
			, os.DestinationStationName
			--, os.travel_id
			, os.TravelPartner
			, cast(os.DepartureTime as smalldatetime)
			, case
				when os.ApproxJourneyTime = '24:00' then cast(dateadd(MI,((datepart(HH,'23:59') * 60) + datepart(MI,'23:59')),cast(os.DepartureTime as datetime)) as smalldatetime)
				else cast(dateadd(MI,((datepart(HH,os.ApproxJourneyTime) * 60) + datepart(MI,os.ApproxJourneyTime)),cast(os.DepartureTime as datetime)) as smalldatetime)
			  end -- using kesineni ,s.arrival_time
			, os.CoachTypeDescription
			, os.ServiceNumber
			, os.ServiceID
			, os.TicketFare
			--, os.lb_fare
			--, os.ub_fare
			, cast(os.OnwardJourneyDate as datetime)
			, os.CoachCapacity
		from OnwardServices os with(nolock)
		where 0=0
			and os.process_id = @ProcessId
			and (@RouteCode = '' or os.route_code = @RouteCode)

		declare
			@TRRouteCode varchar(100),
            @TRFromId varchar(50),
            @TRFromName varchar(100),
            @TRToId varchar(50),
            @TRToName varchar(100),
            --@TRTravelId int,
            @TRTravelName varchar(100),
            @TRDepTime smalldatetime,
            @TRArrivalTime smalldatetime,
            @TRBusType varchar(20),
            @TRBusModel varchar(100),
            @TRServiceNumber varchar(50),
            @TRSeatFare varchar(20),
            --@TRLBFare numeric(9,2),
            --@TRUBFare numeric(9,2),
            @TRJouneyDate datetime,
            @TRTotalSeats int,
            @TRTripId int,

            @HasSeater bit,
            @HasSleeper bit,
            @HasAC bit,
            @HasNAC bit,
            @LowestSeaterFare numeric(5,2),
            @LowestSleeperFare numeric(5,2)

		open CurTR
		fetch next from CurTR into @TRRouteCode
			, @TRFromId, @TRFromName
			, @TRToId, @TRToName
			--, @TRTravelId
			, @TRTravelName
			, @TRDepTime, @TRArrivalTime
			, @TRBusType--, @TRBusModel
			, @TRServiceNumber, @TRTripId
			, @TRSeatFare--, @TRLBFare, @TRUBFare
			, @TRJouneyDate, @TRTotalSeats
		while @@fetch_status = 0
		begin
			begin try
				set @HasSeater = 0
				set @HasSleeper = 0
				set @HasAC = 0
				set @HasNAC = 0
				set @LowestSeaterFare = 0
				set @LowestSleeperFare = 0

				if charindex('Non A/c',@TRBusType) > 0
					set @HasNAC = 1
				else if charindex('A/c',@TRBusType) > 0
					set @HasAC = 1
				else
					set @HasNAC = 1

				--if @TRBusType = 'Non A/c 2+2 Seater'
				--begin
				--	set @HasSleeper = 1
				--	set @HasSeater = 1
				--end
				--else 
				
				delete from @tblFareParam
				insert into @tblFareParam select * from dbo.ParmsToList(@TRSeatFare,',')
				select top 1 @LowestSeaterFare = cast(fp.route_fare as numeric) from @tblFareParam fp order by id
				--set @LowestSeaterFare = @TRSeatFare
				--set @LowestSleeperFare = case when @TRLBFare < @TRUBFare then @TRLBFare else @TRUBFare end
				
				if @TRBusType like '%Sleeper%'
					if @TRBusType like '%Semi%Sleeper%'
					begin
						set @HasSeater = 1
						set @HasSleeper = 0
						set @LowestSleeperFare = 0
					end
					else
					begin
						set @HasSleeper = 1
						set @LowestSleeperFare = @LowestSeaterFare
						set @LowestSeaterFare = 0
					end
				else
				begin
					set @HasSeater = 1
					set @HasSleeper = 0
					set @LowestSleeperFare = 0
				end

				set @TRBusModel=Functions.dbo.GET_STRING_FROM_STRING(@ProviderId,@TRBusType,'BUS_MODEL')

				delete from gdsdb.GDS.dbo._ROUTES_NEW where PROCESS_ID = @ProcessId and (ROUTE_CODE='' or ROUTE_CODE=@TRRouteCode)
				insert into gdsdb.GDS.dbo._ROUTES_NEW(
					ROUTE_CODE,
					PROCESS_ID,
					PROVIDER_ID,
					FROM_CITY_ID,    
					TO_CITY_ID,
					COMPANY_ID,
					COMPANY_NAME,
					DEPARTURE_TIME,
					ARRIVAL_TIME,
					LOWEST_SEATER_FARE,
					LOWEST_SLEEPER_FARE,
					HAS_AC,
					HAS_NAC,
					HAS_SEATER,
					HAS_SLEEPER,
					BUS_LABEL_NAME,
					ROUTE_NAME,
					COMM_PCT,
					COMM_AMOUNT,
					TOTAL_SEATS,
					bus_type_name,
					route_remarks,
					JOURNEY_DATE,
					--provider_bus_type_id,
					--Provider_Chart_ID,
					Trip_ID,
					--Service_ID,
					--chart_date,
					service_tax
					--pseudo_trip_id
				) values (
					@TRRouteCode,
					@ProcessId,
					@ProviderId,
					@TRFromId,
					@TRToId,
					0,	--@TRTravelId,
					@TRTravelName,
					@TRDepTime,
					@TRArrivalTime,
					@LowestSeaterFare,
					@LowestSleeperFare,
					@HasAC,
					@HasNAC,
					@HasSeater,
					@HasSleeper,
					@TRBusModel, --bus lable name
					@TRFromName + ' - ' + @TRToName,
					0, --'comm pct',
					0, --'comm amt',
					@TRTotalSeats,
					@TRServiceNumber +'-'+ @TRBusType,-- 'bus type name',
					'', --route_remarks
					@TRJouneyDate,
					--provider_bus_type_id,
					--Provider_Chart_ID,
					--0, --Trip_ID
					@TRTripId,	--Service_ID,
					--chart_date,
					0 --service_tax
				)
			end try
			begin catch
				insert into sp_error_logs(
					process_id
					, sp_name
					, error_no
					, error_line
					, error_msg
					, log_date
					, error_data
				) select 
					@ProcessId
					, error_procedure()
					, error_number()
					, error_line()
					, error_message()
					, dateadd(MI,690,current_timestamp)
					, '[02]Bulk Route Insertt Failed: ProcessId='+convert(varchar,@ProcessId)+'RouteCode='+@TRRouteCode
				--@todo : insert routes route code wise one by one
			end catch
			fetch next from CurTR into @TRRouteCode
				, @TRFromId, @TRFromName
				, @TRToId, @TRToName
				--, @TRTravelId
				, @TRTravelName
				, @TRDepTime, @TRArrivalTime
				, @TRBusType--, @TRBusModel
				, @TRServiceNumber, @TRTripId
				, @TRSeatFare--, @TRLBFare, @TRUBFare
				, @TRJouneyDate, @TRTotalSeats
		end
		close CurTR
		deallocate CurTR
	end try
	begin catch
		insert into sp_error_logs(
			process_id
			, sp_name
			, error_no
			, error_line
			, error_msg
			, log_date
			, error_data
		) select 
			@ProcessId
			, error_procedure()
			, error_number()
			, error_line()
			, error_message()
			, dateadd(MI,690,current_timestamp)
			, '[01]ProcessId='+convert(varchar,@ProcessId)+'RouteCode='+@TRRouteCode
	end catch
	if @IsProcess = 1
	begin
		insert into sp_status_log(sp_name
			, process_id
			, spid
			, proc_status
			, log_date
			, status
		) select 'Pull_AshokaVL.dbo.ATA_Process_Routes'
			, @ProcessId
			, sp.spid
			, sp.status
			, dateadd(MI,690,current_timestamp)
			, 'd'
		from master..sysprocesses sp where sp.spid = @@spid
		exec gdsdb.GDS.dbo._PROCESS_ROUTES_NEW @ProcessId, 0
	end

	insert into sp_status_log(sp_name
		, process_id
		, spid
		, proc_status
		, log_date
		, status
	) select 'Pull_AshokaVL.dbo.ATA_Process_Routes'
		, @ProcessId
		, sp.spid
		, sp.status
		, dateadd(MI,690,current_timestamp)
		, 'c'
	from master..sysprocesses sp where sp.spid = @@spid
end

if object_id('ATA_Process_Route_Fares') IS NULL
    exec('create procedure ATA_Process_Route_Fares as select 1')
go
-- =============================================
-- Author:		sWaRtHi
-- Create date: November 20 2014
-- Description: Process Routes Fares
-- =============================================  
-- Exec SP_HELPTEXT ATA_Process_Route_Fares
-- =============================================  
alter procedure ATA_Process_Route_Fares
	@ProcessId int,
	@IsProcess smallint = 1,
	@RouteCode varchar(100) = ''
as
begin
	set nocount on;

    insert into sp_status_log(sp_name
		, process_id
		, spid
		, proc_status
		, log_date
		, status
	) select 'Pull_AshokaVL.dbo.ATA_Process_Route_Fares'
		, @ProcessId
		, sp.spid
		, sp.status
		, dateadd(MI,690,current_timestamp)
		, 's'
	from master..sysprocesses sp with(nolock) where sp.spid = @@spid

	declare
		@ProviderId int,
		@ProviderName varchar(50) = 'Ashoka Travel Agency',
		@ProcessProviderId int

	select @ProviderId = p.provider_id from gdsdb.GDS.dbo.providers p with(nolock) where p.provider_name = @ProviderName
	select @ProcessProviderId = p.provider_id from gdsdb.GDS.dbo.processes p with(nolock) where p.process_id = @ProcessId
	if @ProviderId <> @ProcessProviderId
	begin
		select 'PROVIDER MIS-MATCH', @ProviderId, @ProcessProviderId
		return
	end

	/*
	IF @ProcessId = 0
	begin
		select @ProcessId = coalesce(max(p.process_id),0)
		from gdsdb.GDS.dbo.processes p with(nolock)
		where p.provider_id = @ProviderId
	end
	--*/
	begin try
		delete from gdsdb.GDS.dbo._ROUTE_FARES where process_id = @ProcessId and (@RouteCode = '' or route_code = @RouteCode)
		insert into gdsdb.GDS.dbo._ROUTE_FARES (
			process_id,
			provider_id,
			route_code,
			journey_date,
			fare,
			fare_seater_nac,
			fare_seater_ac,
			fare_sleeper_nac,
			fare_sleeper_ac
		) select distinct
			@ProcessId
			, @ProviderId
			, rn.ROUTE_CODE
			, rn.JOURNEY_DATE
			, case when rn.LOWEST_SLEEPER_FARE > rn.LOWEST_SEATER_FARE then rn.LOWEST_SLEEPER_FARE else rn.LOWEST_SEATER_FARE end
			, case when rn.HAS_SEATER <> 0 and rn.HAS_NAC <> 0 then rn.LOWEST_SEATER_FARE else 0 end
			, case when rn.HAS_SEATER <> 0 and rn.HAS_AC <> 0 then rn.LOWEST_SEATER_FARE else 0 end
			, case when rn.HAS_SLEEPER <> 0 AND rn.HAS_NAC <> 0 then rn.LOWEST_SLEEPER_FARE else 0 end
			, case when rn.HAS_SLEEPER <> 0 and rn.HAS_AC <> 0 then rn.LOWEST_SLEEPER_FARE else 0 end
		from gdsdb.GDS.dbo._ROUTES_NEW rn with(nolock)
		where 0=0
			and rn.PROCESS_ID = @ProcessId
			and (@RouteCode = '' or rn.ROUTE_CODE = @RouteCode)
	end try
	begin catch
		insert into sp_error_logs(
			process_id
			, sp_name
			, error_no
			, error_line
			, error_msg
			, log_date
			, error_data
		) select @ProcessId
			, error_procedure()
			, error_number()
			, error_line()
			, error_message()
			, dateadd(MI,690,current_timestamp)
			, '[01]Bulk Route Fare Failed:ProcessId='+convert(varchar,@ProcessId)+'RouteCode='+@RouteCode

		delete from gdsdb.GDS.dbo._ROUTE_FARES where process_id = @ProcessId and (@RouteCode = '' or route_code = @RouteCode)		
		-- loop through routes and set this info up into the _ROUTE_FARES table
		declare
			@ROUTE_CODE varchar(128),
			@JOURNEY_DATE datetime,
			@FARE int,
			@FARE_SEATER_NAC int,
			@FARE_SEATER_AC int,
			@FARE_SLEEPER_NAC int,
			@FARE_SLEEPER_AC int,
			@HAS_AC int,
			@HAS_NAC int,
			@HAS_SEATER int,
			@HAS_SLEEPER int,
			@LOWEST_SEATER_FARE int,
			@LOWEST_SLEEPER_FARE int

		declare @TAB_ROUTE_FARES as table (
		 	PROCESS_ID int,
			PROVIDER_ID int,
			ROUTE_CODE varchar(128),
			FARE int,
			FARE_SEATER_NAC int,
			FARE_SEATER_AC int,
			FARE_SLEEPER_NAC int,
			FARE_SLEEPER_AC int,
			JOURNEY_DATE datetime
		)

		declare CUR cursor local for
		select distinct ROUTE_CODE
			, rn.HAS_AC
			, rn.HAS_NAC
			, rn.HAS_SEATER
			, rn.HAS_SLEEPER
			, rn.LOWEST_SEATER_FARE
			, rn.LOWEST_SLEEPER_FARE
			, rn.JOURNEY_DATE
		from gdsdb.GDS.dbo._ROUTES_NEW rn
		where 0=0
			and rn.PROCESS_ID = @ProcessId
			and rn.PROVIDER_ID = @ProviderId

		open CUR
		fetch next from CUR into @ROUTE_CODE
			, @HAS_AC, @HAS_NAC
			, @HAS_SEATER, @HAS_SLEEPER
			, @LOWEST_SEATER_FARE, @LOWEST_SLEEPER_FARE
			, @JOURNEY_DATE
		while @@fetch_status = 0
		begin
			begin try
				SELECT @FARE = 0, @FARE_SEATER_NAC = 0, @FARE_SEATER_AC = 0, @FARE_SLEEPER_NAC = 0, @FARE_SLEEPER_AC = 0

				IF (@LOWEST_SEATER_FARE <> 0 AND @LOWEST_SLEEPER_FARE <> 0)
					BEGIN
					IF(@LOWEST_SEATER_FARE < @LOWEST_SLEEPER_FARE)
						SET @FARE = @LOWEST_SEATER_FARE
					ELSE
						SET @FARE = @LOWEST_SLEEPER_FARE
					END
				ELSE
				BEGIN
					IF (@LOWEST_SEATER_FARE <> 0)
						SET @FARE = @LOWEST_SEATER_FARE
					ELSE
						SET @FARE = @LOWEST_SLEEPER_FARE
				END

				IF(@HAS_SEATER <> 0 AND @HAS_NAC <> 0)
					SET @FARE_SEATER_NAC = @LOWEST_SEATER_FARE

				IF(@HAS_SEATER <> 0 AND @HAS_AC <> 0)
					SET @FARE_SEATER_AC = @LOWEST_SEATER_FARE

				IF(@HAS_SLEEPER <> 0 AND @HAS_NAC <> 0)
					SET @FARE_SLEEPER_NAC = @LOWEST_SLEEPER_FARE

				IF(@HAS_SLEEPER <> 0 AND @HAS_AC <> 0)
					SET @FARE_SLEEPER_AC = @LOWEST_SLEEPER_FARE

				--INSERT INTO @TAB_ROUTE_FARES (
				insert into gdsdb.GDS.dbo._ROUTE_FARES (
					PROCESS_ID
					,PROVIDER_ID
					,ROUTE_CODE
					,JOURNEY_DATE
					,FARE
					,FARE_SEATER_NAC
					,FARE_SEATER_AC
					,FARE_SLEEPER_NAC
					,FARE_SLEEPER_AC
				) values (
					@ProcessId
					,@ProviderId
					,@ROUTE_CODE
					,@JOURNEY_DATE
					,@FARE
					,@FARE_SEATER_NAC
					,@FARE_SEATER_AC
					,@FARE_SLEEPER_NAC
					,@FARE_SLEEPER_AC
				)
			end try
			begin catch
				insert into sp_error_logs(
					process_id
					, sp_name
					, error_no
					, error_line
					, error_msg
					, log_date
					, error_data
				) select 
					@ProcessId
					, error_procedure()
					, error_number()
					, error_line()
					, error_message()
					, dateadd(MI,690,current_timestamp)
					, '[02]ProcessId='+convert(varchar,@ProcessId)+'RouteCode='+@RouteCode+
					', Fare='+CONVERT(VARCHAR,@FARE)+    
					', FareSeaterNAC='+CONVERT(VARCHAR,@FARE_SEATER_NAC)+    
					', FareSeaterAC='+CONVERT(VARCHAR,@FARE_SEATER_AC)+    
					', FareSleeperNAC='+CONVERT(VARCHAR,@FARE_SLEEPER_NAC)+    
					', FareSleeperAC='+CONVERT(VARCHAR,@FARE_SLEEPER_AC)+
					', JourneyDate='+CONVERT(VARCHAR,@JOURNEY_DATE)
			end catch
			fetch next from CUR into @ROUTE_CODE
				, @HAS_AC, @HAS_NAC
				, @HAS_SEATER, @HAS_SLEEPER
				, @LOWEST_SEATER_FARE, @LOWEST_SLEEPER_FARE
				, @JOURNEY_DATE
		end
		close CUR
		deallocate CUR
	end catch

	if @IsProcess = 1
	begin
		insert into sp_status_log(sp_name
			, process_id
			, spid
			, proc_status
			, log_date
			, status
		) select 'Pull_AshokaVL.dbo.ATA_Process_Route_Fares'
			, @ProcessId
			, sp.spid
			, sp.status
			, dateadd(MI,690,current_timestamp)
			, 'd'
		from master..sysprocesses sp where sp.spid = @@spid
		exec gdsdb.GDS.dbo._PROCESS_ROUTE_FARES @ProcessId
	end

	insert into sp_status_log(sp_name
		, process_id
		, spid
		, proc_status
		, log_date
		, status
	) select 'Pull_AshokaVL.dbo.ATA_Process_Route_Fares'
		, @ProcessId
		, sp.spid
		, sp.status
		, dateadd(MI,690,current_timestamp)
		, 'c'
	from master..sysprocesses sp where sp.spid = @@spid
end

if object_id('ATA_Process_Pickups') IS NULL
    exec('create procedure ATA_Process_Pickups as select 1')
go
-- =============================================
-- Author:		sWaRtHi
-- Create date: January 22, 2015
-- Description: Process Routes Pickups
-- =============================================
-- Exec SP_HELPTEXT ATA_Process_Pickups
-- =============================================
alter procedure ATA_Process_Pickups
	@ProcessId int,
	@IsProcess smallint = 1,
	@RouteCode varchar(100) = ''
as
begin
	set nocount on;

    insert into sp_status_log(sp_name
		, process_id
		, spid
		, proc_status
		, log_date
		, status
	) select 'Pull_AshokaVL.dbo.ATA_Process_Pickups'
		, @ProcessId
		, sp.spid
		, sp.status
		, dateadd(MI,690,current_timestamp)
		, 's'
	from master..sysprocesses sp with(nolock) where sp.spid = @@spid

	declare
		@ProviderId int,
		@ProviderName varchar(50) = 'Ashoka Travel Agency',
		@ProcessProviderId int

	select @ProviderId = p.provider_id from gdsdb.GDS.dbo.providers p with(nolock) where p.provider_name = @ProviderName
	select @ProcessProviderId = p.provider_id from gdsdb.GDS.dbo.processes p with(nolock) where p.process_id = @ProcessId
	if @ProviderId <> @ProcessProviderId
	begin
		select 'PROVIDER MIS-MATCH', @ProviderId, @ProcessProviderId
		return
	end

	/*
	IF @ProcessId = 0
	begin
		select @ProcessId = coalesce(max(p.process_id),0)
		from gdsdb.GDS.dbo.processes p with(nolock)
		where p.provider_id = @ProviderId
	end
	--*/
	begin try
		-- process diff pickups only
		declare @GDS_PICKUPS table (
			PROVIDER_PICKUP_ID VARCHAR(200),
			PICKUP_TIME DATETIME,
			ROUTE_CODE VARCHAR(200),
			unique clustered (ROUTE_CODE, PICKUP_TIME, PROVIDER_PICKUP_ID)
		)
		
		INSERT INTO @GDS_PICKUPS (PROVIDER_PICKUP_ID, PICKUP_TIME, ROUTE_CODE)
		SELECT DISTINCT P.provider_pickup_id, RP.pickup_time, RBP.route_code
		from gdsdb.GDS.dbo.pickups P with (nolock) 
		inner join gdsdb.GDS.dbo.route_pickups RP with (nolock) on P.pickup_id = RP.pickup_id
		inner join gdsdb.GDS.dbo.routes_by_provider RBP with (nolock) on RP.route_schedule_id = RBP.route_schedule_id
		where P.provider_id = @ProviderId AND RP.is_active = 1
		
		DECLARE @PULL_PICKUPS table (
			PROVIDER_PICKUP_ID VARCHAR(200),
			PICKUP_TIME DATETIME,
			ROUTE_CODE VARCHAR(200),
			unique clustered (ROUTE_CODE, PICKUP_TIME, PROVIDER_PICKUP_ID)
		)

		INSERT INTO @PULL_PICKUPS (PROVIDER_PICKUP_ID, PICKUP_TIME, ROUTE_CODE)
		select distinct
			bp.BoardingPointID
			, cast(bp.JourneyDate as datetime)+cast(ltrim(replace(bp.ArrivalTime,bp.BoardingPointName,'')) as datetime)
			, bp.route_code
		from BoardingPoints bp with(nolock)
			inner join OnwardServices os with(nolock) on os.route_code = bp.route_code and os.process_id = bp.process_id
		where 0=0
			and bp.process_id = @ProcessId
			and bp.route_code is not null
		
		DECLARE @RES_IDS table ( ROUTE_CODE VARCHAR(200) )
		
		INSERT INTO @RES_IDS
		SELECT DISTINCT P.ROUTE_CODE
		FROM @PULL_PICKUPS P
			LEFT JOIN @GDS_PICKUPS G ON P.ROUTE_CODE = G.ROUTE_CODE AND P.PROVIDER_PICKUP_ID = G.PROVIDER_PICKUP_ID
		WHERE G.ROUTE_CODE IS null OR P.PICKUP_TIME <> G.PICKUP_TIME

		INSERT INTO GDSDB.GDS.DBO._PICKUPS (
			process_id,
			provider_id,
			route_code,
			provider_pickup_id,
			pickup_name,
			pickup_time,
			pickup_address,
			pickup_landmark,
			pickup_phone,
			PROVIDER_CITY_ID
		) SELECT DISTINCT
			@ProcessId
			, @ProviderId
			, bp.route_code
			, bp.BoardingPointID
			, bp.BoardingPointName
			, cast(ltrim(replace(bp.ArrivalTime,bp.BoardingPointName,'')) as smalldatetime)
			, bp.BoardingPointAddress
			, bp.BoardingPointLandmark
			, bp.BoardingPointContactNo
			, os.SourceStationID
		FROM BoardingPoints bp with (nolock)
			inner join OnwardServices os with (nolock) on os.process_id = bp.process_id and os.route_code = bp.route_code
			inner join @RES_IDS r on r.ROUTE_CODE = os.route_code
		WHERE 0=0
			and bp.process_id = @ProcessId
			and bp.route_code is not null
			and bp.BoardingPointName is not null
			and bp.ArrivalTime is not null 
	end try
	begin catch
		insert into sp_error_logs(
			process_id
			, sp_name
			, error_no
			, error_line
			, error_msg
			, log_date
			, error_data
		) select @ProcessId
			, error_procedure()
			, error_number()
			, error_line()
			, error_message()
			, dateadd(MI,690,current_timestamp)
			, '[01]Bulk Pickup Insert Failed:ProcessId='+convert(varchar,@ProcessId)+'RouteCode='+@RouteCode
	end catch

	if @IsProcess = 1
	begin
		insert into sp_status_log(sp_name
			, process_id
			, spid
			, proc_status
			, log_date
			, status
		) select 'Pull_AshokaVL.dbo.ATA_Process_Pickups'
			, @ProcessId
			, sp.spid
			, sp.status
			, dateadd(MI,690,current_timestamp)
			, 'd'
		from master..sysprocesses sp where sp.spid = @@spid
		exec gdsdb.GDS.dbo._PROCESS_PICKUPS @ProcessId
	end

	insert into sp_status_log(sp_name
		, process_id
		, spid
		, proc_status
		, log_date
		, status
	) select 'Pull_AshokaVL.dbo.ATA_Process_Pickups'
		, @ProcessId
		, sp.spid
		, sp.status
		, dateadd(MI,690,current_timestamp)
		, 'c'
	from master..sysprocesses sp where sp.spid = @@spid
end

if object_id('ATA_Process_Dropoffs') IS NULL
    exec('create procedure ATA_Process_Dropoffs as select 1')
go
-- =============================================
-- Author:		sWaRtHi
-- Create date: January 23, 2015
-- Description: Process Routes Dropoffs
-- =============================================
-- Exec SP_HELPTEXT ATA_Process_Dropoffs
-- =============================================
alter procedure ATA_Process_Dropoffs
	@ProcessId int,
	@IsProcess smallint = 1,
	@RouteCode varchar(100) = ''
as
begin
	set nocount on;

    insert into sp_status_log(sp_name
		, process_id
		, spid
		, proc_status
		, log_date
		, status
	) select 'Pull_AshokaVL.dbo.ATA_Process_Dropoffs'
		, @ProcessId
		, sp.spid
		, sp.status
		, dateadd(MI,690,current_timestamp)
		, 's'
	from master..sysprocesses sp with(nolock) where sp.spid = @@spid

	declare
		@ProviderId int,
		@ProviderName varchar(50) = 'Ashoka Travel Agency',
		@ProcessProviderId int

	select @ProviderId = p.provider_id from gdsdb.GDS.dbo.providers p with(nolock) where p.provider_name = @ProviderName
	select @ProcessProviderId = p.provider_id from gdsdb.GDS.dbo.processes p with(nolock) where p.process_id = @ProcessId
	if @ProviderId <> @ProcessProviderId
	begin
		select 'PROVIDER MIS-MATCH', @ProviderId, @ProcessProviderId
		return
	end

	/*
	IF @ProcessId = 0
	begin
		select @ProcessId = coalesce(max(p.process_id),0)
		from gdsdb.GDS.dbo.processes p with(nolock)
		where p.provider_id = @ProviderId
	end
	--*/
	begin try
		insert into gdsdb.GDS.dbo._DROPOFFS (
			process_id,
			provider_id,
			route_code,
			provider_dropoff_id,
			dropoff_name,
			dropoff_time
		) select distinct
			@ProcessId
			, @ProviderId
			, dp.route_code
			, dp.DroppingPointID
			, dp.DroppingPointName
			, cast(dp.JourneyDate as smalldatetime)
		from DroppingPoints dp with(nolock)
			inner join gdsdb.gds.dbo.routes_by_provider rbp with(nolock) on rbp.route_code = dp.route_code
			inner join gdsdb.gds.dbo.routes r with(nolock) on r.route_schedule_id = rbp.route_schedule_id
		where 0=0
			and dp.process_id = @ProcessId
			and dp.DroppingPointID is not null
			and dp.DroppingPointName is not null
			and dp.route_code is not null
	end try
	begin catch
		insert into sp_error_logs(
			process_id
			, sp_name
			, error_no
			, error_line
			, error_msg
			, log_date
			, error_data
		) select @ProcessId
			, error_procedure()
			, error_number()
			, error_line()
			, error_message()
			, dateadd(MI,690,current_timestamp)
			, '[01]Bulk Dropoff Insert Failed:ProcessId='+convert(varchar,@ProcessId)+'RouteCode='+@RouteCode
	end catch

	if @IsProcess = 1
	begin
		insert into sp_status_log(sp_name
			, process_id
			, spid
			, proc_status
			, log_date
			, status
		) select 'Pull_AshokaVL.dbo.ATA_Process_Dropoffs'
			, @ProcessId
			, sp.spid
			, sp.status
			, dateadd(MI,690,current_timestamp)
			, 'd'
		from master..sysprocesses sp where sp.spid = @@spid
		exec gdsdb.GDS.dbo._PROCESS_DROPOFFS @ProcessId
	end

	insert into sp_status_log(sp_name
		, process_id
		, spid
		, proc_status
		, log_date
		, status
	) select 'Pull_AshokaVL.dbo.ATA_Process_Dropoffs'
		, @ProcessId
		, sp.spid
		, sp.status
		, dateadd(MI,690,current_timestamp)
		, 'c'
	from master..sysprocesses sp where sp.spid = @@spid
end

if object_id('ATA_Process_Data_NoChart') IS NULL
    exec('create procedure ATA_Process_Data_NoChart as select 1')
go
-- =============================================
-- Author:		sWaRtHi
-- Create date: January 23, 2015
-- Description: Process Data without chart
-- =============================================  
-- Exec SP_HELPTEXT ATA_Process_Data_NoChart
-- =============================================  
alter procedure ATA_Process_Data_NoChart
	@ProcessId int,
	@IsProcess smallint = 1
as
begin
	set nocount on;

    insert into sp_status_log(sp_name
		, process_id
		, spid
		, proc_status
		, log_date
		, status
	) select 'Pull_AshokaVL.dbo.ATA_Process_Data_NoChart'
		, @ProcessId
		, sp.spid
		, sp.status
		, dateadd(MI,690,current_timestamp)
		, 's'
	from master..sysprocesses sp where sp.spid = @@spid

	--Get Provider Id
	declare
		@ProviderId int,
		@ProviderName varchar(50) = 'Ashoka Travel Agency',
		@ProcessProviderId int

	select @ProviderId = p.provider_id from gdsdb.GDS.dbo.providers p with(nolock) where p.provider_name = @ProviderName
	select @ProcessProviderId = p.provider_id from gdsdb.GDS.dbo.processes p with(nolock) where p.process_id = @ProcessId
	if @ProviderId <> @ProcessProviderId
	begin
		select 'PROVIDER MIS-MATCH', @ProviderId, @ProcessProviderId
		return
	end

	/*
	IF @ProcessId = 0
	begin
		select @ProcessId = coalesce(max(p.process_id),0)
		from gdsdb.GDS.dbo.processes p with(nolock)
		where p.provider_id = @ProviderId
	end
	--*/

	begin try
		exec ATA_Process_Cities @ProcessId, @IsProcess
		exec ATA_Process_Citiy_Pairs @ProcessId, @IsProcess
		exec ATA_Process_Companies @ProcessId, @IsProcess

		exec ATA_Process_Routes @ProcessId
		exec ATA_Process_Route_Fares @ProcessId
		--exec TE_Process_Cancellation_Policies @ProcessId
		exec ATA_Process_Pickups @ProcessId
		exec ATA_Process_Dropoffs @ProcessId
	end try
	begin catch
		insert into sp_error_logs(process_id
			, sp_name
			, error_no
			, error_line
			, error_msg
			, log_date
			, error_data
		) select @ProcessId
			, error_procedure()
			, error_number()
			, error_line()
			, error_message()
			, current_timestamp
			, ''
	end catch

	insert into sp_status_log(sp_name
		, process_id
		, spid
		, proc_status
		, log_date
		, status
	) select 'Pull_AshokaVL.dbo.ATA_Process_Data_NoChart'
		, @ProcessId
		, sp.spid
		, sp.status
		, dateadd(MI,690,current_timestamp)
		, 'c'
	from master..sysprocesses sp where sp.spid = @@spid
end

if object_id('ATA_Save_Seat_Layout') IS NULL
    exec('create procedure ATA_Save_Seat_Layout as select 1')
go
--seat_layout_details
alter procedure ATA_Save_Seat_Layout
	@PROCESS_ID INT,
	@ROUTE_CODE VARCHAR(50),
	@FROM_STATION_ID INT,
	@TO_STATION_ID INT,
	@ROUTE_NAME VARCHAR(100),
	@JOURNEY_DATE DATETIME,
	@SERVICE_ID INT,
	@SERVICE_NUMBER VARCHAR(50),
	@COACH_TYPE_ID INT,
	@COACH_TYPE_NAME VARCHAR(100),
	@SEAT_LAYOUT_ID INT,
	@SEAT_TYPE VARCHAR(20),
	@TOTAL_ROWS INT,
	@TOTAL_COLUMNS INT,
	@DIVIDER_ROW INT,
	@OCCUPIED_SEAT_LIST VARCHAR(250),
	@BALANCE_SEATS INT,
	@AVAILABLE_SEATS VARCHAR(100),
	@DRIVER_SEATS VARCHAR(100),
	@TICKET_FARE DECIMAL(7,2),
	@SEAT_DETAILS VARCHAR(8000),
	@ROW_NOS VARCHAR(8000),
	@COL_NOS VARCHAR(8000),
	@CABIN_NOS VARCHAR(8000),
	@SEAT_NOS VARCHAR(8000),
	@STATUSES VARCHAR(8000),
	@DELIMITER VARCHAR(10),
	@TO_PROCESS bit =0
AS 
BEGIN

	set nocount on
	
	
	declare @SEAT_DETAILS_TAB table (
		id int identity,
		SEAT_DETAIL VARCHAR(200))
	
	declare @ROW_NOS_TAB table (
		id int identity,
		ROW_NO int)
		
	declare @COL_NOS_TAB table (
		id int identity,
		COL_NO int)
		
	DECLARE @CABIN_NOS_TAB table (
		id int identity,
		CABIN_NO int)
		
	DECLARE @SEAT_NOS_TAB table (
		id int identity,
		SEAT_NOS VARCHAR(10))
		
				
	DECLARE @STATUSES_TAB table (
		id int identity,
		[STATUS] VARCHAR(10))
			
		
	insert into @SEAT_DETAILS_TAB(SEAT_DETAIL) select * from dbo.parmstolist(@SEAT_DETAILS,@DELIMITER)	
	insert into @ROW_NOS_TAB(ROW_NO) select * from dbo.parmstolist(@ROW_NOS,@DELIMITER)
	insert into @COL_NOS_TAB(COL_NO) select * from dbo.parmstolist(@COL_NOS,@DELIMITER)
	insert into @CABIN_NOS_TAB(CABIN_NO) select * from dbo.parmstolist(@CABIN_NOS,@DELIMITER)
	insert into @SEAT_NOS_TAB(SEAT_NOS) select * from dbo.parmstolist(@SEAT_NOS,@DELIMITER)
	insert into @STATUSES_TAB([STATUS]) select * from dbo.parmstolist(@STATUSES,@DELIMITER)
		
		

	
		INSERT INTO seat_layout (process_id,route_code,from_station_id,to_station_id,route_name,journey_date,service_id,service_number,coach_type_id,coach_type_name,
							seat_layout_id,seat_type,total_rows,total_columns,divider_row,occupied_seat_list,balance_seats,available_seats,driver_seats,ticket_fare)
				VALUES ( @PROCESS_ID, @ROUTE_CODE,@FROM_STATION_ID,@TO_STATION_ID,@ROUTE_NAME,@JOURNEY_DATE,@SERVICE_ID,@SERVICE_NUMBER,@COACH_TYPE_ID,@COACH_TYPE_NAME,
							@SEAT_LAYOUT_ID,@SEAT_TYPE,@TOTAL_ROWS,@TOTAL_COLUMNS,@DIVIDER_ROW,@OCCUPIED_SEAT_LIST,@BALANCE_SEATS,@AVAILABLE_SEATS,@DRIVER_SEATS,@TICKET_FARE)
	
							
		INSERT INTO seat_layout_details (
						process_id,
						route_code,
						journey_date,
						seat_detail,
						row_no,
						col_no,
						cabin_no,
						seat_no,
						[status]
									)
				SELECT  @PROCESS_ID,
						@ROUTE_CODE,
						@JOURNEY_DATE,
						SD.SEAT_DETAIL,
						R.ROW_NO,
						C.COL_NO,
						CB.CABIN_NO,
						SB.SEAT_NOS ,
						S.[STATUS]
				FROM @SEAT_DETAILS_TAB SD INNER JOIN @ROW_NOS_TAB R ON R.id = SD.id
										  INNER JOIN @COL_NOS_TAB C ON C.id = SD.id
										  INNER JOIN @CABIN_NOS_TAB CB ON CB.id = SD.id
										  INNER JOIN @SEAT_NOS_TAB SB ON SB.id = SD.id
										  INNER JOIN @STATUSES_TAB S ON S.id = SD.id
					WHERE  NOT (SD.SEAT_DETAIL IS NULL AND R.ROW_NO IS  NULL AND C.COL_NO IS  NULL AND
								CB.CABIN_NO IS  NULL AND SB.SEAT_NOS IS  NULL AND S.STATUS IS  NULL)
								
							
	--===============================================================================
	--update the number of records inserted for the given process_id and tablename
	
	declare @pulled_records int = @@rowcount

	if exists(select * from  GDSDB.GDS.DBO.pulled_records where process_id = @process_id and table_name='seat_layout')
	begin
		update  GDSDB.GDS.DBO.pulled_records
		set records = records+@pulled_records
		where process_id = @process_id and table_name='seat_layout'
	end
	else
	begin
		insert into  GDSDB.GDS.DBO.pulled_records
		select @process_id,'seat_layout',@pulled_records
	end

	--	update the end time for the process
	exec  GDSDB.GDS.DBO.update_process_end_date @process_id
	
	if @TO_PROCESS = 1
	begin
		exec ATA_PROCESS_CHARTS @PROCESS_ID
	end
END

if object_id('ATA_Process_Charts') IS NULL
    exec('create procedure ATA_Process_Charts as select 1')
go
--SELECT MAX(PROCESS_ID) FROM SEARCH_RESULT
--EXEC PROCESS_CHARTS 525919,'25-3-4-5'
ALTER PROCEDURE [dbo].[ATA_Process_Charts](
	@PROCESS_ID INT,
	@PROCESS_ROUTE_CODE VARCHAR(50) = ''
)AS
BEGIN
	SET NOCOUNT ON;

	insert into sp_status_log(
    	sp_name,
		process_id,
		spid,
		proc_status,
		log_date,
		status
	) select 'Pull_AshokaVL.DBO.ATA_PROCESS_CHARTS'
		, @PROCESS_ID
		, sp.spid
		, sp.status
		, dateadd(MI,690,current_timestamp)
		, 's'
	from master..sysprocesses sp where sp.spid = @@spid

	
	--GET THE PROVIDER_ID
	DECLARE @PROVIDER_ID INT
	DECLARE @PROVIDER_NAME VARCHAR(50) = 'Ashoka Travel Agency'
	DECLARE @PROCESS_PROVIDER_ID INT
	SELECT @PROVIDER_ID=PROVIDER_ID FROM  GDSDB.GDS.DBO.PROVIDERS WHERE PROVIDER_NAME=@PROVIDER_NAME
	SELECT @PROCESS_PROVIDER_ID=PROVIDER_ID FROM  GDSDB.GDS.DBO.PROCESSES WHERE PROCESS_ID=@PROCESS_ID
	
	--IF THE PROCESS_IDs DO NOT MATCH, GET OUT...
	IF @PROVIDER_ID<> @PROCESS_PROVIDER_ID
	BEGIN
		SELECT 'PROVIDER MIS-MATCH', @PROVIDER_ID, @PROCESS_PROVIDER_ID
		RETURN 
	END
	
	--STRUCTURE OF THE INPUT DATA FOR THIS PROCESS_ID 
	DECLARE @_CHARTS TABLE(
		JOURNEY_DATE SMALLDATETIME,
		ROUTE_CODE VARCHAR(50),
		[ROW_NO] INT,
		[COLUMN_NO] INT,
		[SEAT_NO] VARCHAR(10),
		[HEIGHT] INT,
		[WIDTH] INT,
		[SEAT_TYPE] VARCHAR(10),
		[IS_AC] INT,
		[IS_SLEEPER] INT,
		[IS_AISLE] INT,
		[DECK] INT DEFAULT(1),
		[CHART_ID] INT
	)

	IF @PROCESS_ROUTE_CODE <> ''
	BEGIN	
		INSERT INTO @_CHARTS
		SELECT DISTINCT 
			NULL,
			SLD.route_code,
			SLD.col_no-1,
			RCD.MAX_ROW_NO - SLD.row_no,
			SLD.seat_no,
			1,
			CASE WHEN SL.seat_type='Seat' then 1 else 2 end,
			CASE WHEN SL.seat_type='Seat' then 'sit' else 'slp' end,
			R.has_ac, --[IS_AC], SETS WHETHER THIS SEAT IS AN AC OR NAC; FOR REDBUS THE BUS IS EITHER AC OR NAC
			CASE WHEN SL.seat_type='Seat' then 0 else 1 end,
			0,
			CASE WHEN SL.seat_type='Seat' then 1 else case when CHARINDEX('L',SLD.seat_no) > 0 then 1 else 2 end end,
			SL.seat_layout_id
		FROM seat_layout_details SLD WITH (NOLOCK)
		INNER JOIN  GDSDB.GDS.DBO.routes_by_provider RBP WITH (NOLOCK)
			ON SLD.route_code = RBP.route_code AND RBP.provider_id = @PROVIDER_ID
		INNER JOIN  GDSDB.GDS.DBO.[ROUTES] R WITH (NOLOCK)
			ON RBP.route_schedule_id = R.route_schedule_id 
		INNER JOIN (SELECT route_code, MAX(journey_date) AS JOURNEY_DATE,MAX(row_no) MAX_ROW_NO
					FROM seat_layout_details WITH (NOLOCK)
					WHERE process_id=@PROCESS_ID
					GROUP BY route_code
					) RCD ON RCD.route_code = SLD.route_code AND RCD.JOURNEY_DATE = SLD.journey_date
		INNER JOIN SEAT_LAYOUT SL WITH (NOLOCK) ON SL.route_code = SLD.route_code AND SLD.JOURNEY_DATE = SL.journey_date
		WHERE SLD.process_id=@PROCESS_ID 
			AND (SLD.route_code = @PROCESS_ROUTE_CODE)
		--ORDER BY SLD.ROUTE_CODE DESC
		
	END
	ELSE
	BEGIN
		INSERT INTO @_CHARTS
		SELECT DISTINCT 
			NULL,
			SLD.route_code,
			SLD.col_no-1,
			RCD.MAX_ROW_NO - SLD.row_no,
			SLD.seat_no,
			1,
			CASE WHEN SL.seat_type='Seat' then 1 else 2 end,
			CASE WHEN SL.seat_type='Seat' then 'sit' else 'slp' end,
			R.has_ac, --[IS_AC], SETS WHETHER THIS SEAT IS AN AC OR NAC; FOR REDBUS THE BUS IS EITHER AC OR NAC
			CASE WHEN SL.seat_type='Seat' then 0 else 1 end,
			0,
			CASE WHEN SL.seat_type='Seat' then 1 else case when CHARINDEX('L',SLD.seat_no) > 0 then 1 else 2 end end,
			SL.seat_layout_id
		FROM seat_layout_details SLD WITH (NOLOCK)
		INNER JOIN  GDSDB.GDS.DBO.routes_by_provider RBP WITH (NOLOCK)
			ON SLD.route_code = RBP.route_code AND RBP.provider_id = @PROVIDER_ID
		INNER JOIN  GDSDB.GDS.DBO.[ROUTES] R WITH (NOLOCK)
			ON RBP.route_schedule_id = R.route_schedule_id 
		INNER JOIN (SELECT route_code, MAX(journey_date) AS JOURNEY_DATE,MAX(row_no) MAX_ROW_NO
					FROM seat_layout_details WITH (NOLOCK)
					WHERE process_id=@PROCESS_ID
					GROUP BY route_code
					) RCD ON RCD.route_code = SLD.route_code AND RCD.JOURNEY_DATE = SLD.journey_date
		INNER JOIN SEAT_LAYOUT SL WITH (NOLOCK) ON SL.route_code = SLD.route_code AND SLD.JOURNEY_DATE = SL.journey_date
		WHERE SLD.process_id=@PROCESS_ID 
	END	

	DECLARE @MAX_ROWS INT = 0
	
	DECLARE @ROUTE_MAX_ROWS TABLE(
		ROUTE_CODE VARCHAR(50),
		MAX_ROWS INT
	)
	
	INSERT INTO @ROUTE_MAX_ROWS
	SELECT ROUTE_CODE, MAX(ROW_NO)
	FROM @_CHARTS
	WHERE (ROUTE_CODE=@PROCESS_ROUTE_CODE OR ''= @PROCESS_ROUTE_CODE)
	GROUP BY ROUTE_CODE
	ORDER BY ROUTE_CODE


	DECLARE @ROUTE_CODE VARCHAR(50)
	DECLARE @ROW_NO INT
	DECLARE @COLUMN_NO INT
	DECLARE @SEAT_NO VARCHAR(10)
	DECLARE @HEIGHT INT
	DECLARE @WIDTH INT
	DECLARE @SEAT_TYPE  VARCHAR(10)
	DECLARE @IS_AISLE INT
	DECLARE @DECK INT
	
	
	DECLARE @_TEMP TABLE(
		ROUTE_CODE VARCHAR(50),
		[ROW_NO] INT,
		[COLUMN_NO] INT,
		[SEAT_NO] VARCHAR(10),
		[HEIGHT] INT,
		[WIDTH] INT,
		[SEAT_TYPE] VARCHAR(10),
		[IS_AC] INT,
		[IS_SLEEPER] INT,
		[IS_AISLE] INT,
		[DECK] INT DEFAULT(1),
		[CHART_ID] INT)
		
	DECLARE @_TEMP2 TABLE(
		ROUTE_CODE VARCHAR(50),
		[ROW_NO] INT,
		[COLUMN_NO] INT,
		[SEAT_NO] VARCHAR(10),
		[HEIGHT] INT,
		[WIDTH] INT,
		[SEAT_TYPE] VARCHAR(10),
		[IS_AC] INT,
		[IS_SLEEPER] INT,
		[IS_AISLE] INT,
		[DECK] INT DEFAULT(1),
		[CHART_ID] INT)
		
	DECLARE @COUNTER INT = 1
	DECLARE @LAST_ROUTE_CODE VARCHAR(50)

	DECLARE CUR CURSOR FOR 
	SELECT ROUTE_CODE, MAX_ROWS FROM @ROUTE_MAX_ROWS ORDER BY ROUTE_CODE DESC
	
	OPEN CUR
	FETCH NEXT FROM CUR INTO @ROUTE_CODE, @MAX_ROWS
	WHILE @@FETCH_STATUS = 0 
	BEGIN
		BEGIN TRY
		
			--SET THE CURRENT ROUTE_CODE
			SET @LAST_ROUTE_CODE = @ROUTE_CODE
			
			delete from  GDSDB.GDS.DBO._CHARTS where ROUTE_CODE= @ROUTE_CODE and process_id = @PROCESS_ID
			
			if exists (select 1 from @_CHARTS where IS_SLEEPER = 1 and ROUTE_CODE = @ROUTE_CODE)
			BEGIN
				
				delete from @_TEMP
				
				INSERT INTO @_TEMP 
				SELECT DISTINCT 
					ROUTE_CODE,ROW_NO,COLUMN_NO,
					SEAT_NO,HEIGHT,WIDTH,
					SEAT_TYPE,[IS_AC],[IS_SLEEPER],
					IS_AISLE,DECK,[CHART_ID]
				FROM @_CHARTS
				WHERE ROUTE_CODE = @ROUTE_CODE 
				
				DECLARE @ROWNO INT
				DECLARE @ROUTECODE2 VARCHAR(50)
				DECLARE @CHARTID2 INT
				DECLARE @ROWCOUNT INT = 0
				DECLARE CURTEMP CURSOR FOR SELECT DISTINCT ROW_NO,ROUTE_CODE,CHART_ID FROM @_TEMP
				OPEN CURTEMP 
				FETCH NEXT FROM CURTEMP INTO @ROWNO,@ROUTECODE2,@CHARTID2
				
				WHILE @@FETCH_STATUS = 0
				BEGIN
					
					INSERT INTO @_TEMP2
					SELECT ROUTE_CODE,@ROWCOUNT,case when COLUMN_NO > 0 then COLUMN_NO +1 else COLUMN_NO end,
						SEAT_NO,HEIGHT,WIDTH,SEAT_TYPE,[IS_AC],[IS_SLEEPER],IS_AISLE,DECK,[CHART_ID]
					FROM @_TEMP
						WHERE ROW_NO = @ROWNO
						
					IF @ROWNO % 2 = 0
					BEGIN
					
						--UPDATE @_TEMP SET ROW_NO = ROW_NO +1 WHERE ROW_NO > @ROWNO
						--UPDATE @_TEMP SET COLUMN_NO = COLUMN_NO + 1 WHERE ROW_NO = @ROWNO AND COLUMN_NO > 0
						SET @ROWCOUNT =@ROWCOUNT + 1
						INSERT INTO @_TEMP2 VALUES(@ROUTECODE2,@ROWCOUNT,0,'',1,1,'aisle',0,0,1,1,@CHARTID2)
						INSERT INTO @_TEMP2 VALUES(@ROUTECODE2,@ROWCOUNT,1,'',1,1,'aisle',0,0,1,1,@CHARTID2)
						INSERT INTO @_TEMP2 VALUES(@ROUTECODE2,@ROWCOUNT,2,'',1,1,'aisle',0,0,1,2,@CHARTID2)
						INSERT INTO @_TEMP2 VALUES(@ROUTECODE2,@ROWCOUNT,3,'',1,1,'aisle',0,0,1,2,@CHARTID2)
					END
					
					SET @ROWCOUNT =@ROWCOUNT + 1
						
					FETCH NEXT FROM CURTEMP INTO @ROWNO,@ROUTECODE2,@CHARTID2
				END
				
				DEALLOCATE CURTEMP 
				
				--SELECT * FROM @_TEMP2
				
				INSERT INTO  GDSDB.GDS.DBO._CHARTS(
					PROCESS_ID,PROVIDER_ID,JOURNEY_DATE,
					ROUTE_CODE,[ROW_NO],[COLUMN_NO],
					[SEAT_NO],[HEIGHT],[WIDTH],
					[SEAT_TYPE],is_aisle,DECK,EXCEPTION,
					IS_AC,IS_SLEEPER,PROVIDER_CHART_ID
				)
				select @PROCESS_ID,@PROVIDER_ID,null,
						ROUTE_CODE,ROW_NO,COLUMN_NO,
						SEAT_NO,HEIGHT,WIDTH,
						SEAT_TYPE,IS_AISLE,DECK,'',
						IS_AC,IS_SLEEPER,CHART_ID
				from @_TEMP2
			
			END
			ELSE
			BEGIN
				
				INSERT INTO  GDSDB.GDS.DBO._CHARTS(
					PROCESS_ID,PROVIDER_ID,JOURNEY_DATE,
					ROUTE_CODE,[ROW_NO],[COLUMN_NO],
					[SEAT_NO],[HEIGHT],[WIDTH],
					[SEAT_TYPE],is_aisle,DECK,EXCEPTION,
					IS_AC,IS_SLEEPER,PROVIDER_CHART_ID
				)
				SELECT DISTINCT 
					@PROCESS_ID,@PROVIDER_ID,NULL,
					ROUTE_CODE,ROW_NO,COLUMN_NO,
					SEAT_NO,HEIGHT,WIDTH,
					SEAT_TYPE,IS_AISLE,DECK,
					'',[IS_AC],[IS_SLEEPER],[CHART_ID]
				FROM @_CHARTS
				WHERE ROUTE_CODE = @ROUTE_CODE 
			
			END
			

		END TRY
		BEGIN CATCH
			--LOG ERRORS
			INSERT INTO SP_ERROR_LOGS
			SELECT
				@PROCESS_ID, 
				ERROR_PROCEDURE(),
				ERROR_NUMBER(),
				ERROR_LINE(),
				ERROR_MESSAGE(),
				GETDATE(),
				'ROUTE_CODE='+@ROUTE_CODE
		END CATCH
		--PRINT COUNTERS
		PRINT CONVERT(VARCHAR,@COUNTER) + ': Loading Chart Data for RouteCode = '+ @ROUTE_CODE
		SET @COUNTER = @COUNTER + 1	
		
		--FETCH THE NEXT SET OF RECORDS
		FETCH NEXT FROM CUR INTO @ROUTE_CODE, @MAX_ROWS
	END
	CLOSE CUR
	DEALLOCATE CUR

	EXEC  GDSDB.GDS.DBO._PROCESS_CHARTS @PROCESS_ID, 0, 1  --(pass process_id, find_deck, re_process-layout)

	insert into sp_status_log(
    	sp_name,
		process_id,
		spid,
		proc_status,
		log_date,
		status
	) select 'Pull_AshokaVL.DBO.ATA_PROCESS_CHARTS'
		, @PROCESS_ID
		, sp.spid
		, sp.status
		, dateadd(MI,690,current_timestamp)
		, 'c'
	from master..sysprocesses sp where sp.spid = @@spid
END