use master
go
create database Pull_TicketEngine
go
use Pull_TicketEngine
go
create table cities(
	process_id int,
	city_id int,
	city_name varchar(100)
)
go
create table services ( 
	process_id int,
	route_code varchar(100),
	service_number varchar(50),
	travel_id int,
	travel_name varchar(100),
	from_id int,
	from_name varchar(100),
	to_id int,
	to_name varchar(100),
	journey_date date,
	bus_type varchar(20),
	bus_model varchar(100),
	seat_fare decimal(8,2),
	lb_fare decimal(8,2),
	ub_fare decimal(8,2),
	dep_time time,
	journey_time varchar(20),
	arrival_time time,
	available_seats int,
	total_seats int
)
go
create table boarding_points (
	process_id int,
	route_code varchar(100),
	bpid varchar(100),
	pickup_point varchar(100),
	landmark varchar(300),
	city_id int,
	city_name varchar(100),
	time time,
	van_pickup varchar(10),
	type varchar(10),
)
go
create table cancellation_policies (
	process_id int,
	route_code varchar(100),
	charges varchar(10),
	from_time varchar(10),
	to_time varchar(10)
)