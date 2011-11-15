create table if not exists telescopes (
	telescope_id bigint not null auto increment primary key,
	name varchar(128) not null,
	latitude double not null,
	longitude double not null,
	datum varchar(64) not null,
	itrf_x double not null,
	itrf_y double not null,
	itrf_z double not null
);
