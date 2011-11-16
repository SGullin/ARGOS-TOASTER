create table if not exists telescopes (
	telescope_id bigint not null auto_increment primary key,
	name varchar(64) not null,
	latitude double,
	longitude double,
	datum varchar(64),
	itrf_x double not null,
	itrf_y double not null,
	itrf_z double not null,	
	telescope_abbrev varchar(16) not null,
	telescope_code varchar(2) not null
);
insert into telescopes (name,itrf_x,itrf_y,itrf_z,telescope_abbrev,telescope_code) values
("Effelsberg",4033949.5,486989.4,4900430.8,"eff","g"),
("Jodrell",3822626.04,-154105.65,5086486.04,"jb","8"),
("Nancay",4324165.81,165927.11,4670132.83,"ncy","f"),
("WSRT",3828445.659,445223.600000,5064921.5677,"wsrt","i");
