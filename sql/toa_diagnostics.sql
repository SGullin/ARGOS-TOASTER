create table if not exists toa_diagnostic (
	toa_id bigint not null,
	value double not null,
	type varchar(16) not null,
	primary key(toa_id,type)
);
