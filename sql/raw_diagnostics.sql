create table if not exists raw_diagnostic (
	rawfile_id bigint not null,
	value double not null,
	type varchar(16) not null,
	primary key(rawfile_id,type)
);
