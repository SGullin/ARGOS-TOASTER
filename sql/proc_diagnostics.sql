create table if not exists proc_diagnostic (
	process_id bigint not null,
	value double not null,
	type varchar(16) not null,
	primary key(process_id,type)
);
