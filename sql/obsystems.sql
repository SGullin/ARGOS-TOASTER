create table if not exists obssystems (
	obssystem_id BIGINT not null auto increment primary key,
	name varchar(64) not null,
	telescope_id bigint not null,
	receiver varchar(64) not null,
	backend varchar(64) not null,
	clock varchar(64) not null
);
 