create table if not exists timfiles (
	timfile_id BIGINT UNSIGNED not NULL AUTO_INCREMENT primary key, 
	user_id bigint not null,
	comments text not null,
	create_time DATETIME not null,
	db_query text not null
);