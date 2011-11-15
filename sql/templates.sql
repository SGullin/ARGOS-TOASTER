CREATE TABLE IF NOT EXISTS templates (
	template_id BIGINT not null AUTO_INCREMENT PRIMARY KEY,
	pulsar_id varchar(20) not null unique,
	obssystem_id BIGINT not null,
	filename varchar(512) not null,
	filepath varchar(512) not null,	
	md5sum varchar(64) not null,
	add_time DATETIME not null,
	nbin mediumint unsigned,
	is_analytic bool not null,
	user_id bigint not null,
	comments text not null
);
