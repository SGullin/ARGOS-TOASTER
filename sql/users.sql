CREATE TABLE IF NOT EXISTS users (
	user_id BIGINT not null AUTO_INCREMENT PRIMARY KEY,
 	user_name varchar(64) not null,
	real_name varchar(64) not null,
	email_address varchar(64) not null,
	passwd_hash varchar(64) not null
);