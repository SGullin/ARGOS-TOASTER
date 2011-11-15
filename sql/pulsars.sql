CREATE TABLE IF NOT EXISTS pulsars (
	pulsar_id BIGINT not null AUTO_INCREMENT PRIMARY KEY,
	pulsar_name varchar(20) not null unique,
	master_parfile_id BIGINT not null
);
