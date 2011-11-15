CREATE TABLE IF NOT EXISTS versions (
	version_id BIGINT not null AUTO_INCREMENT PRIMARY KEY,
	pipeline_githash varchar(64) not null,
	psrchive_githash varchar(64) not null,
	tempo2_cvsrevno varchar(64) not null
);
