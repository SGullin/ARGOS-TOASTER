create table if not exists parfiles (
	parfile_id BIGINT UNSIGNED not NULL AUTO_INCREMENT primary key, 
	pulsar_id BIGINT not null,
	md5sum varchar(64) not null,
        add_time DATETIME not null,
        filename varchar(512) not null,
        filepath varchar(512) not null,
	user_id bigint not null,
	PSRJ VARCHAR(12), 
	PSRB VARCHAR(12), 
	RAJ VARCHAR(32),
	DECJ VARCHAR(32),
	PEPOCH DOUBLE,
	F0 DOUBLE,
	F1 DOUBLE,
	DM DOUBLE,
	PMRA DOUBLE,
	PMDEC DOUBLE,
	PX DOUBLE,
	DMEPOCH DOUBLE,
	POSEPOCH DOUBLE,
	BINARY_MODEL VARCHAR(6),
	PB DOUBLE,
	ECC DOUBLE,
	A1 DOUBLE,
	T0 DOUBLE,
	OM DOUBLE,
	TASC DOUBLE,
	EPS1 DOUBLE,
	EPS2 DOUBLE,
	PBDOT DOUBLE,
	OMDOT DOUBLE,
	A1DOT DOUBLE,
	SINI DOUBLE,
	M2 DOUBLE,
	START DOUBLE,
	FINISH DOUBLE,
	EPHEM VARCHAR(8),
	CLK VARCHAR(16),
	TZRMJD DOUBLE,
	TZRFRQ DOUBLE,
	TZRSITE VARCHAR(2)
);
