-- process
CREATE TABLE IF NOT EXISTS process (
	process_id BIGINT not null AUTO_INCREMENT PRIMARY KEY,
	version_id BIGINT not null,
	rawfile_id BIGINT not null,
	proc_start_time DATETIME not null, 
	input_args TEXT not null,
	template_id BIGINT not null,
	parfile_id BIGINT not null,
	nchan INT not null,
	nsub INT not null,
	dm DOUBLE not null,
	toa_fitting_method varchar(12) not null,
	polcal_id BIGINT,
	fluxcal_id BIGINT,
	user_id bigint not null
);
