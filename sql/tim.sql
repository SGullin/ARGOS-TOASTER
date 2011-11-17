create table if not exists toa_tim (
	timfile_id BIGINT not null, 	
	toa_id bigint not null,
	primary key(timfile_id,toa_id)
);
