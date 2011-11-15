create table if not exists raw_diagnostic_plots (
	rawfile_id bigint not null,
	filename varchar(512) not null,
	filepath varchar(512) not null,
	plot_type varchar(16) not null,
	primary key(rawfile_id,plot_type)
);
