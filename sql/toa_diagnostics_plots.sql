create table if not exists toa_diagnostic_plots (
	toa_id bigint not null,
	filename varchar(512) not null,
	filepath varchar(512) not null,
	plot_type varchar(16) not null,
	primary key(toa_id,plot_type)
);
