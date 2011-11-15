create table if not exists proc_diagnostic_plots (
	process_id bigint not null,
	filename varchar(512) not null,
	filepath varchar(512) not null,
	plot_type varchar(16) not null,
	primary key(process_id,plot_type)
);
