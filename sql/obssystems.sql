create table if not exists obssystems (
	obssystem_id BIGINT not null auto_increment primary key,
	name varchar(64) not null,
	telescope_id bigint not null,
	frontend varchar(64) not null,
	backend varchar(64) not null,
	clock varchar(64) not null,
	code varchar(5) not null
);
insert into obssystems (name,telescope_id,frontend,backend,clock,code) values
("JB_ROACH_L-BAND",2,"L-BAND","ROACH","jbdfb2gps.clk","q"),
("JB_DFB_L-BAND",2,"L_BAND","DFB","jbdfb2gps.clk","q"),
("WSRT_PUMA2",4,"MFFE","PUMA2","wsrt2gps.clk","i"),
("EFF_ASTERIX_7-BEAM",1,"7-BEAM","ASTERIX","eff2gps.clk","g"),
("EFF_ASTERIX_20cm",1,"20cm","ASTERIX","eff2gps.clk","g"),
("EFF_ASTERIX_11cm",1,"11cm","ASTERIX","eff2gps.clk","g"),
("NCY_BON512_L-band",3,"L-band","BON512","ncyobs2obspm.clk","f"),
("NCY_BON512_S-band",3,"S-band","BON512","ncyobs2obspm.clk","f"),
("NCY_BON128_L-band",3,"L-band","BON128","ncy2gps.clk","f"),
("NCY_BON128_S-band",3,"S-band","BON128","ncy2gps.clk","f");
