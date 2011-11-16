CREATE TABLE IF NOT EXISTS users (
	user_id BIGINT not null AUTO_INCREMENT PRIMARY KEY,
 	user_name varchar(64) not null,
	real_name varchar(64) not null,
	email_address varchar(64) not null,
	passwd_hash varchar(64)
);
insert into users (user_name,real_name,email_address) values
("cbassa","Cees Bassa","bassa@jb.man.ac.uk"),
("gdesvignes","Gregory Desvignes","gdesvignes@mpifr-bonn.mpg.de"),
("gemma","Gemma Janssen","gemma.janssen@manchester.ac.uk"),
("hessels","Jason Hessels","hessels@astron.nl"),
("joris","Joris Verbiest","verbiest@mpifr-bonn.mpg.de"),
("plazar","Patrick Lazarus","plazarus@mpifr-bonn.mpg.de"),
("ramesh","Ramesh Karuppusamy","ramesh@mpifr-bonn.mpg.de");

