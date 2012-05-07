-- toa
CREATE TABLE IF NOT EXISTS toa (
	toa_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	process_id BIGINT NOT NULL,
	template_id BIGINT NOT NULL,
	rawfile_id BIGINT NOT NULL,
	pulsar_id BIGINT NOT NULL,
	obssystem_id BIGINT NOT NULL,
	imjd MEDIUMINT NOT NULL,
	fmjd double NOT NULL,
	freq float NOT NULL,
	toa_unc_us float NOT NULL
);
