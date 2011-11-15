CREATE TABLE IF NOT EXISTS master_templates (
	template_id BIGINT not null,
	pulsar_id BIGINT not null,
	obssystem_id BIGINT not null,
	primary key(pulsar_id,obssystem_id)
);
