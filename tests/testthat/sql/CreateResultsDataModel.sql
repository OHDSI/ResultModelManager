-- Drop old tables if exist

DROP TABLE IF EXISTS test_table_1;
DROP TABLE IF EXISTS test_table_2;
DROP TABLE IF EXISTS test_table_3;

-- Create tables

CREATE TABLE test_table_1 (
      database_id VARCHAR NOT NULL,
			analysis_id BIGINT NOT NULL,
			analysis_name VARCHAR NOT NULL,
			domain_id VARCHAR(20),
			start_day FLOAT,
			end_day FLOAT,
			is_binary VARCHAR(1) NOT NULL,
			missing_means_zero VARCHAR(1),
			PRIMARY KEY(database_id,analysis_id)
);

CREATE TABLE test_table_2 (
      database_id VARCHAR NOT NULL,
			analysis2_id BIGINT NOT NULL,
			concept_id INTEGER NOT NULL,
			logic_description VARCHAR,
			valid_start_date DATE NOT NULL,
			concept_name VARCHAR(255) NOT NULL,
			p_10_value FLOAT NOT NULL,
			PRIMARY KEY(database_id,analysis2_id, concept_id)
);

CREATE TABLE test_table_3 (
      database_id VARCHAR NOT NULL,
			analysis3_id BIGINT NOT NULL,
			concept_id INTEGER NOT NULL,
			logic_description VARCHAR,
			valid_start_date DATE NOT NULL,
			concept_name VARCHAR(255) NOT NULL,
			p_10_value FLOAT NOT NULL,
			PRIMARY KEY(database_id,analysis3_id, concept_id)
);