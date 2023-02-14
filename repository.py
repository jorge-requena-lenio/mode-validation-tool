import sqlalchemy as sa
from config import config

snowflake_config = config["snowflake"]

db_url = "snowflake://{user}:{password}@{account_identifier}/{database_name}/{schema_name}?warehouse={warehouse_name}&role={role_name}".format(
    user=snowflake_config["user"],
    password=snowflake_config["password"],
    account_identifier=snowflake_config["account_identifier"],
    database_name=snowflake_config["database_name"],
    schema_name=snowflake_config["schema_name"],
    warehouse_name=snowflake_config["warehouse_name"],
    role_name=snowflake_config["role_name"],
)

create_table_sql = """\
CREATE TABLE IF NOT EXISTS mode_report_validation (
	redshift_report VARCHAR(128),
	redshift_run VARCHAR(128),
	redshift_query_run VARCHAR(128),
	snowflake_report VARCHAR(128),
	snowflake_run VARCHAR(128),
	snowflake_query_run VARCHAR(128),
	redshift_total NUMBER(38,0),
	redshift_duplicates NUMBER(38,0),
	redshift_uniques NUMBER(38,0),
	snowflake_total NUMBER(38,0),
	snowflake_duplicates NUMBER(38,0),
	snowflake_uniques NUMBER(38,0),
	total_combined NUMBER(38,0),
	exact_matches NUMBER(38,0),
	redshift_only NUMBER(38,0),
	snowflake_only NUMBER(38,0),
	error VARCHAR(4096),
	created_at TIMESTAMP_NTZ(9) DEFAULT sysdate(),
    PRIMARY KEY (redshift_report, redshift_run, redshift_query_run, snowflake_report, snowflake_run, snowflake_query_run)
);
"""

delete_sql = """\
DELETE FROM
	mode_report_validation
WHERE
	redshift_report = :redshift_report
	AND redshift_run = :redshift_run
	AND redshift_query_run = :redshift_query_run
	AND snowflake_report = :snowflake_report
	AND snowflake_run = :snowflake_run
	AND snowflake_query_run = :snowflake_query_run;
"""

insert_result_sql = """\
INSERT INTO
	mode_report_validation (
		redshift_report,
		redshift_run,
		redshift_query_run,
		snowflake_report,
		snowflake_run,
		snowflake_query_run,
		redshift_total,
		redshift_duplicates,
		redshift_uniques,
		snowflake_total,
		snowflake_duplicates,
		snowflake_uniques,
		total_combined,
		exact_matches,
		redshift_only,
		snowflake_only
	)
VALUES
	(
		:redshift_report,
		:redshift_run,
		:redshift_query_run,
		:snowflake_report,
		:snowflake_run,
		:snowflake_query_run,
		:redshift_total,
		:redshift_duplicates,
		:redshift_uniques,
		:snowflake_total,
		:snowflake_duplicates,
		:snowflake_uniques,
		:total_combined,
		:exact_matches,
		:redshift_only,
		:snowflake_only
	);
"""

insert_error_sql = """\
INSERT INTO
	mode_report_validation (
		redshift_report,
		redshift_run,
		redshift_query_run,
		snowflake_report,
		snowflake_run,
		snowflake_query_run,
		error
	)
VALUES
	(
		:redshift_report,
		:redshift_run,
		:redshift_query_run,
		:snowflake_report,
		:snowflake_run,
		:snowflake_query_run,
		:error
	);
"""


class Repository:
    def __init__(self):
        self.engine = sa.create_engine(db_url)

    def save_result(self, results):
        with self.engine.begin() as conn:
            conn.execute(sa.text(create_table_sql))
            conn.execute(sa.text(delete_sql), results)
            conn.execute(sa.text(insert_result_sql), results)

    def save_error(self, error):
        with self.engine.begin() as conn:
            conn.execute(sa.text(create_table_sql))
            conn.execute(sa.text(delete_sql), error)
            conn.execute(sa.text(insert_result_sql), error)
