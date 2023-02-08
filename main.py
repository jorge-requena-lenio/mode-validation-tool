from download_queries import get_query_runs, validate_same_queries, get_query_run_results, get_query_run_by_name, get_report_info
from compare_results import compare_results
from datetime import datetime

from logger import ReportLogger, QueryLogger

def compare_reports(redshift_report, snowflake_report):
  print(f"Comparing Redshift:{redshift_report} vs Snowflake:{snowflake_report}")
  
  redshift_report_info = get_report_info(redshift_report)
  snowflake_report_info = get_report_info(snowflake_report)

  report_log_folder = f'logs/{redshift_report_info["name"]}/{datetime.now().isoformat()}'
  report_logger = ReportLogger(report_log_folder).get_logger()

  redshift_run = redshift_report_info['last_successful_run_token']
  snowflake_run = snowflake_report_info['last_successful_run_token']

  report_logger.info(f'Redshift last successful run: {redshift_run}')
  report_logger.info(f'Snowflake last successful run: {redshift_run}')

  redshift_query_runs = get_query_runs(redshift_report, redshift_run)
  snowflake_query_runs = get_query_runs(snowflake_report, snowflake_run)
  validate_same_queries(redshift_query_runs, snowflake_query_runs, logger=report_logger)
  query_runs_by_name = get_query_run_by_name(redshift_query_runs, snowflake_query_runs)

  for index, items in enumerate(query_runs_by_name.items()):
    query_name, query_runs = items
    
    query_logger_file = f'{report_log_folder}/{query_name}.log'
    query_logger = QueryLogger(query_logger_file).get_logger()
    query_logger.info(f'Comparing query results ({index+1}/{len(query_runs_by_name.keys())}): "{query_name}"')

    redshift_query_run = query_runs['redshift']
    snowflake_query_run = query_runs['snowflake']

    redshift_query_run_results = get_query_run_results(redshift_report, redshift_run, redshift_query_run['token'], logger=query_logger)
    snowflake_query_run_results = get_query_run_results(snowflake_report, snowflake_run, snowflake_query_run['token'], logger=query_logger)
    compare_results(redshift_query_run_results, snowflake_query_run_results, logger=query_logger)

    query_logger.debug(f'|-----*****----->>> Redshift-SQL: <<<-----*****-----|\n{redshift_query_run["raw_source"]}')
    query_logger.debug(f'|-----*****----->>> Snowflake-SQL: <<<-----*****-----|\n{snowflake_query_run["raw_source"]}')



if __name__ == '__main__':
  redshift_report = '8f69df62dbd4'
  snowflake_report = '676bc3781f0b'
  compare_reports(redshift_report, snowflake_report)
