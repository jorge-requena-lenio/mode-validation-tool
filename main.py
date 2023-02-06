from download_queries import get_report_last_successful_run, get_query_runs, validate_same_queries, get_query_run_token_by_name, get_query_run_results
from compare_results import compare_results

def compare_reports(redshift_report, snowflake_report):
  print(f"Comparing Redshift:{redshift_report} vs Snowflake:{snowflake_report}")

  redshift_run = get_report_last_successful_run(redshift_report)
  snowflake_run = get_report_last_successful_run(snowflake_report)

  print(f'Redshift last successful run: {redshift_run}')
  print(f'Snowflake last successful run: {redshift_run}')

  redshift_query_runs = get_query_runs(redshift_report, redshift_run)
  snowflake_query_runs = get_query_runs(snowflake_report, snowflake_run)
  validate_same_queries(redshift_query_runs, snowflake_query_runs)
  query_runs_by_name = get_query_run_token_by_name(redshift_query_runs, snowflake_query_runs)

  for query_name, query_run_tokens in query_runs_by_name.items():
    print(f'\nComparing query "{query_name}" results')
    redshift_query_run_token = query_run_tokens['redshift']
    redshift_query_run_results = get_query_run_results(redshift_report, redshift_run, redshift_query_run_token)
    snowflake_query_run_token = query_run_tokens['snowflake']
    snowflake_query_run_results = get_query_run_results(snowflake_report, snowflake_run, snowflake_query_run_token)
    compare_results(redshift_query_run_results, snowflake_query_run_results)


if __name__ == '__main__':
  redshift_report = '8f69df62dbd4'
  snowflake_report = '676bc3781f0b'
  compare_reports(redshift_report, snowflake_report)
