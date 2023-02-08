from download_queries import get_report_last_successful_run, get_query_runs, validate_same_queries, get_query_run_results, get_query_run_by_name
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
  query_runs_by_name = get_query_run_by_name(redshift_query_runs, snowflake_query_runs)

  for index, items in enumerate(query_runs_by_name.items()):
    query_name, query_runs = items 
    print(f'\nComparing query results ({index+1}/{len(query_runs_by_name.keys())}): "{query_name}"')

    redshift_query_run = query_runs['redshift']
    snowflake_query_run = query_runs['snowflake']
    print(f'Redshift SQL:\n{redshift_query_run["raw_source"]}\n')
    print(f'Snowflake SQL:\n{snowflake_query_run["raw_source"]}\n')

    redshift_query_run_results = get_query_run_results(redshift_report, redshift_run, redshift_query_run['token'])
    snowflake_query_run_results = get_query_run_results(snowflake_report, snowflake_run, snowflake_query_run['token'])
    compare_results(redshift_query_run_results, snowflake_query_run_results)


if __name__ == '__main__':
  redshift_report = '8f69df62dbd4'
  snowflake_report = '676bc3781f0b'
  compare_reports(redshift_report, snowflake_report)
