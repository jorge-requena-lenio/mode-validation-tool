import requests
from requests.auth import HTTPBasicAuth
import json
import pandas as pd
from base64 import b64encode
from compare_results import compare_results

host = 'https://app.mode.com'
account = '***'

username = '***'
password = '***'


def get_report_last_successful_run(report: str) -> str:
  url = f'{host}/api/{account}/reports/{report}'
  response = requests.get(url, auth=HTTPBasicAuth(username, password))
  data = response.json()
  return data['last_successful_run_token']


def get_query_runs(report: str, run: str) -> list:
  url = f'{host}/api/{account}/reports/{report}/runs/{run}/query_runs'
  response = requests.get(url, auth=HTTPBasicAuth(username, password))
  data = response.json()
  return data['_embedded']['query_runs']


def validate_same_queries(redshift_query_runs: list, snowflake_query_runs: list) -> None:
  redshift_queries = [query_run['query_name'] for query_run in redshift_query_runs]
  redshift_queries.sort()
  snowflake_queries = [query_run['query_name'] for query_run in snowflake_query_runs]
  snowflake_queries.sort()

  print(f'\nRedshift queries ({len(redshift_queries)})')
  print('\n'.join(redshift_queries))
  print(f'\nSnowflake queries ({len(snowflake_queries)})')
  print('\n'.join(snowflake_queries))

  for snowflake_query in snowflake_queries:
    if snowflake_query not in redshift_queries:
      raise Exception(f'Query "{snowflake_query}" not found in Redshift')
  

def get_query_run_by_name(redshift_query_runs: list, snowflake_query_runs: list) -> dict:
  query_runs_by_name = {}
  for snowflake_query_run in snowflake_query_runs:
    name = snowflake_query_run['query_name']
    redshift_query_run = next((query_run for query_run in redshift_query_runs if query_run['query_name'] == name))
    query_runs_by_name[name] = dict(redshift=redshift_query_run, snowflake=snowflake_query_run) 
  return query_runs_by_name

def get_query_run_results(report: str, run: str, query_run: str) -> pd.DataFrame:
  url = f'{host}/api/{account}/reports/{report}/runs/{run}/query_runs/{query_run}/results/content.csv'
  basic_auth = b64encode(f'{username}:{password}'.encode('utf-8'))
  return pd.read_csv(
    url,
    low_memory=False,
    storage_options={'Authorization': b'Basic %s' % basic_auth})
