import pandas as pd
import sys

def primary_key_check(redshift_only, snowflake_only, primary_key):
  mismatch_df = pd.concat([redshift_only, snowflake_only])

  partial_matches = mismatch_df[mismatch_df['loan_id'].isin(redshift_only['loan_id']) & mismatch_df['loan_id'].isin(snowflake_only['loan_id'])]
  print(len(partial_matches))
  
  partial_match_df = mismatch_df[mismatch_df.duplicated(subset=primary_key, keep=False)]
  print(f'There are {len(partial_match_df)/2} partial matches')


  not_duplicates_df = mismatch_df[~mismatch_df['loan_id'].isin(partial_match_df['loan_id'])]
  redshift_no_partial_only_df = not_duplicates_df[not_duplicates_df['_merge'] == 'left_only']
  snowflake_no_partial_only_df = not_duplicates_df[not_duplicates_df['_merge'] == 'right_only']

  print(f'There are {len(redshift_no_partial_only_df)} records in redshift only')
  print(f'There are {len(snowflake_no_partial_only_df)} records in redshift only')
  print(f'There are {len(partial_match_df)} records in both redshift and snowflake')

  for loan_id in partial_match_df['loan_id'].drop_duplicates():
    df_redshift = partial_match_df[(partial_match_df['loan_id'] == loan_id) & (partial_match_df['_merge'] == 'left_only')].drop(columns="_merge")
    df_snowflake = partial_match_df[(partial_match_df['loan_id'] == loan_id) & (partial_match_df['_merge'] == 'right_only')].drop(columns="_merge") 
    
    assert len(df_redshift) == 1, f"There should be only 1 row for loan_id {loan_id} in redshift and there are {len(df_redshift)}"
    assert len(df_snowflake) == 1, f"There should be only 1 row for loan_id {loan_id} in snowflake and there are {len(df_snowflake)}"

    redshift_row = df_redshift.iloc[0, :].fillna('NaN')
    snowflake_row = df_snowflake.iloc[0, :].fillna('NaN')

    differences = redshift_row[redshift_row != snowflake_row]
    columns_with_differences = differences.index.tolist()
    
    print(f'\nloan_id: {loan_id} has diffrerences in {len(columns_with_differences)} columns: {", ".join(columns_with_differences)}')
    print('\nRedshift: values')
    print(redshift_row[columns_with_differences])
    print('\nSnowflake: values')
    print(snowflake_row[columns_with_differences])
    sys.exit()
  
  output_forlder = 'output/'
  partial_match_df.to_csv(output_forlder + 'partial_match.csv', index=False)
  redshift_no_partial_only_df.to_csv(output_forlder + 'redshift_only.csv', index=False)
  snowflake_no_partial_only_df.to_csv(output_forlder + 'snowflake_only.csv', index=False)
  

def main(redshift_csv_file=None, snowflake_csv_file=None, primary_key=None):
  assert redshift_csv_file is not None, 'Please provide redshift csv file'
  assert snowflake_csv_file is not None, 'Please provide snowflake csv file'

  redshift_df = pd.read_csv(filename_redshift, low_memory=False)
  print(f'Records in redshift: {len(redshift_df)}')
  print(f'Redshift csv has {len(redshift_df.columns)} columns:\n{", ".join(redshift_df.columns)}\n')

  snowflake_df = pd.read_csv(filename_snowflake, low_memory=False)
  print(f'Records in snowflake: {len(snowflake_df)}')
  print(f'Snowflake csv has {len(snowflake_df.columns)} columns:\n{", ".join(snowflake_df.columns)}\n')

  combined_df = redshift_df.merge(snowflake_df, indicator=True, how='outer')
  number_of_records_in_both = len(combined_df[combined_df['_merge'] == 'both'])
  print(f'There are {number_of_records_in_both} exact matches')

  redshift_only = combined_df[combined_df['_merge'] == 'left_only']
  print(f'There are {len(redshift_only)} records in redshift only')

  snowflake_only = combined_df[combined_df['_merge'] == 'right_only']
  print(f'There are {len(snowflake_only)} records in snowflake only')

  if (primary_key is not None):
    primary_key_check(redshift_only, snowflake_only, primary_key)


if __name__ == '__main__':
  filename_redshift = 'modereport/dropoff_dashboard-1_start_-_pq-df780fa3ea7c-2023-02-01-03-49-45.csv'
  filename_snowflake = 'modereport/drop_off_dashboard_snowflake-1_start_-_pq-0660239a18b6-2023-01-31-00-06-30_snowflake.csv'

  if (len(sys.argv) == 3):
    filename_redshift = sys.argv[1]    
    filename_snowflake = sys.argv[2]    

  main(filename_redshift, filename_snowflake)