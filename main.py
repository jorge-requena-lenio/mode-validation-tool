from config import config
from download_queries import (
    get_query_runs,
    validate_same_queries,
    get_query_run_results,
    get_query_run_by_name,
    get_report_info,
)
from compare_results import compare_results
from repository import Repository
from datetime import datetime
from logger import ReportLogger, QueryLogger
import os
import boto3
import pandas as pd

bucket_name = config["aws"]["bucket_name"]
aws_access_key_id = config["aws"]["access_key_id"]
aws_secret_access_key = config["aws"]["secret_access_key"]


def compare_reports(redshift_report, snowflake_report):
    repository = Repository()
    print(f"Comparing Redshift:{redshift_report} vs Snowflake:{snowflake_report}")

    redshift_report_info = get_report_info(redshift_report)
    snowflake_report_info = get_report_info(snowflake_report)

    report_log_folder = (
        f'logs/{redshift_report_info["name"]}/{datetime.now().isoformat()}'
    )
    report_logger = ReportLogger(report_log_folder).get_logger()

    redshift_run = redshift_report_info["last_successful_run_token"]
    snowflake_run = snowflake_report_info["last_successful_run_token"]

    report_logger.info(f"Redshift last successful run: {redshift_run}")
    report_logger.info(f"Snowflake last successful run: {redshift_run}")

    redshift_query_runs = get_query_runs(redshift_report, redshift_run)
    snowflake_query_runs = get_query_runs(snowflake_report, snowflake_run)
    validate_same_queries(
        redshift_query_runs, snowflake_query_runs, logger=report_logger
    )
    query_runs_by_name = get_query_run_by_name(
        redshift_query_runs, snowflake_query_runs
    )

    for index, items in enumerate(query_runs_by_name.items()):
        query_name, query_runs = items

        query_logger_file = f"{report_log_folder}/{query_name}.log"
        query_logger = QueryLogger(query_logger_file).get_logger()
        query_logger.info(
            f'Comparing query results ({index+1}/{len(query_runs_by_name.keys())}): "{query_name}"'
        )

        redshift_query_run = query_runs["redshift"]
        snowflake_query_run = query_runs["snowflake"]

        results_metadata = {
            "redshift_report": redshift_report,
            "redshift_run": redshift_run,
            "redshift_query_run": redshift_query_run["token"],
            "snowflake_report": snowflake_report,
            "snowflake_run": snowflake_run,
            "snowflake_query_run": snowflake_query_run["token"],
        }

        try:
            redshift_query_run_results = get_query_run_results(
                redshift_report,
                redshift_run,
                redshift_query_run["token"],
                logger=query_logger,
            )
            snowflake_query_run_results = get_query_run_results(
                snowflake_report,
                snowflake_run,
                snowflake_query_run["token"],
                logger=query_logger,
            )
            results = compare_results(
                redshift_query_run_results,
                snowflake_query_run_results,
                logger=query_logger,
            )

            report_logger.info(f'Query "{query_name}" results')
            for key, value in results.items():
                report_logger.info(f"{key}: {value}")

            report_logger.info(
                f'Exact matches: {results["exact_matches"] / results["total_combined"] * 100:.2f}%'
            )
            report_logger.info(
                f'Only redshift: {results["redshift_only"] / results["total_combined"] * 100:.2f}%'
            )
            report_logger.info(
                f'Only snowflake: {results["snowflake_only"] / results["total_combined"] * 100:.2f}%'
            )

            query_logger.debug(
                f'|-----*****----->>> Redshift-SQL: <<<-----*****-----|\n{redshift_query_run["raw_source"]}'
            )
            query_logger.debug(
                f'|-----*****----->>> Snowflake-SQL: <<<-----*****-----|\n{snowflake_query_run["raw_source"]}'
            )

            repository.save_result({**results_metadata, **results})
        except Exception as e:
            report_logger.error(e)
            repository.save_error({**results_metadata, "error": str(e)})

    push_folder_to_s3(report_log_folder)


def push_folder_to_s3(folder: str):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        with open(file_path, "rb") as data:
            s3.upload_fileobj(data, bucket_name, f"{folder}/{filename}")

if __name__ == "__main__":
    redshift_column = "Redshift links"
    snowflake_column = "Snowflake links"

    input_df = pd.read_csv("input.csv")
    input_df = input_df.dropna(subset=[redshift_column, snowflake_column])

    for index, row in input_df.iterrows():
        print(f"Report {index+1}/{len(input_df.index)}")
        redshift_report = row[redshift_column]
        snowflake_report = row[snowflake_column]

        compare_reports(redshift_report, snowflake_report)
