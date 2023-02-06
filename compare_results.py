import pandas as pd


format_number = lambda x: f"{x:,.0f}"


def compare_results(redshift_df, snowflake_df, logger=None):
    logger.info(
        f'Redshift csv has {len(redshift_df.columns)} columns:\n{", ".join(redshift_df.columns)}'
    )
    redshift_total = len(redshift_df)
    redshift_duplicates = int(redshift_df.duplicated().sum())
    logger.info(f"Redshift csv has {format_number(redshift_total)} records")
    logger.info(f"Redshift csv has {format_number(redshift_duplicates)} duplicates")

    redshift_df = redshift_df.drop_duplicates()
    redshift_uniques = len(redshift_df)
    logger.info(f"Redshift csv has {format_number(redshift_uniques)} unique records")

    logger.info(
        f'Snowflake csv has {len(snowflake_df.columns)} columns:\n{", ".join(snowflake_df.columns)}'
    )
    snowflake_total = len(snowflake_df)
    snowflake_duplicates = int(snowflake_df.duplicated().sum())
    logger.info(f"Snowflake csv has {format_number(snowflake_total)} records")
    logger.info(f"Snowflake csv has {format_number(snowflake_duplicates)} duplicates")

    snowflake_df = snowflake_df.drop_duplicates()
    snowflake_uniques = len(snowflake_df)
    logger.info(f"Redshift csv has {format_number(snowflake_uniques)} unique records")

    combined_records = redshift_df.merge(snowflake_df, indicator=True, how="outer")

    total_combined = len(combined_records)
    exact_matches = len(combined_records[combined_records["_merge"] == "both"])
    redshift_only = len(combined_records[combined_records["_merge"] == "left_only"])
    snowflake_only = len(combined_records[combined_records["_merge"] == "right_only"])

    logger.info(
        f"There are {format_number(total_combined)} combined records (no duplicates)"
    )
    logger.info(
        f"There are {format_number(exact_matches)} exact matches ({exact_matches / total_combined * 100:.2f}%)"
    )
    logger.info(
        f"There are {format_number(redshift_only)} records in redshift only ({redshift_only / total_combined * 100:.2f}%)"
    )
    logger.info(
        f"There are {format_number(snowflake_only)} records in snowflake only ({snowflake_only / total_combined * 100:.2f}%)"
    )

    return dict(
        redshift_total=redshift_total,
        redshift_duplicates=redshift_duplicates,
        redshift_uniques=redshift_uniques,
        snowflake_total=snowflake_total,
        snowflake_duplicates=snowflake_duplicates,
        snowflake_uniques=snowflake_uniques,
        total_combined=total_combined,
        exact_matches=exact_matches,
        redshift_only=redshift_only,
        snowflake_only=snowflake_only,
    )


if __name__ == "__main__":
    filename_redshift = "modereport/redshift-8f69df62dbd4-c9a3a2ba3043-71c9487dce99.csv"
    filename_snowflake = (
        "modereport/snowflake-676bc3781f0b-6ceb4e12fe5f-e89c4a47462c.csv"
    )

    redshift_df = pd.read_csv(filename_redshift, low_memory=False)
    snowflake_df = pd.read_csv(filename_snowflake, low_memory=False)

    import logging

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

    compare_results(redshift_df, snowflake_df, logger)
