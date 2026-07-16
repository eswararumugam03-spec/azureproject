import dlt

@dlt.table
def dimdate_stg():
    df = spark.readStream.table("spotify.silver.dimdate")
    return df

dlt.creat_streaming_table("dimdate")

dlt.create_auto_cdc_flow(
    target = "dimdate",
    source = "dimdate_stg",
    keys = ["date_key"],
    sequence_by = "date",
    stored_as_scd_type = 2,
    track_history_except_column_list = None,
    once = None
    )