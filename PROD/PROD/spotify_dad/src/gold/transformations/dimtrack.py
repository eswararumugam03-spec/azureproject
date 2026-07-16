import dlt

@dlt.table
def dimuser_stg():
    df = spark.readStream.table("spotify.silver.dimuser")
    return df

dlt.creat_streaming_table("dimuser")

dlt.create_auto_cdc_flow(
    target = "dimuser",
    source = "dimuser_stg",
    keys = ["user_id"],
    sequence_by = "update_at",
    stored_as_scd_type = 2,
    track_history_except_column_list = None,
    once = None
    )