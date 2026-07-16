import dlt

expectations={
    "rule_1": "expect('track_id').isNotNull()"
}

@dlt.table
@dlt.expectt_all_or_drop(expectations)
def dimtrack_stg():
    df = spark.readStream.table("spotify.silver.dimtrack")
    return df

dlt.creat_streaming_table(
    name ="dimuser"
    expect_all_or_drop = expectations
)

dlt.create_auto_cdc_flow(
    target = "dimtrack",
    source = "dimtrack_stg",
    keys = ["track_id"],
    sequence_by = "update_at",
    stored_as_scd_type = 2,
    track_history_except_column_list = None,
    once = None
    )