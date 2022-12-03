package io.odpf.dagger.core.source.config.models;

import com.google.gson.annotations.SerializedName;

import static io.odpf.dagger.core.utils.Constants.*;

public enum SourceName {
    @SerializedName(STREAM_SOURCE_DETAILS_SOURCE_NAME_KAFKA)
    KAFKA_SOURCE,
    @SerializedName(STREAM_SOURCE_DETAILS_SOURCE_NAME_PARQUET)
    PARQUET_SOURCE,
    @SerializedName(STREAM_SOURCE_DETAILS_SOURCE_NAME_KAFKA_CONSUMER)
    KAFKA_CONSUMER,
    @SerializedName(STREAM_SOURCE_DETAILS_SOURCE_NAME_BIGQUERY_SOURCE)
    BIGQUERY_SOURCE
}
