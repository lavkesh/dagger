package io.odpf.dagger.core.source.bigquery;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.core.source.DaggerSource;
import io.odpf.dagger.core.source.config.StreamConfig;
import io.odpf.dagger.core.source.config.models.SourceDetails;
import io.odpf.dagger.core.source.config.models.SourceName;
import io.odpf.dagger.core.source.config.models.SourceType;
import io.odpf.dagger.flink.source.bigquery.BigQuerySource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

public class BigqueryDaggerSource implements DaggerSource<Row> {
    private final StreamConfig streamConfig;
    private final Configuration configuration;
    private final DaggerDeserializer<Row> deserializer;

    public BigqueryDaggerSource(StreamConfig streamConfig, Configuration configuration, DaggerDeserializer<Row> deserializer) {
        this.streamConfig = streamConfig;
        this.configuration = configuration;
        this.deserializer = deserializer;
    }

    @Override
    public DataStream<Row> register(StreamExecutionEnvironment executionEnvironment, WatermarkStrategy<Row> watermarkStrategy) {
        BigQuerySource source = new BigQuerySource(configuration.getParam().getConfiguration(), deserializer);
        return executionEnvironment.fromSource(source, watermarkStrategy, streamConfig.getSchemaTable());
    }


    @Override
    public boolean canBuild() {
        SourceDetails[] sourceDetailsArray = streamConfig.getSourceDetails();
        if (sourceDetailsArray.length != 1) {
            return false;
        } else {
            SourceName sourceName = sourceDetailsArray[0].getSourceName();
            SourceType sourceType = sourceDetailsArray[0].getSourceType();
            return sourceName.equals(SourceName.BIGQUERY_SOURCE) && sourceType.equals(SourceType.BOUNDED);
        }
    }
}
