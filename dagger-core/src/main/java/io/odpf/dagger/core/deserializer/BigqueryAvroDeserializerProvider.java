package io.odpf.dagger.core.deserializer;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.serde.avro.BigqueryType;
import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.common.serde.avro.BigqueryAvroDeserializer;
import io.odpf.dagger.core.source.config.StreamConfig;
import io.odpf.dagger.core.source.config.models.SourceDetails;
import io.odpf.dagger.core.source.config.models.SourceName;
import lombok.AllArgsConstructor;
import org.apache.flink.types.Row;

import static io.odpf.dagger.core.utils.Constants.FLINK_ROWTIME_ATTRIBUTE_NAME_DEFAULT;
import static io.odpf.dagger.core.utils.Constants.FLINK_ROWTIME_ATTRIBUTE_NAME_KEY;

@AllArgsConstructor
public class BigqueryAvroDeserializerProvider implements DaggerDeserializerProvider<Row> {

    private static final SourceName COMPATIBLE_SOURCE = SourceName.BIGQUERY_SOURCE;
    protected StreamConfig streamConfig;
    protected Configuration configuration;

    @Override
    public DaggerDeserializer<Row> getDaggerDeserializer() {
        BigqueryType schema = new BigqueryType(
                streamConfig.getBigqueryGoogleProjectId(),
                streamConfig.getBigqueryTableName(),
                streamConfig.getBigqueryDatasetName(),
                streamConfig.getBigqueryTableFields());
        String rowTimeAttributeName = configuration.getString(FLINK_ROWTIME_ATTRIBUTE_NAME_KEY, FLINK_ROWTIME_ATTRIBUTE_NAME_DEFAULT);
        return new BigqueryAvroDeserializer(rowTimeAttributeName, schema);
    }

    @Override
    public boolean canProvide() {
        SourceDetails[] sourceDetailsList = streamConfig.getSourceDetails();
        for (SourceDetails sourceDetails : sourceDetailsList) {
            SourceName sourceName = sourceDetails.getSourceName();
            if (sourceName.equals(COMPATIBLE_SOURCE)) {
                return true;
            }
        }
        return false;
    }
}

