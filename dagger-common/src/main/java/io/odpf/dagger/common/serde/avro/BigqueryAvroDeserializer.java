package io.odpf.dagger.common.serde.avro;

import io.odpf.dagger.common.serde.DaggerDeserializer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

public class BigqueryAvroDeserializer implements DaggerDeserializer<Row> {
    private final TypeInformation<Row> rowType;
    private final String rowTimeAttributeName;

    public BigqueryAvroDeserializer(String rowTimeAttributeName, BigqueryType schema) {
        this.rowTimeAttributeName = rowTimeAttributeName;
        this.rowType = schema.getRowType();
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return rowType;
    }
}
