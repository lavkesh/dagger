package io.odpf.dagger.flink.source.bigquery;

import io.odpf.dagger.common.serde.DaggerDeserializer;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.types.Row;

public class BigquerySourceRecordEmitter implements RecordEmitter<Row, Row, BigQuerySplitState> {
    public BigquerySourceRecordEmitter(DaggerDeserializer<Row> deserializer) {
        // Take this deserializer and Instead of ROW takes the AvoroRecord and deserialse it to row.
    }

    @Override
    public void emitRecord(Row element, SourceOutput<Row> output, BigQuerySplitState splitState) throws Exception {
  //      System.out.println(element);
        // We Can put  deserializer here.
        output.collect(element);
    }
}
