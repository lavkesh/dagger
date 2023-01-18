package io.odpf.dagger.flink.source.bigquery;

import com.google.api.services.storage.Storage;
import com.google.cloud.bigquery.storage.v1.AvroSchema;
import com.google.protobuf.ByteString;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.types.Row;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Queue;

public class AvroRecords {
    final RowTypeInfo typeInfo;
    private final GenericDatumReader<GenericRecord> datumReader;
    GenericRecord record;
    Schema schema;
    Queue<Row> queue;

    public AvroRecords(AvroSchema avroSchema) {
        Schema schema = new Schema.Parser().parse(avroSchema.getSchema());
        this.schema = schema;
        TypeInformation<?> objectTypeInformation = AvroSchemaConverter.convertToTypeInfo(avroSchema.getSchema());
        this.typeInfo = (RowTypeInfo) objectTypeInformation;
        this.datumReader = new GenericDatumReader<>(schema);
        this.queue = new ArrayDeque<>();
    }

    public Row next() {
        if(more()) {
            return queue.poll();
        }
        return null;
    }

    public boolean more() {
        return queue.size() > 0;
    }

    public void processRows(ByteString avroRows) throws IOException {
        try (InputStream inputStream = new ByteArrayInputStream(avroRows.toByteArray())) {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
            while (!decoder.isEnd()) {
                record = datumReader.read(record, decoder);
                //copy record and emit.
                Row row = AvroToRowUtils.convertAvroRecordToRow(schema, typeInfo, record, 2);
                row.setField(row.getArity() - 2, true);
                row.setField(row.getArity() - 1, Timestamp.from(Instant.ofEpochSecond(100, 100)));
                queue.add(row);
            }
        }
    }
}
