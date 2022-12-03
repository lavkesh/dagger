package io.odpf.dagger.flink.source.bigquery;

import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

public class BigQueryRecords implements RecordsWithSplitIds<Row> {
    private final Set<String> finishedSplits;
    private final AvroRecords avroRecord;
    private AvroRecords recordsForSplitCurrent;
    private String splitId;

    private BigQueryRecords(
            @Nullable AvroRecords avroRecords,
            @Nullable String splitId,
            @Nullable ReadRowsResponse recordsForSplit,
            Set<String> finishedSplits) throws IOException {
        this.splitId = splitId;
        this.finishedSplits = finishedSplits;
        this.avroRecord = avroRecords;
        if (recordsForSplit != null && avroRecords != null) {
            avroRecords.processRows(recordsForSplit.getAvroRows().getSerializedBinaryRows());
        }
    }

    public static BigQueryRecords forRecords(AvroRecords avroRecords, String currentSplitId, ReadRowsResponse rowsResponse) throws IOException {
        return new BigQueryRecords(avroRecords, currentSplitId, rowsResponse, Collections.emptySet());
    }

    public static BigQueryRecords finishedSplit(String currentSplitId) throws IOException {
        return new BigQueryRecords(null, null, null, Collections.singleton(currentSplitId));
    }

    @Nullable
    @Override
    public String nextSplit() {
        final String nextSplit = this.splitId;
        this.splitId = null;
        this.recordsForSplitCurrent = nextSplit != null ? this.avroRecord : null;
        return nextSplit;
    }

    @Nullable
    @Override
    public Row nextRecordFromSplit() {
        if (recordsForSplitCurrent == null) {
            throw new IllegalStateException();
        }
        return recordsForSplitCurrent.next();
    }

    @Override
    public Set<String> finishedSplits() {
        return finishedSplits;
    }
}
