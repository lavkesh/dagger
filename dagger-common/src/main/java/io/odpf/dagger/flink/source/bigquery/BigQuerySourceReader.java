package io.odpf.dagger.flink.source.bigquery;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.types.Row;

import java.util.Map;

public class BigQuerySourceReader extends SourceReaderBase<Row, Row, BigQuerySplit, BigQuerySplitState> {

    public BigQuerySourceReader(FutureCompletingBlockingQueue<RecordsWithSplitIds<Row>> elementsQueue, SplitFetcherManager<Row, BigQuerySplit> splitFetcherManager, RecordEmitter<Row, Row, BigQuerySplitState> recordEmitter, Configuration config, SourceReaderContext context) {
        super(elementsQueue, splitFetcherManager, recordEmitter, config, context);
    }

    @Override
    protected void onSplitFinished(Map<String, BigQuerySplitState> finishedSplitIds) {
        context.sendSplitRequest();
    }

    @Override
    public void start() {
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }

    }

    @Override
    protected BigQuerySplitState initializedState(BigQuerySplit split) {
        return new BigQuerySplitState(split);
    }

    @Override
    protected BigQuerySplit toSplitType(String splitId, BigQuerySplitState splitState) {
        return splitState.toBigquerySplit();
    }
}
