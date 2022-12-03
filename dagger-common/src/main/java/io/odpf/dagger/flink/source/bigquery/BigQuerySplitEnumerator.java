package io.odpf.dagger.flink.source.bigquery;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.joda.time.Instant;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.PriorityBlockingQueue;

public class BigQuerySplitEnumerator implements SplitEnumerator<BigQuerySplit, BigquerySourceCheckpointState> {
    private static final int INITIAL_DEFAULT_CAPACITY = 11;
    private final SplitEnumeratorContext<BigQuerySplit> context;
    private final Properties configuration;
    private final PriorityBlockingQueue<BigQuerySplit> sortedQueue;

    public BigQuerySplitEnumerator(Properties configuration, SplitEnumeratorContext<BigQuerySplit> context) {
        this.context = context;
        this.configuration = configuration;
        this.sortedQueue = new PriorityBlockingQueue<>(INITIAL_DEFAULT_CAPACITY, Comparator.naturalOrder());
    }

    @Override
    public void start() {
        String startTime = configuration.getProperty("SOURCE_BIGQUERY_START_TIME");
        String endTime = configuration.getProperty("SOURCE_BIGQUERY_END_TIME");
        //hour buckets
        Instant sTime = Instant.parse(startTime);
        Instant eTime = Instant.parse(endTime);
        for (Instant timeWindow = sTime; timeWindow.isBefore(eTime); timeWindow = timeWindow.plus(3600000)) {
            BigQuerySplit split = new BigQuerySplit(configuration, timeWindow);
            sortedQueue.add(split);
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        System.out.println("handleSplitRequest called");
        BigQuerySplit nextSplit = sortedQueue.poll();
        if (nextSplit != null) {
            context.assignSplit(nextSplit, subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<BigQuerySplit> splits, int subtaskId) {
        sortedQueue.addAll(splits);

    }

    @Override
    public void addReader(int subtaskId) {
    }

    @Override
    public BigquerySourceCheckpointState snapshotState(long checkpointId) throws Exception {
        return new BigquerySourceCheckpointState();
    }

    @Override
    public void close() throws IOException {

    }
}
