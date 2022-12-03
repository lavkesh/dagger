package io.odpf.dagger.flink.source.bigquery;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ServerStream;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.common.base.Preconditions;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.types.Row;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

/*
 * Should create a bigquery reader for the split
 */
public class BigQuerySplitReader implements SplitReader<Row, BigQuerySplit> {
    private final Queue<BigQuerySplit> splits = new ArrayDeque<>();
    private final BigQueryReadClient client;
    private Iterator<ReadRowsResponse> iterator;
    private String currentSplitId;
    private AvroRecords avroRecords;

    public BigQuerySplitReader() {
        System.out.println("BigQuerySplitReader created");
        try {
            CredentialsProvider credentialsProvider = FixedCredentialsProvider.create(
                    GoogleCredentials.fromStream(new FileInputStream("/etc/secret/gcp/auth3.json")));
            BigQueryReadSettings readSettings =
                    BigQueryReadSettings.newBuilder()
                            .setCredentialsProvider(credentialsProvider)
                            .build();
            client = BigQueryReadClient.create(readSettings);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RecordsWithSplitIds<Row> fetch() throws IOException {
        checkSplitOrStartNext();
        return iterator.hasNext()
                ? BigQueryRecords.forRecords(avroRecords, currentSplitId, iterator.next())
                : finishSplit();
    }

    private RecordsWithSplitIds<Row> finishSplit() throws IOException {
        if (iterator != null) {
            iterator = null;
        }

        final BigQueryRecords finishRecords = BigQueryRecords.finishedSplit(currentSplitId);
        currentSplitId = null;
        return finishRecords;
    }

    private void checkSplitOrStartNext() throws IOException {
        if (iterator != null && iterator.hasNext()) {
            return;
        }
        final BigQuerySplit nextSplit = splits.poll();
        if (nextSplit == null) {
            throw new IOException("Cannot fetch from another split - no split remaining");
        }

        currentSplitId = nextSplit.splitId();
        ReadSession session = client.createReadSession(nextSplit.getReadSessionRequest());
        Preconditions.checkState(session.getStreamsCount() > 0);
        // Only one stream
        String streamName = session.getStreams(0).getName();
        ReadRowsRequest readRowsRequest =
                ReadRowsRequest.newBuilder().setReadStream(streamName).build();
        ServerStream<ReadRowsResponse> stream = client.readRowsCallable().call(readRowsRequest);
        iterator = stream.iterator();
        if (avroRecords == null) {
            avroRecords = new AvroRecords(session.getAvroSchema());
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<BigQuerySplit> splitsChanges) {
        splits.addAll(splitsChanges.splits());
    }

    @Override
    public void wakeUp() {

    }

    @Override
    public void close() throws Exception {

    }
}
