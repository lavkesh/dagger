package io.odpf.dagger.flink.source.bigquery;

import io.odpf.dagger.common.serde.DaggerDeserializer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;

import java.util.Properties;


public class BigQuerySource implements Source<Row, BigQuerySplit, BigquerySourceCheckpointState>, ResultTypeQueryable<Row> {

    private final Configuration configuration;
    private final DaggerDeserializer<Row> deserializer;

    public BigQuerySource(Configuration configuration, DaggerDeserializer<Row> deserializer) {
        this.configuration = configuration;
        this.deserializer = deserializer;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<Row, BigQuerySplit> createReader(SourceReaderContext readerContext) throws Exception {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<Row>> elementsQueue = new FutureCompletingBlockingQueue<>();
        SplitFetcherManager<Row, BigQuerySplit> splitFetcherManager =
                new SingleThreadFetcherManager<>(elementsQueue, BigQuerySplitReader::new);
        BigquerySourceRecordEmitter emitter = new BigquerySourceRecordEmitter(deserializer);
        return new BigQuerySourceReader(elementsQueue, splitFetcherManager, emitter, configuration, readerContext);
    }

    @Override
    public SplitEnumerator<BigQuerySplit, BigquerySourceCheckpointState> createEnumerator
            (SplitEnumeratorContext<BigQuerySplit> context) throws Exception {
        Properties properties = new Properties();
        properties.putAll(configuration.toMap());
        System.out.println("createEnumerator called");
        return new BigQuerySplitEnumerator(properties, context);
    }

    @Override
    public SplitEnumerator<BigQuerySplit, BigquerySourceCheckpointState> restoreEnumerator
            (SplitEnumeratorContext<BigQuerySplit> enumContext, BigquerySourceCheckpointState checkpoint) throws
            Exception {
        System.out.println("restoreEnumerator called");
        return null;
    }

    @Override
    public SimpleVersionedSerializer<BigQuerySplit> getSplitSerializer() {
        return new BigquerySplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<BigquerySourceCheckpointState> getEnumeratorCheckpointSerializer() {
        return new BigquerySourceCheckpointStateSerializer();
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return deserializer.getProducedType();
    }
}
