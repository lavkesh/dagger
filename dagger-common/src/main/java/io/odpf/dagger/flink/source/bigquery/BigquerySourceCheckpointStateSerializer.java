package io.odpf.dagger.flink.source.bigquery;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

public class BigquerySourceCheckpointStateSerializer implements SimpleVersionedSerializer<BigquerySourceCheckpointState> {
    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(BigquerySourceCheckpointState obj) throws IOException {
        return new byte[0];
    }

    @Override
    public BigquerySourceCheckpointState deserialize(int version, byte[] serialized) throws IOException {
        return null;
    }
}
