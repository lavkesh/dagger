package io.odpf.dagger.flink.source.bigquery;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;

public class BigquerySplitSerializer implements SimpleVersionedSerializer<BigQuerySplit> {
    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(BigQuerySplit obj) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            ObjectOutputStream out;
            out = new ObjectOutputStream(bos);
            out.writeObject(obj);
            out.flush();
            System.out.println("SPLIT SERIALIZED");
            return bos.toByteArray();
        }
    }

    @Override
    public BigQuerySplit deserialize(int version, byte[] serialized) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
        try (ObjectInput in = new ObjectInputStream(bis)) {
            System.out.println("SPLIT DESERIALIZED");
            return (BigQuerySplit) in.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }
}
