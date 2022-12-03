package io.odpf.dagger.common.serde.avro;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.storage.v1.*;
import io.odpf.dagger.common.serde.DaggerInternalTypeInformation;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.types.Row;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

public class BigqueryType implements Serializable, DaggerInternalTypeInformation {
    private final String bigqueryGoogleProjectId;
    private final String bigqueryTableName;
    private final String bigqueryDatasetName;
    private final String[] bigqueryFieldNames;

    public BigqueryType(String bigqueryGoogleProjectId, String bigqueryTableName, String bigqueryDatasetName, String[] bigqueryFieldNames) {
        this.bigqueryGoogleProjectId = bigqueryGoogleProjectId;
        this.bigqueryTableName = bigqueryTableName;
        this.bigqueryDatasetName = bigqueryDatasetName;
        this.bigqueryFieldNames = bigqueryFieldNames;
    }

    // THis should be optimised a lot. Think about proper abstractions.
    private AvroSchema getAvroSchema() {
        try {
            CredentialsProvider credentialsProvider = FixedCredentialsProvider.create(
                    GoogleCredentials.fromStream(new FileInputStream("/etc/secret/gcp/auth3.json")));
            BigQueryReadSettings readSettings =
                    BigQueryReadSettings.newBuilder()
                            .setCredentialsProvider(credentialsProvider)
                            .build();
            BigQueryReadClient client = BigQueryReadClient.create(readSettings);
            String parent = String.format("projects/%s", bigqueryGoogleProjectId);
            String filter = String.format("DATE(%s) = \"%s\"\n", "event_timestamp", "2023-01-01");
            ReadSession.TableReadOptions tableReadOptions = ReadSession.TableReadOptions
                    .newBuilder()
                    .addAllSelectedFields(Arrays.asList(bigqueryFieldNames))
                    .setRowRestriction(filter).build();

            String srcTable =
                    String.format(
                            "projects/%s/datasets/%s/tables/%s",
                            bigqueryGoogleProjectId, bigqueryDatasetName, bigqueryTableName);
            ReadSession.Builder sessionBuilder =
                    ReadSession.newBuilder()
                            .setTable(srcTable)
                            .setReadOptions(tableReadOptions)
                            .setDataFormat(DataFormat.AVRO);
            CreateReadSessionRequest sessionRequest = CreateReadSessionRequest.newBuilder()
                    .setParent(parent)
                    .setReadSession(sessionBuilder)
                    .setMaxStreamCount(1).build();
            ReadSession readSession = client.createReadSession(sessionRequest);
            client.close();
            return readSession.getAvroSchema();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TypeInformation<Row> getRowType() {
        TypeInformation<Row> objectTypeInformation = AvroSchemaConverter.convertToTypeInfo(getAvroSchema().getSchema());
        return addInternalFields(objectTypeInformation, "rowtime");
    }

}
