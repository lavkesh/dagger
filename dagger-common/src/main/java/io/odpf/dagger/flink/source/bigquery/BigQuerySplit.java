package io.odpf.dagger.flink.source.bigquery;

import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import org.apache.flink.api.connector.source.SourceSplit;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class BigQuerySplit implements SourceSplit, Comparable<BigQuerySplit>, Serializable {

    private final String startTimeStamp;
    private final String date;
    private final String endTimeStamp;
    private final String dateColumn;
    private final List<String> fields = new ArrayList<>();
    private final String projectId;
    private final String dataset;
    private final String table;

    public BigQuerySplit(Properties configuration, org.joda.time.Instant start) {
        DateTime dt = start.toDateTime(DateTimeZone.UTC);
        this.date = dt.toString(DateTimeFormat.forPattern("yyyy-MM-dd"));
        int hour = dt.getHourOfDay();
        String hourString = "";
        if (hour < 10) {
            hourString = "0";
        }
        hourString += hour;
        this.startTimeStamp = date + "T" + hourString + ":00:00.000000";
        this.endTimeStamp = date + "T" + hourString + ":59:59.999999";

        this.dateColumn = configuration.getProperty("SOURCE_BIGQUERY_DATE_COLUMN", "event_timestamp");
        this.projectId = configuration.getProperty("SOURCE_BIGQUERY_GOOGLE_PROJECT_ID");
        this.dataset = configuration.getProperty("SOURCE_BIGQUERY_DATASET_NAME");
        this.table = configuration.getProperty("SOURCE_BIGQUERY_TABLE_NAME");
        Arrays.stream(configuration.getProperty("SOURCE_BIGQUERY_TABLE_FIELDS", "").split(",")).filter(s -> !s.isEmpty()).forEach(fields::add);
        System.out.println("***CREATED*** " + startTimeStamp + " to " + endTimeStamp);
    }

    public CreateReadSessionRequest getReadSessionRequest() {
        String parent = String.format("projects/%s", projectId);
        String srcTable =
                String.format(
                        "projects/%s/datasets/%s/tables/%s",
                        projectId, dataset, table);
        String filter = String.format("DATE(%s) = \"%s\"\n" +
                        "  AND DATETIME(%s) >= \"%s\"\n" +
                        "  AND DATETIME(%s) <= \"%s\"",
                dateColumn,
                date,
                dateColumn,
                startTimeStamp,
                dateColumn,
                endTimeStamp);
        ReadSession.TableReadOptions tableReadOptions = ReadSession.TableReadOptions
                .newBuilder()
                .addAllSelectedFields(fields)
                .setRowRestriction(filter).build();
        ReadSession.Builder sessionBuilder =
                ReadSession.newBuilder()
                        .setTable(srcTable)
                        .setDataFormat(DataFormat.AVRO)
                        .setReadOptions(tableReadOptions);

        return CreateReadSessionRequest.newBuilder()
                .setParent(parent)
                .setReadSession(sessionBuilder)
                .setMaxStreamCount(1).build();
    }

    @Override
    public String splitId() {
        return startTimeStamp + " to " + endTimeStamp;
    }


    @Override
    public int compareTo(BigQuerySplit split) {
        org.joda.time.Instant thisStart = org.joda.time.Instant.parse(startTimeStamp);
        org.joda.time.Instant otherStart = org.joda.time.Instant.parse(split.startTimeStamp);
        return thisStart.compareTo(otherStart);
    }
}
