package io.odpf.dagger.core.source.config.adapter;

import com.google.gson.stream.JsonReader;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;

import static org.junit.Assert.assertArrayEquals;

public class StringListAdapterTest {
    @Test
    public void shouldDeserializeJsonArrayToStringArray() throws IOException {
        String parquetFilePathJSONString = "[\"gs://something\", \"gs://anything\"]";
        JsonReader reader = new JsonReader(new StringReader(parquetFilePathJSONString));

        StringListAdapter adapter = new StringListAdapter();

        assertArrayEquals(new String[]{"gs://something", "gs://anything"}, adapter.read(reader));
    }

    @Test
    public void shouldDeserializeEmptyJsonArrayToEmptyStringArray() throws IOException {
        String parquetFilePathJSONString = "[]";
        JsonReader reader = new JsonReader(new StringReader(parquetFilePathJSONString));

        StringListAdapter adapter = new StringListAdapter();

        assertArrayEquals(new String[]{}, adapter.read(reader));
    }

    @Test
    public void shouldDeserializeJsonArrayContainingNullsToStringArray() throws IOException {
        String parquetFilePathJSONString = "[null, \"gs://anything\"]";
        JsonReader reader = new JsonReader(new StringReader(parquetFilePathJSONString));

        StringListAdapter adapter = new StringListAdapter();

        assertArrayEquals(new String[]{"null", "gs://anything"}, adapter.read(reader));
    }

    @Test
    public void shouldDeserializeByTrimmingLeadingAndTrailingWhitespacesFromEachElementIfAny() throws IOException {
        String parquetFilePathJSONString = "[null, \"       gs://something\", \"gs://anything          \"]";

        JsonReader reader = new JsonReader(new StringReader(parquetFilePathJSONString));

        StringListAdapter adapter = new StringListAdapter();

        assertArrayEquals(new String[]{"null", "gs://something", "gs://anything"}, adapter.read(reader));
    }
}
