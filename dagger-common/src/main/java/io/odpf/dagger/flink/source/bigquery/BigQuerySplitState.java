package io.odpf.dagger.flink.source.bigquery;

import java.io.Serializable;

public class BigQuerySplitState implements Serializable {
    private final BigQuerySplit split;

    public BigQuerySplitState(BigQuerySplit split) {
        this.split = split;
    }

    public BigQuerySplit toBigquerySplit() {
        return split;
    }
}
