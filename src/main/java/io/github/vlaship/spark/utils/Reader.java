package io.github.vlaship.spark.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.lang.NonNull;

public interface Reader {
    @NonNull
    Dataset<Row> read(@NonNull String sourceName);
}
