package io.github.vlaship.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.lang.NonNull;

public interface Saver {
    void save(@NonNull Dataset<Row> ds, @NonNull String targetName);
}
