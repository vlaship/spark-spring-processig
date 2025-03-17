package io.github.vlaship.spark.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class CsvReader implements Reader {

    private final SparkSession spark;

    @Value("${transaction-job.input.path}")
    private String inputFolder;

    public @NotNull Dataset<Row> read(@NotNull String filename) {
        log.info("Reading file: {}", filename);

        Dataset<Row> ds = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(inputFolder + "/" + filename);

        log.info("File schema: {}", ds.schema().treeString());

        return ds;
    }

}
