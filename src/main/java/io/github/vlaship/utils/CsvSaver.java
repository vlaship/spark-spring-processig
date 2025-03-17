package io.github.vlaship.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class CsvSaver implements Saver{

    @Value("${transaction-job.output.path}")
    private String outputPath;

    public void save(@NotNull Dataset<Row> ds, @NotNull String filename) {
        log.info("Saving data to CSV");

        ds.write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv(outputPath + "/" + filename);

        log.info("Data saved successfully");
    }
}
