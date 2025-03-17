package io.github.vlaship.spark.steps;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class Step04Filtering {

    public Dataset<Row> execute(Dataset<Row> ds) {
        log.info("Step04Filtering");

        Dataset<Row> filtered = ds
                .filter(functions.col("amount").$greater("1000"));

        log.info("Step04Filtering - done");

        return filtered;
    }
}
