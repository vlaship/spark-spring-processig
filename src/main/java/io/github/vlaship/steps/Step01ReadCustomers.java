package io.github.vlaship.steps;

import io.github.vlaship.utils.CsvReader;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class Step01ReadCustomers {

    private final CsvReader reader;

    public Dataset<Row> execute() {
        log.info("Step01ReadCustomers");

        Dataset<Row> ds = reader.read("customers.csv");

        log.info("Step01ReadCustomers - done");

        return ds;
    }
}
