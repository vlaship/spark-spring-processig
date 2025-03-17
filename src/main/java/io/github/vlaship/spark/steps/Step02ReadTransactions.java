package io.github.vlaship.spark.steps;

import io.github.vlaship.spark.utils.CsvReader;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class Step02ReadTransactions {

    private final CsvReader reader;

    public Dataset<Row> execute() {
        log.info("Step02ReadTransactions");

        Dataset<Row> ds = reader.read("transactions.csv");

        log.info("Step02ReadTransactions - done");

        return ds;
    }
}
