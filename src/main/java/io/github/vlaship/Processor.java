package io.github.vlaship;

import io.github.vlaship.steps.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class Processor {

    private final Step01ReadCustomers step01;
    private final Step02ReadTransactions step02;
    private final Step03JoinCustomersAndTransactions step03;
    private final Step04Filtering step04;
    private final Step05Saving step05;

    public void process() {
        log.info("Starting data processing pipeline");

        // Step 01: Read Customers
        Dataset<Row> customers = step01.execute();

        // Step 02: Read Transactions
        Dataset<Row> transactions = step02.execute();

        // Step 03: Join datasets
        Dataset<Row> joinedDs = step03.execute(customers, transactions);

        // Step 04: Filtering data
        Dataset<Row> filtered = step04.execute(joinedDs);

        // Step 05: Saving results
        step05.execute(filtered);

        log.info("Data processing pipeline completed");
    }
}