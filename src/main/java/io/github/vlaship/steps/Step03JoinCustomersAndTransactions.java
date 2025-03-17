package io.github.vlaship.steps;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class Step03JoinCustomersAndTransactions {

    public Dataset<Row> execute(Dataset<Row> customers, Dataset<Row> transactions) {
        log.info("Step03JoinCustomersAndTransactions");

        Dataset<Row> ds = customers
                .join(transactions, "id")
                .select("customer_email", "date_time", "amount");

        log.info("Step03JoinCustomersAndTransactions - done");

        return ds;
    }
}
