package io.github.vlaship.spark.steps;

import io.github.vlaship.spark.utils.CsvSaver;
import io.github.vlaship.spark.utils.PostgresSaver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class Step05Saving {

    private final PostgresSaver postgresSaver;
    private final CsvSaver csvSaver;

    @Value("${transaction-job.output.table}")
    private String tableName;
    @Value("${transaction-job.output.filename}")
    private String filename;

    public void execute(Dataset<Row> ds) {
        log.info("Step05Saving");

        postgresSaver.save(ds, tableName);

        // ! windows problems
        // ! csvSaver.save(ds, filename);

        log.info("Step05Saving - done");
    }

}
