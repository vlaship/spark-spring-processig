package io.github.vlaship.steps;

import io.github.vlaship.utils.PostgresSaver;
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

    private final PostgresSaver saver;

    @Value("${transaction-job.output.table}")
    private String tableName;

    public void execute(Dataset<Row> ds) {
        log.info("Step05Saving");

        saver.save(ds, tableName);

        log.info("Step05Saving - done");
    }

}
