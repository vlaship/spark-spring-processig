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
public class PostgresSaver implements Saver{

    @Value("${transaction-job.db.jdbcUrl}")
    private String jdbcUrl;

    @Value("${transaction-job.db.user}")
    private String user;

    @Value("${transaction-job.db.password}")
    private String password;

    public void save(@NotNull Dataset<Row> ds, @NotNull String table) {
        log.info("Saving data to PostgreSQL");

        ds.write()
                .mode(SaveMode.Append)
                .format("jdbc")
                .option("url", jdbcUrl)
                .option("user", user)
                .option("password", password)
                .option("dbtable", table)
                .save();

        log.info("Data saved successfully to PostgreSQL");
    }
}
