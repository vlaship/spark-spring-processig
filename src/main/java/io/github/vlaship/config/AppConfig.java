package io.github.vlaship.config;

import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

@Configuration
@ComponentScan("com.example")
@PropertySource(value = "classpath:application.yaml", factory = YamlPropertySourceFactory.class)
public class AppConfig {

    @Value("${transaction-job.spark.app-name:CSV Processor}")
    private String appName;

    @Value("${transaction-job.spark.master:local[*]}")
    private String master;

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }

    @Bean(destroyMethod = "close")
    public SparkSession sparkSession() {
        return SparkSession.builder()
                .appName(appName)
                .master(master)
                .getOrCreate();
    }
}