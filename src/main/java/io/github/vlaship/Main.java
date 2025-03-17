package io.github.vlaship;

import io.github.vlaship.config.AppConfig;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Main {
    public static void main(String[] args) {
        log.info("Starting the CSV processing application");

        // Initialize Spring context
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(AppConfig.class)) {
            // Get the processor bean and run the processing pipeline
            Processor processor = context.getBean(Processor.class);
            processor.process();

            log.info("CSV processing completed successfully");
        } catch (Exception e) {
            log.error("Error processing CSV files", e);
        }
        // Close Spring context
    }
}