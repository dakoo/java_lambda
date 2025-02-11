package com.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import java.util.List;

@Slf4j
public class MainLambdaHandler implements RequestHandler<KafkaEvent, String> {

    @Override
    public String handleRequest(KafkaEvent event, Context context) {
        if (event == null) {
            log.warn("Received null event.");
            return "No event data";
        }

        // 1) Load config from environment
        EnvironmentConfig config = EnvironmentConfig.loadFromSystemEnv();
        log.info("Loaded environment: {}", config);

        // 2) Flatten the Kafka event
        List<KafkaEvent.KafkaEventRecord> records = KafkaEventFlattener.flatten(event);
        log.info("Flattened {} record(s).", records.size());

        // 3) Create the parser via factory
        ParserInterface<?> parser = ParserFactory.createParser(config.getParserName());
        log.info("Using parser: {}", config.getParserName());

        // 4) Create the DynamoDB async client + writer
        DynamoDbAsyncClient ddbAsyncClient = DynamoDbAsyncClient.builder().build();
        AsyncDynamoDbWriter writer = new AsyncDynamoDbWriter(ddbAsyncClient, config.getDynamoDbTableName());

        int parsedCount = 0;

        // 5) For each record, parse + prepare a write (unless DRY_RUN)
        for (KafkaEvent.KafkaEventRecord r : records) {
            try {
                // parse
                Object modelObj = parser.parseRecord(r);
                if (modelObj != null) {
                    log.debug("Parsed model: {}", modelObj);
                    if (!config.isDryRun()) {
                        writer.prepareWrite(modelObj);
                    }
                    parsedCount++;
                }
            } catch (Exception e) {
                log.error("Error parsing record offset={} partition={}: {}",
                        r.getOffset(), r.getPartition(), e.getMessage(), e);
            }
        }

        // 6) If not DRY_RUN, do asynchronous writes with concurrency checks
        if (!config.isDryRun()) {
            writer.executeAsyncWrites();
        } else {
            log.info("DRY_RUN=true, skipping DynamoDB writes.");
        }

        String result = "Processed " + parsedCount + " record(s). (DRY_RUN=" + config.isDryRun() + ")";
        log.info(result);
        return result;
    }
}
