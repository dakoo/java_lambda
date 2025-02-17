package com.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import com.example.ParserInterface;
import com.example.AsyncDynamoDbWriter;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
public class MainLambdaHandler {

    private final AtomicInteger successfulWrites = new AtomicInteger();
    private final AtomicInteger conditionalCheckFailedCount = new AtomicInteger();
    private final AtomicInteger otherFailedWrites = new AtomicInteger();

    public String handleRequest(KafkaEvent event, Context context) {
        long startTime = System.currentTimeMillis();
        LambdaLogger logger = context.getLogger();

        // 1. Load environment config
        EnvironmentConfig config = EnvironmentConfig.loadFromSystemEnv();
        logger.log("Loaded config: " + config + "\n");

        // 2. Create DDB writer
        DynamoDbAsyncClient ddbClient = DynamoDbAsyncClient.builder().build();
        AsyncDynamoDbWriter writer = new AsyncDynamoDbWriter(
                ddbClient,
                config.getDynamoDbTableName(),
                successfulWrites,
                conditionalCheckFailedCount,
                otherFailedWrites
        );

        // 3. Create parser
        ParserInterface<?> parser = ParserFactory.createParser(config.getParserName());

        logger.log("Starting to process Kafka event...\n");

        // 4. Flatten Kafka records, parse them asynchronously
        List<CompletableFuture<Void>> parseFutures = event.getRecords().values().stream()
                .flatMap(List::stream)
                .map(record -> CompletableFuture.runAsync(() -> {
                    try {
                        Object model = parser.parseRecord(record);
                        if (model != null) {
                            writer.prepareWrite(model);
                        }
                    } catch (Exception e) {
                        log.error("Failed to parse record offset={} partition={}: {}",
                                record.getOffset(), record.getPartition(), e.getMessage(), e);
                        otherFailedWrites.incrementAndGet();
                    }
                }))
                .collect(Collectors.toList());

        // 5. Wait for all parsing tasks to finish
        CompletableFuture.allOf(parseFutures.toArray(new CompletableFuture[0])).join();

        // 6. Wait for all batch writes to finish
        writer.executeAsyncWrites().join();

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;

        // 7. Log final summary
        String resultMsg = String.format("Processed event in %d ms. " +
                        "Success=%d, CondCheckFailed=%d, OtherFailed=%d",
                totalTime, successfulWrites.get(),
                conditionalCheckFailedCount.get(), otherFailedWrites.get()
        );
        logger.log("Lambda Execution Summary: " + resultMsg + "\n");
        log.info("Lambda execution completed. " + resultMsg);

        return resultMsg;
    }
}
