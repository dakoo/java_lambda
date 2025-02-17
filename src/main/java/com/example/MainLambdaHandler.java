package com.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * AWS Lambda Handler for processing Kafka events and writing to DynamoDB.
 */
@Slf4j
public class MainLambdaHandler {

    private final DynamoDbAsyncClient dynamoDbAsyncClient = DynamoDbAsyncClient.create();
    private final String tableName = System.getenv("DYNAMODB_TABLE_NAME");

    // ✅ Counters for execution tracking (reset per invocation)
    private final AtomicInteger totalRecords = new AtomicInteger();
    private final AtomicInteger successfulWrites = new AtomicInteger();
    private final AtomicInteger conditionalCheckFailedCount = new AtomicInteger();
    private final AtomicInteger otherFailedWrites = new AtomicInteger();

    public void handleRequest(KafkaEvent event, Context context) {
        long startTime = System.currentTimeMillis();
        LambdaLogger logger = context.getLogger();

        // ✅ Reset counters for new invocation
        totalRecords.set(0);
        successfulWrites.set(0);
        conditionalCheckFailedCount.set(0);
        otherFailedWrites.set(0);

        AsyncDynamoDbWriter writer = new AsyncDynamoDbWriter(
                dynamoDbAsyncClient, tableName, successfulWrites, conditionalCheckFailedCount, otherFailedWrites
        );

        // ✅ Retrieve parser from environment variable
        String parserName = System.getenv("PARSER_NAME");
        ParserInterface<?> parser = ParserFactory.createParser(parserName);

        logger.log("Processing records...\n");

        // ✅ Process Kafka records asynchronously
        List<CompletableFuture<Void>> parseFutures = event.getRecords().values().stream()
                .flatMap(List::stream)
                .map(record -> CompletableFuture.runAsync(() -> {
                    try {
                        Object model = parser.parseRecord(record);
                        if (model != null) {
                            totalRecords.incrementAndGet();
                            writer.prepareWrite(model);
                        }
                    } catch (Exception e) {
                        log.error("Failed to parse Kafka record: {}", e.getMessage(), e);
                        otherFailedWrites.incrementAndGet();
                    }
                }))
                .collect(Collectors.toList());

        // ✅ Wait for all parsing tasks to complete
        CompletableFuture.allOf(parseFutures.toArray(new CompletableFuture[0])).join();

        // ✅ Execute batch writes and wait for completion
        writer.executeAsyncWrites().join();

        // ✅ Log final execution summary
        long totalTime = System.currentTimeMillis() - startTime;

        logger.log("Lambda Execution Summary: " +
                "{ \"Total Records\": " + totalRecords.get() +
                ", \"Successful Writes\": " + successfulWrites.get() +
                ", \"ConditionalCheckFailed\": " + conditionalCheckFailedCount.get() +
                ", \"Other Failed\": " + otherFailedWrites.get() +
                ", \"Execution Time (ms)\": " + totalTime + " }\n");

        log.info("Lambda execution completed.");
    }
}
