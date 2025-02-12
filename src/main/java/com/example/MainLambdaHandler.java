package com.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Lambda handler for processing incoming Kafka messages, parsing, and writing to DynamoDB.
 * - Tracks execution time
 * - Logs total records processed, failures, successes
 * - Measures latency between steps
 */
@Slf4j
public class MainLambdaHandler {

    private final DynamoDbAsyncClient dynamoDbAsyncClient = DynamoDbAsyncClient.create();
    private final String tableName = System.getenv("DYNAMODB_TABLE_NAME");

    // Metrics tracking
    private final AtomicInteger totalRecords = new AtomicInteger(0);
    private final AtomicInteger invalidRecords = new AtomicInteger(0);
    private final AtomicInteger failedWrites = new AtomicInteger(0);
    private final AtomicInteger successfulWrites = new AtomicInteger(0);

    public void handleRequest(List<Object> kafkaMessages, Context context) {
        long startTime = System.currentTimeMillis();
        LambdaLogger logger = context.getLogger();

        logger.log("Lambda invoked, processing records...");

        // Initialize writer
        AsyncDynamoDbWriter writer = new AsyncDynamoDbWriter(dynamoDbAsyncClient, tableName);

        long parsingStart = System.currentTimeMillis();
        for (Object message : kafkaMessages) {
            if (message == null) {
                invalidRecords.incrementAndGet();
                continue;
            }
            writer.prepareWrite(message);
            totalRecords.incrementAndGet();
        }
        long parsingTime = System.currentTimeMillis() - parsingStart;

        long preparingStart = System.currentTimeMillis();
        writer.executeAsyncWrites();
        long preparingTime = System.currentTimeMillis() - preparingStart;

        long totalTime = System.currentTimeMillis() - startTime;

        // ✅ Log Summary at the END of Lambda Execution
        logger.log("\nLambda Execution Summary:");
        logger.log("\n  - Total Records Processed: " + totalRecords.get());
        logger.log("\n  - Invalid Records (discarded during parsing): " + invalidRecords.get());
        logger.log("\n  - Write Failures (due to ConditionalCheckFailedException): " + failedWrites.get());
        logger.log("\n  - Successful Writes: " + successfulWrites.get());
        logger.log("\n  - Latency:");
        logger.log("\n    - Parsing Time: " + parsingTime + " ms");
        logger.log("\n    - Preparing Writes Time: " + preparingTime + " ms");
        logger.log("\n    - Total Lambda Execution Time: " + totalTime + " ms");

        log.info("Lambda execution completed.");
    }
}
