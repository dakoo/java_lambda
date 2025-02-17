package com.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Handles batch writes to DynamoDB asynchronously.
 */
@Slf4j
@RequiredArgsConstructor
public class AsyncDynamoDbWriter {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final DynamoDbAsyncClient dynamoDbAsyncClient;
    private final String tableName;

    private static final int MAX_BATCH_SIZE = 25; // ✅ DynamoDB batch limit

    // ✅ Counters for execution tracking
    private final AtomicInteger successfulWrites;
    private final AtomicInteger conditionalCheckFailedCount;
    private final AtomicInteger otherFailedWrites;

    private final List<Map<String, AttributeValue>> pendingItems = new ArrayList<>();
    private final List<CompletableFuture<BatchWriteItemResponse>> pendingFutures = new ArrayList<>();

    /**
     * Prepare an item for batch writing.
     */
    public void prepareWrite(Object modelObj) {
        try {
            Map<String, AttributeValue> item = convertToDynamoDBItem(modelObj);
            if (item != null) {
                pendingItems.add(item);
            }
        } catch (Exception e) {
            log.error("Failed to convert item: {}", e.getMessage(), e);
            otherFailedWrites.incrementAndGet();
        }

        // ✅ If batch reaches limit, flush it asynchronously
        if (pendingItems.size() >= MAX_BATCH_SIZE) {
            flushBatch();
        }
    }

    /**
     * Ensures all pending writes are completed before Lambda exits.
     */
    public CompletableFuture<Void> executeAsyncWrites() {
        if (!pendingItems.isEmpty()) {
            flushBatch(); // ✅ Flush remaining items before finishing
        }

        // ✅ Wait for all batch writes to complete before Lambda exits
        return CompletableFuture.allOf(pendingFutures.toArray(new CompletableFuture[0]));
    }

    /**
     * Converts model object to DynamoDB format.
     */
    private Map<String, AttributeValue> convertToDynamoDBItem(Object modelObj) throws IllegalAccessException {
        Map<String, AttributeValue> item = new HashMap<>();
        Class<?> clazz = modelObj.getClass();

        for (var field : clazz.getDeclaredFields()) {
            field.setAccessible(true);
            Object value = field.get(modelObj);
            if (value == null) continue;

            item.put(field.getName(), AttributeValue.builder().s(serializeToJson(value)).build());
        }

        return item;
    }

    /**
     * Asynchronously sends a batch request to DynamoDB.
     */
    private void flushBatch() {
        if (pendingItems.isEmpty()) return;

        List<WriteRequest> writeRequests = pendingItems.stream()
                .map(item -> WriteRequest.builder()
                        .putRequest(PutRequest.builder().item(item).build())
                        .build())
                .collect(Collectors.toList());

        BatchWriteItemRequest batchRequest = BatchWriteItemRequest.builder()
                .requestItems(Collections.singletonMap(tableName, writeRequests))
                .build();

        CompletableFuture<BatchWriteItemResponse> future = dynamoDbAsyncClient.batchWriteItem(batchRequest)
                .thenApply(response -> {
                    successfulWrites.addAndGet(pendingItems.size());
                    log.info("Batch write successful: {} items written.", pendingItems.size());
                    return response;
                })
                .exceptionally(ex -> {
                    log.error("Batch write failed: {}", ex.getMessage(), ex);
                    otherFailedWrites.addAndGet(pendingItems.size());
                    return null;
                });

        pendingFutures.add(future); // ✅ Track future for waiting before Lambda exit
        pendingItems.clear(); // ✅ Clear pending items after sending
    }

    /**
     * Converts an object to a JSON string for DynamoDB storage.
     */
    private String serializeToJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            log.error("JSON serialization failed: {}", value, e);
            return "{}"; // Return empty JSON if serialization fails
        }
    }
}
