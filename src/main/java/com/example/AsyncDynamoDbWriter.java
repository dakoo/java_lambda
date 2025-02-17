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
 * Handles **fully asynchronous batch writes** to DynamoDB in chunks of up to 25 items.
 */
@Slf4j
@RequiredArgsConstructor
public class AsyncDynamoDbWriter {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final DynamoDbAsyncClient dynamoDbAsyncClient;
    private final String tableName;

    // ✅ Atomic counters for execution tracking
    private final AtomicInteger successfulWrites;
    private final AtomicInteger conditionalCheckFailedCount;
    private final AtomicInteger otherFailedWrites;

    // ✅ Maximum batch size for DynamoDB
    private static final int MAX_BATCH_SIZE = 25;

    // ✅ List to collect items before writing
    private final List<Map<String, AttributeValue>> pendingItems = new ArrayList<>();

    // ✅ Track batch write futures
    private final List<CompletableFuture<BatchWriteItemResponse>> pendingFutures = new ArrayList<>();

    /**
     * Prepare an item for batch writing.
     * If pendingItems reaches 25, flush the batch asynchronously.
     */
    public void prepareWrite(Object model) {
        try {
            Map<String, AttributeValue> item = convertToDynamoItem(model);
            if (item != null) {
                pendingItems.add(item);
            }
        } catch (Exception e) {
            log.error("Failed to convert item: {}", e.getMessage(), e);
            otherFailedWrites.incrementAndGet();
        }

        // ✅ If pendingItems reaches 25, flush immediately
        if (pendingItems.size() >= MAX_BATCH_SIZE) {
            flushBatch();
        }
    }

    /**
     * Flushes any remaining items and waits for all batch writes to complete.
     */
    public CompletableFuture<Void> executeAsyncWrites() {
        if (!pendingItems.isEmpty()) {
            flushBatch(); // ✅ Flush remaining items before finishing
        }

        // ✅ Wait for all batch writes to complete before Lambda exits
        return CompletableFuture.allOf(pendingFutures.toArray(new CompletableFuture[0]));
    }

    /**
     * Flushes a batch of items asynchronously to DynamoDB.
     * If more than 25 items are pending, splits them into multiple batches.
     */
    private void flushBatch() {
        if (pendingItems.isEmpty()) return;

        // ✅ Split into chunks of 25 for batch write
        List<List<Map<String, AttributeValue>>> batches = splitIntoBatches(pendingItems, MAX_BATCH_SIZE);

        for (List<Map<String, AttributeValue>> batch : batches) {
            List<WriteRequest> writeRequests = batch.stream()
                    .map(item -> WriteRequest.builder()
                            .putRequest(PutRequest.builder().item(item).build())
                            .build())
                    .collect(Collectors.toList());

            BatchWriteItemRequest batchRequest = BatchWriteItemRequest.builder()
                    .requestItems(Collections.singletonMap(tableName, writeRequests))
                    .build();

            CompletableFuture<BatchWriteItemResponse> future = dynamoDbAsyncClient.batchWriteItem(batchRequest)
                    .thenApply(response -> {
                        successfulWrites.addAndGet(batch.size());
                        log.info("Batch write successful: {} items written.", batch.size());
                        return response;
                    })
                    .exceptionally(ex -> {
                        log.error("Batch write failed: {}", ex.getMessage(), ex);
                        otherFailedWrites.addAndGet(batch.size());
                        return null;
                    });

            pendingFutures.add(future);
        }

        // ✅ Clear the pendingItems list after writing
        pendingItems.clear();
    }

    /**
     * Splits a list into batches of maxSize.
     */
    private List<List<Map<String, AttributeValue>>> splitIntoBatches(List<Map<String, AttributeValue>> items, int maxSize) {
        List<List<Map<String, AttributeValue>>> batches = new ArrayList<>();
        for (int i = 0; i < items.size(); i += maxSize) {
            batches.add(new ArrayList<>(items.subList(i, Math.min(items.size(), i + maxSize))));
        }
        return batches;
    }

    /**
     * Converts a model object to a DynamoDB item.
     */
    private Map<String, AttributeValue> convertToDynamoItem(Object modelObj) throws IllegalAccessException {
        Map<String, AttributeValue> item = new HashMap<>();
        Class<?> clazz = modelObj.getClass();

        for (var field : clazz.getDeclaredFields()) {
            field.setAccessible(true);
            Object value = field.get(modelObj);
            if (value == null) continue;

            item.put(field.getName(), convertToAttributeValue(value));
        }

        return item;
    }

    /**
     * Converts Java objects to DynamoDB AttributeValue.
     */
    private AttributeValue convertToAttributeValue(Object value) {
        if (value instanceof Number) {
            return AttributeValue.builder().n(value.toString()).build();
        } else if (value instanceof String) {
            return AttributeValue.builder().s((String) value).build();
        } else if (value instanceof Boolean) {
            return AttributeValue.builder().bool((Boolean) value).build();
        } else if (value instanceof List) {
            List<AttributeValue> listVal = ((List<?>) value).stream()
                    .map(this::convertToAttributeValue)
                    .collect(Collectors.toList());
            return AttributeValue.builder().l(listVal).build();
        } else if (value instanceof Map) {
            Map<String, AttributeValue> mapVal = ((Map<?, ?>) value).entrySet().stream()
                    .collect(Collectors.toMap(
                            e -> e.getKey().toString(),
                            e -> convertToAttributeValue(e.getValue())
                    ));
            return AttributeValue.builder().m(mapVal).build();
        } else {
            return AttributeValue.builder().s(serializeToJson(value)).build();
        }
    }

    /**
     * Serializes an object to JSON.
     */
    private String serializeToJson(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            log.error("JSON serialization failed: {}", value, e);
            return "{}";
        }
    }
}
