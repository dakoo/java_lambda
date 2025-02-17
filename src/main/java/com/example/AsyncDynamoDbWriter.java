package com.example;

import com.example.annotations.PartitionKey;
import com.example.annotations.VersionKey;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Handles asynchronous batch writes to DynamoDB in groups of up to 25 items.
 */
@Slf4j
@RequiredArgsConstructor
public class AsyncDynamoDbWriter {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final DynamoDbAsyncClient dynamoDbAsyncClient;
    private final String tableName;

    // Counters for success/failure/tracking
    private final AtomicInteger successfulWrites;
    private final AtomicInteger conditionalCheckFailedCount;
    private final AtomicInteger otherFailedWrites;

    // For grouping items before flush
    private static final int MAX_BATCH_SIZE = 25;
    private final List<Map<String, AttributeValue>> pendingItems = new ArrayList<>();

    // Keep track of ongoing batch requests
    private final List<CompletableFuture<BatchWriteItemResponse>> pendingFutures = new ArrayList<>();

    /**
     * Prepare an item for batch writing.
     * If pendingItems reaches 25, flush the batch immediately.
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

        if (pendingItems.size() >= MAX_BATCH_SIZE) {
            flushBatch();
        }
    }

    /**
     * Flush any remaining items, then wait for all batch operations to complete.
     */
    public CompletableFuture<Void> executeAsyncWrites() {
        if (!pendingItems.isEmpty()) {
            flushBatch();
        }
        return CompletableFuture.allOf(pendingFutures.toArray(new CompletableFuture[0]));
    }

    /**
     * Convert a model object to a DynamoDB item (Map<String, AttributeValue>).
     * Also logs partition key and version key for debugging.
     */
    private Map<String, AttributeValue> convertToDynamoItem(Object modelObj) throws IllegalAccessException {
        Map<String, AttributeValue> item = new HashMap<>();
        Class<?> clazz = modelObj.getClass();

        String partitionKeyName = null;
        String versionKeyName = null;

        for (Field f : clazz.getDeclaredFields()) {
            f.setAccessible(true);
            Object value = f.get(modelObj);
            if (value == null) continue;

            // Detect partition key & version key
            if (f.isAnnotationPresent(PartitionKey.class)) {
                partitionKeyName = f.getName();
            }
            if (f.isAnnotationPresent(VersionKey.class)) {
                versionKeyName = f.getName();
            }

            item.put(f.getName(), convertToAttributeValue(value));
        }

        // Log partition key if present
        if (partitionKeyName != null && item.containsKey(partitionKeyName)) {
            AttributeValue pkAttr = item.get(partitionKeyName);
            log.info("Partition Key: {}={} (Type: {})", partitionKeyName, pkAttr, pkAttr.type());
        } else {
            log.warn("PartitionKey not found or missing in item: {}", item);
        }

        // Log version key if present
        if (versionKeyName != null && item.containsKey(versionKeyName)) {
            AttributeValue verAttr = item.get(versionKeyName);
            log.info("Version Key: {}={} (Type: {})", versionKeyName, verAttr, verAttr.type());
        }

        return item;
    }

    /**
     * Convert a Java object to DynamoDB AttributeValue.
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
            // fallback -> store as JSON string
            return AttributeValue.builder().s(serializeToJson(value)).build();
        }
    }

    /**
     * Actually flush the batch (if pendingItems are > 0).
     */
    private void flushBatch() {
        if (pendingItems.isEmpty()) {
            return;
        }

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
                    // On success
                    successfulWrites.addAndGet(pendingItems.size());
                    log.info("Batch write successful: {} items written.", pendingItems.size());
                    return response;
                })
                .exceptionally(ex -> {
                    log.error("Batch write failed: {}", ex.getMessage(), ex);
                    otherFailedWrites.addAndGet(pendingItems.size());
                    return null;
                });

        pendingFutures.add(future);
        pendingItems.clear();
    }

    /**
     * Convert object to JSON string for fallback storage.
     */
    private String serializeToJson(Object val) {
        try {
            return MAPPER.writeValueAsString(val);
        } catch (JsonProcessingException e) {
            log.error("JSON serialization failed: {}", val, e);
            return "{}";
        }
    }
}
