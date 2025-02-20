
package com.example;

import com.example.annotations.PartitionKey;
import com.example.annotations.VersionKey;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Getter;
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
 * Handles asynchronous writes to DynamoDB.
 */
@Slf4j
@RequiredArgsConstructor
public class AsyncDynamoDbWriter {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final DynamoDbAsyncClient dynamoDbAsyncClient;
    private final String tableName;

    // ✅ Counters for tracking execution results
    private final AtomicInteger successfulWrites;
    private final AtomicInteger conditionalCheckFailedCount;
    private final AtomicInteger otherFailedWrites;

    private final List<Object> pendingModels = new ArrayList<>();

    public void prepareWrite(Object modelObj) {
        if (modelObj != null) {
            pendingModels.add(modelObj);
        }
    }

    public void executeAsyncWrites() {
        if (pendingModels.isEmpty()) {
            return;
        }
        long startTime = System.currentTimeMillis();

        List<UpdateItemRequest> updateRequests = new ArrayList<>();
        for (Object model : pendingModels) {
            log.info("Starting async update for model: {}", model);
            try {
                IdVersionResult idVer = extractIdAndVersion(model);
                if(idVer == null) {
                    otherFailedWrites.incrementAndGet();
                    continue;
                }
                ReflectionExpressions expr = buildUpdateAndConditionExpressions(model, idVer.getIncomingVersion(), idVer.getVersionKey(), idVer.getPartitionKey());
                if(expr == null) {
                    otherFailedWrites.incrementAndGet();
                    continue;
                }

                UpdateItemRequest request = buildUpdateRequest(idVer, expr);
                updateRequests.add(request);

            } catch (NoSuchFieldException | IllegalAccessException ex) {
                log.error("Reflection error: {}", ex.getMessage(), ex);
                otherFailedWrites.incrementAndGet();
            }
        }

        long parsingAndRequestPrepTime = System.currentTimeMillis();

        List<CompletableFuture<UpdateItemResponse>> futures =
                updateRequests.parallelStream()
                        .map(request -> dynamoDbAsyncClient.updateItem(request).thenApply(response -> {
                                    log.info("Completed update at {}", System.currentTimeMillis());
                                    successfulWrites.incrementAndGet();
                                    return response;
                                })
                                .exceptionally(ex -> {
                                    if (ex.getCause() instanceof ConditionalCheckFailedException) {
                                        conditionalCheckFailedCount.incrementAndGet();
                                        log.warn("Conditional update failed.");
                                    } else {
                                        otherFailedWrites.incrementAndGet();
                                        log.error("Async update failed: {}", ex.getMessage(), ex);
                                    }
                                    return null;
                                }))
                        .collect(Collectors.toList());

//        List<CompletableFuture<UpdateItemResponse>> futures = new ArrayList<>();
//        for (UpdateItemRequest request : updateRequests) {
//            CompletableFuture<UpdateItemResponse> future = dynamoDbAsyncClient.updateItem(request)
//                            .thenApply(response -> {
//                                log.info("Completed update at {}", System.currentTimeMillis());
//                                successfulWrites.incrementAndGet();
//                                return response;
//                            })
//                            .exceptionally(ex -> {
//                                if (ex.getCause() instanceof ConditionalCheckFailedException) {
//                                    conditionalCheckFailedCount.incrementAndGet();
//                                    log.warn("Conditional update failed.");
//                                } else {
//                                    otherFailedWrites.incrementAndGet();
//                                    log.error("Async update failed: {}", ex.getMessage(), ex);
//                                }
//                                return null;
//                            });
//
//            futures.add(future);
//        }
        long sendUpdateTime = System.currentTimeMillis();
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        long updateCompletionTime = System.currentTimeMillis();
        pendingModels.clear();

        // Print execution time details
        System.out.println("Execution time breakdown:");
        System.out.println("  Update Requests Prep time: " + (parsingAndRequestPrepTime - startTime) + " ms");
        System.out.println("  Send Requests time: " + (sendUpdateTime - parsingAndRequestPrepTime) + " ms");
        System.out.println("  Completion Waiting time: " + (updateCompletionTime - sendUpdateTime) + " ms");
    }

    /**
     * ✅ Extracts partition key and version key dynamically, supporting both Long and String IDs.
     */
    private IdVersionResult extractIdAndVersion(Object modelObj)
            throws NoSuchFieldException, IllegalAccessException {

        Class<?> clazz = modelObj.getClass();
        Field idField = null;
        Field versionField = null;

        // Find fields annotated with @PartitionKey and @VersionKey
        for (Field field : clazz.getDeclaredFields()) {
            if (field.isAnnotationPresent(PartitionKey.class)) {
                idField = field;
            }
            if (field.isAnnotationPresent(VersionKey.class)) {
                versionField = field;
            }
        }

        if (idField == null) {
            throw new NoSuchFieldException("Partition key not found in class: " + clazz.getSimpleName());
        }
        if (versionField == null) {
            throw new NoSuchFieldException("Version key not found in class: " + clazz.getSimpleName());
        }

        idField.setAccessible(true);
        versionField.setAccessible(true);

        Object idVal = idField.get(modelObj);
        Object verVal = versionField.get(modelObj);

        if (idVal == null || verVal == null) {
            log.warn("Skipping model with null id/version. class={}", clazz.getSimpleName());
            return null;
        }

        long incomingVersion = ((Number) verVal).longValue();
        boolean isNumeric = idVal instanceof Number;

        return new IdVersionResult(idField.getName(), versionField.getName(), idVal.toString(), isNumeric, incomingVersion);
    }

    /**
     * ✅ Builds the DynamoDB UpdateItemRequest dynamically.
     */
    private UpdateItemRequest buildUpdateRequest(IdVersionResult idVer, ReflectionExpressions expr) {
        Map<String, AttributeValue> key = Collections.singletonMap(
                idVer.getPartitionKey(), idVer.isIdNumeric() ?
                        AttributeValue.builder().n(idVer.getIdValue()).build() :
                        AttributeValue.builder().s(idVer.getIdValue()).build()
        );

        UpdateItemRequest.Builder requestBuilder = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .updateExpression(expr.updateExpr)
                .conditionExpression(expr.conditionExpr)
                .expressionAttributeValues(expr.eav);

        if (!expr.ean.isEmpty()) {
            requestBuilder.expressionAttributeNames(expr.ean);
        }

        return requestBuilder.build();
    }

    private ReflectionExpressions buildUpdateAndConditionExpressions(Object modelObj, long incomingVersion, String versionKey, String partitionKey)
            throws IllegalAccessException {

        Class<?> clazz = modelObj.getClass();
        Field[] allFields = clazz.getDeclaredFields();

        StringBuilder updateExpr = new StringBuilder("SET " + versionKey + " = :incomingVersion");
        List<String> conditions = new ArrayList<>();
        Map<String, AttributeValue> eav = new HashMap<>();
        Map<String, String> ean = new HashMap<>();

        eav.put(":incomingVersion", AttributeValue.builder().n(Long.toString(incomingVersion)).build());

        for (Field f : allFields) {
            f.setAccessible(true);
            String fieldName = f.getName();

            // ✅ Skip partition key to avoid DynamoDB error
            if (fieldName.equals(versionKey) || fieldName.equals(partitionKey)) {
                continue;
            }

            Object fieldValue = f.get(modelObj);
            if (fieldValue == null) {
                continue;
            }

            String serializedValue = serializeToJson(fieldValue);

            String alias = "#r_" + fieldName;
            ean.put(alias, fieldName);

            String verName = fieldName + "_ver";
            String verAlias = "#r_" + verName;
            ean.put(verAlias, verName);

            String placeholder = ":" + fieldName;
            eav.put(placeholder, AttributeValue.builder().s(serializedValue).build());

            updateExpr.append(", ").append(alias).append(" = ").append(placeholder)
                    .append(", ").append(verAlias).append(" = :incomingVersion");

            String cond = "(attribute_not_exists(" + verAlias + ") OR " + verAlias + " < :incomingVersion)";
            conditions.add(cond);
        }

        if (conditions.isEmpty()) {
            return null;
        }

        return new ReflectionExpressions(updateExpr.toString(), String.join(" AND ", conditions), eav, ean);
    }


    /**
     * ✅ Converts nested objects and collections to JSON before storing in DynamoDB.
     */
    private String serializeToJson(Object val) {
        try {
            return objectMapper.writeValueAsString(val);
        } catch (JsonProcessingException e) {
            log.error("JSON serialization failed: {}", val, e);
            return "{}"; // Return empty JSON if serialization fails
        }
    }

    @Getter
    @AllArgsConstructor
    private static class IdVersionResult {
        private final String partitionKey;  // ✅ Stores the partition key name
        private final String versionKey;    // ✅ Stores the version key name
        private final String idValue;
        private final boolean isIdNumeric;
        private final long incomingVersion;
    }

    @Getter
    @AllArgsConstructor
    private static class ReflectionExpressions {
        private final String updateExpr;
        private final String conditionExpr;
        private final Map<String, AttributeValue> eav;
        private final Map<String, String> ean;
    }
}
