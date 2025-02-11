package com.example;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class AsyncDynamoDbWriter {

    private final DynamoDbAsyncClient dynamoDbAsyncClient;
    private final String tableName;
    private final List<Object> pendingModels = new ArrayList<>();

    public AsyncDynamoDbWriter(DynamoDbAsyncClient client, String tableName) {
        this.dynamoDbAsyncClient = client;
        this.tableName = tableName;
    }

    public void prepareWrite(Object modelObj) {
        if (modelObj != null) {
            pendingModels.add(modelObj);
        }
    }

    /**
     * Executes concurrency-protected updates in parallel for all pending models.
     * We gather futures with 'CompletableFuture.allOf(...)' to ensure we wait for them all.
     */
    public void executeAsyncWrites() {
        if (pendingModels.isEmpty()) {
            log.info("No models to write to DynamoDB.");
            return;
        }
        log.info("Submitting {} model(s) to DynamoDB asynchronously...", pendingModels.size());

        List<CompletableFuture<UpdateItemResponse>> futures = new ArrayList<>();
        for (int i = 0; i < pendingModels.size(); i++) {
            final int index = i;
            Object model = pendingModels.get(i);

            CompletableFuture<UpdateItemResponse> future =
                    doConditionalUpdateAsync(model)
                            .exceptionally(ex -> {
                                log.error("Async update failed for item index={}, error={}", index, ex.getMessage(), ex);
                                return null;
                            });
            futures.add(future);
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        log.info("All asynchronous DynamoDB writes completed.");
        pendingModels.clear();
    }

    /**
     * Builds an UpdateItemRequest with concurrency checks for each field.
     * - 'id' is partition key
     * - 'version' is concurrency version
     * - Each other field F => "F = :fVal, F_ver = :incomingVersion" in SET
     * - Condition => "(attribute_not_exists(F_ver) OR F_ver < :incomingVersion)" for each field
     */
    private CompletableFuture<UpdateItemResponse> doConditionalUpdateAsync(Object modelObj) {
        Class<?> clazz = modelObj.getClass();
        try {
            // Identify 'id' and 'version' fields
            Field idField = clazz.getDeclaredField("id");
            Field versionField = clazz.getDeclaredField("version");
            idField.setAccessible(true);
            versionField.setAccessible(true);

            Object idVal = idField.get(modelObj);
            Object verVal = versionField.get(modelObj);

            if (idVal == null || verVal == null) {
                log.warn("Skipping model with null id/version. class={}", clazz.getSimpleName());
                return CompletableFuture.completedFuture(null);
            }

            long incomingVersion = ((Number) verVal).longValue();

            // Build expression
            StringBuilder updateExpr = new StringBuilder("SET ");
            List<String> conditions = new ArrayList<>();
            Map<String, AttributeValue> eav = new HashMap<>();

            // Key => "id" as string
            Map<String, AttributeValue> key = Collections.singletonMap("id",
                    AttributeValue.builder().s(idVal.toString()).build());

            // We'll store the version in expression attributes
            eav.put(":incomingVersion", AttributeValue.builder().n(Long.toString(incomingVersion)).build());

            // Reflect on all declared fields
            Field[] allFields = clazz.getDeclaredFields();
            boolean firstSet = true;

            for (Field f : allFields) {
                f.setAccessible(true);
                String fieldName = f.getName();
                if ("id".equals(fieldName) || "version".equals(fieldName)) {
                    continue; // skip
                }
                Object value = f.get(modelObj);
                if (value == null) {
                    continue; // skip null
                }

                // e.g. "fieldName = :fieldVal, fieldName_ver = :incomingVersion"
                String placeholder = ":" + fieldName;
                eav.put(placeholder, toAttributeValue(value));

                if (!firstSet) {
                    updateExpr.append(", ");
                }
                updateExpr.append(fieldName).append(" = ").append(placeholder)
                        .append(", ").append(fieldName).append("_ver = :incomingVersion");
                firstSet = false;

                // Condition => "(attribute_not_exists(fieldName_ver) OR fieldName_ver < :incomingVersion)"
                String verCol = fieldName + "_ver";
                conditions.add("(attribute_not_exists(" + verCol + ") OR " + verCol + " < :incomingVersion)");
            }

            // If we found no updatable fields, skip
            if (firstSet) {
                log.debug("No fields to update for object with id={}", idVal);
                return CompletableFuture.completedFuture(null);
            }

            String conditionExpr = String.join(" AND ", conditions);

            UpdateItemRequest request = UpdateItemRequest.builder()
                    .tableName(tableName)
                    .key(key)
                    .updateExpression(updateExpr.toString())
                    .conditionExpression(conditionExpr)
                    .expressionAttributeValues(eav)
                    .build();

            log.debug("DDB Update: key={}, updateExpr={}, conditionExpr={}", key, updateExpr, conditionExpr);
            return dynamoDbAsyncClient.updateItem(request);

        } catch (NoSuchFieldException | IllegalAccessException ex) {
            log.error("Reflection error for class={}, ex={}", clazz.getSimpleName(), ex.getMessage(), ex);
            return CompletableFuture.completedFuture(null);
        }
    }

    private AttributeValue toAttributeValue(Object val) {
        if (val instanceof Number) {
            return AttributeValue.builder().n(val.toString()).build();
        }
        // fallback: store as string
        return AttributeValue.builder().s(val.toString()).build();
    }
}
