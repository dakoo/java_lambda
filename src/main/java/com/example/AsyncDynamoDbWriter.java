package com.example;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous DynamoDB writer that:
 * - Reflects on a model object to build concurrency-based "fieldName_ver" logic.
 * - ALWAYS aliases all field names in the UpdateExpression (prevents reserved keyword issues).
 * - Dynamically detects whether the id field is a Number or String and formats it correctly.
 */
@Slf4j
@RequiredArgsConstructor
public class AsyncDynamoDbWriter {

    private final DynamoDbAsyncClient dynamoDbAsyncClient;
    private final String tableName;
    private final List<Object> pendingModels = new ArrayList<>();

    public void prepareWrite(Object modelObj) {
        if (modelObj != null) {
            pendingModels.add(modelObj);
        }
    }

    public void executeAsyncWrites() {
        if (pendingModels.isEmpty()) {
            log.info("No models to write to DynamoDB.");
            return;
        }

        log.info("Submitting {} model(s) asynchronously to DynamoDB...", pendingModels.size());

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

    private CompletableFuture<UpdateItemResponse> doConditionalUpdateAsync(Object modelObj) {
        try {
            IdVersionResult idVer = extractIdAndVersion(modelObj);
            if (idVer == null) {
                return CompletableFuture.completedFuture(null);
            }

            ReflectionExpressions expr = buildUpdateAndConditionExpressions(modelObj, idVer.getIncomingVersion());
            if (expr == null) {
                return CompletableFuture.completedFuture(null);
            }

            UpdateItemRequest request = buildUpdateRequest(idVer.getIdValue(), idVer.isIdNumeric(), expr);
            log.debug("DynamoDB update: key={}, updateExpr='{}', condition='{}', EAN={}, EAV={}",
                    idVer.getIdValue(), expr.updateExpr, expr.conditionExpr, expr.ean, expr.eav);

            return dynamoDbAsyncClient.updateItem(request);

        } catch (NoSuchFieldException | IllegalAccessException ex) {
            log.error("Reflection error in doConditionalUpdateAsync: {}", ex.getMessage(), ex);
            return CompletableFuture.completedFuture(null);
        }
    }

    private IdVersionResult extractIdAndVersion(Object modelObj)
            throws NoSuchFieldException, IllegalAccessException {

        Class<?> clazz = modelObj.getClass();
        Field idField = clazz.getDeclaredField("id");
        Field versionField = clazz.getDeclaredField("version");
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
        return new IdVersionResult(idVal.toString(), isNumeric, incomingVersion);
    }

    private ReflectionExpressions buildUpdateAndConditionExpressions(Object modelObj, long incomingVersion)
            throws IllegalAccessException {

        Class<?> clazz = modelObj.getClass();
        Field[] allFields = clazz.getDeclaredFields();

        StringBuilder updateExpr = new StringBuilder("SET version = :incomingVersion");  // ✅ Ensure version is updated
        List<String> conditions = new ArrayList<>();
        Map<String, AttributeValue> eav = new HashMap<>();
        Map<String, String> ean = new HashMap<>();

        eav.put(":incomingVersion", AttributeValue.builder().n(Long.toString(incomingVersion)).build());

        for (Field f : allFields) {
            f.setAccessible(true);
            String fieldName = f.getName();
            if ("id".equals(fieldName) || "version".equals(fieldName)) {
                continue;
            }

            Object fieldValue = f.get(modelObj);
            if (fieldValue == null) {
                continue;
            }

            String alias = "#r_" + fieldName;
            ean.put(alias, fieldName);

            String verName = fieldName + "_ver";
            String verAlias = "#r_" + verName;
            ean.put(verAlias, verName);

            String placeholder = ":" + fieldName;
            eav.put(placeholder, toAttributeValue(fieldValue));

            updateExpr.append(", ").append(alias).append(" = ").append(placeholder)
                    .append(", ").append(verAlias).append(" = :incomingVersion");

            String cond = "(attribute_not_exists(" + verAlias + ") OR " + verAlias + " < :incomingVersion)";
            conditions.add(cond);
        }

        if (conditions.isEmpty()) {
            log.debug("No updatable fields for class={}, skipping", clazz.getSimpleName());
            return null;
        }

        return new ReflectionExpressions(updateExpr.toString(), String.join(" AND ", conditions), eav, ean);
    }

    private UpdateItemRequest buildUpdateRequest(String idVal, boolean isIdNumeric, ReflectionExpressions expr) {
        Map<String, AttributeValue> key = Collections.singletonMap(
                "id", isIdNumeric ? AttributeValue.builder().n(idVal).build()
                        : AttributeValue.builder().s(idVal).build()
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

    private AttributeValue toAttributeValue(Object val) {
        if (val instanceof Number) {
            return AttributeValue.builder().n(val.toString()).build();
        }
        return AttributeValue.builder().s(val.toString()).build();
    }

    @Getter
    @AllArgsConstructor
    private static class IdVersionResult {
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
