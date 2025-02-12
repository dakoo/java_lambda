package com.example;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous DynamoDB writer that:
 * 1) Reflects on a model object to build concurrency-based "fieldName_ver" logic.
 * 2) ALWAYS aliases all field names in the UpdateExpression (no reserved word checks).
 * 3) Dynamically detects the id field type (Number or String) and formats it correctly.
 */
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

    /**
     * ✅ **Missing method added: buildUpdateAndConditionExpressions()**
     *
     * Builds:
     * - updateExpr → `"SET #r_field = :val, #r_field_ver = :incomingVersion"`
     * - conditionExpr → `"attribute_not_exists(#r_field_ver) OR #r_field_ver < :incomingVersion"`
     * - Always aliases field names as `#r_field`
     */
    private ReflectionExpressions buildUpdateAndConditionExpressions(Object modelObj, long incomingVersion)
            throws IllegalAccessException {

        Class<?> clazz = modelObj.getClass();
        Field[] allFields = clazz.getDeclaredFields();

        StringBuilder updateExpr = new StringBuilder("SET ");
        List<String> conditions = new ArrayList<>();
        Map<String, AttributeValue> eav = new HashMap<>();
        Map<String, String> ean = new HashMap<>();

        eav.put(":incomingVersion", AttributeValue.builder().n(Long.toString(incomingVersion)).build());

        boolean firstSet = true;

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

            if (!firstSet) {
                updateExpr.append(", ");
            }
            updateExpr.append(alias).append(" = ").append(placeholder)
                    .append(", ").append(verAlias).append(" = :incomingVersion");
            firstSet = false;

            String cond = "(attribute_not_exists(" + verAlias + ") OR " + verAlias + " < :incomingVersion)";
            conditions.add(cond);
        }

        if (firstSet) {
            log.debug("No updatable fields for class={}, skipping", clazz.getSimpleName());
            return null;
        }

        ReflectionExpressions expr = new ReflectionExpressions();
        expr.updateExpr = updateExpr.toString();
        expr.conditionExpr = String.join(" AND ", conditions);
        expr.eav = eav;
        expr.ean = ean;

        return expr;
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

    private static class IdVersionResult {
        private final String idValue;
        private final boolean isIdNumeric;
        private final long incomingVersion;

        public IdVersionResult(String idValue, boolean isIdNumeric, long incomingVersion) {
            this.idValue = idValue;
            this.isIdNumeric = isIdNumeric;
            this.incomingVersion = incomingVersion;
        }

        public String getIdValue() { return idValue; }
        public boolean isIdNumeric() { return isIdNumeric; }
        public long getIncomingVersion() { return incomingVersion; }
    }

    private static class ReflectionExpressions {
        String updateExpr;
        String conditionExpr;
        Map<String, AttributeValue> eav;
        Map<String, String> ean;
    }
}
