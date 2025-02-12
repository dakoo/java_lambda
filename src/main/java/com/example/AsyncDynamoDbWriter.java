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
 *    i.e. "SET #r_fieldName = :val, #r_fieldName_ver = :incomingVersion"
 *         and condition => "(attribute_not_exists(#r_fieldName_ver) OR #r_fieldName_ver < :incomingVersion)"
 *    This ensures no collisions with DynamoDB reserved keywords, since we always use an alias.
 *
 * The doConditionalUpdateAsync method is refactored into smaller helpers for readability.
 */
@Slf4j
public class AsyncDynamoDbWriter {

    private final DynamoDbAsyncClient dynamoDbAsyncClient;
    private final String tableName;

    // We accumulate model objects here
    private final List<Object> pendingModels = new ArrayList<>();

    public AsyncDynamoDbWriter(DynamoDbAsyncClient client, String tableName) {
        this.dynamoDbAsyncClient = client;
        this.tableName = tableName;
    }

    /**
     * Accumulate model objects for later writing.
     */
    public void prepareWrite(Object modelObj) {
        if (modelObj != null) {
            pendingModels.add(modelObj);
        }
    }

    /**
     * Perform concurrency-protected updates for all pending models in parallel (async).
     */
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

    /**
     * Reflect on 'id' + 'version' fields, build a concurrency-based UpdateItemRequest,
     * aliasing all field names in the expression.
     */
    private CompletableFuture<UpdateItemResponse> doConditionalUpdateAsync(Object modelObj) {
        try {
            // 1) Extract 'id' and 'version'
            IdVersionResult idVer = extractIdAndVersion(modelObj);
            if (idVer == null) {
                return CompletableFuture.completedFuture(null);
            }

            // 2) Build reflection-based expressions for the other fields
            ReflectionExpressions expr = buildUpdateAndConditionExpressions(modelObj, idVer.getIncomingVersion());
            if (expr == null) {
                // means no fields to update
                return CompletableFuture.completedFuture(null);
            }

            // 3) Build final request
            UpdateItemRequest request = buildUpdateRequest(idVer.getIdVal(), expr);
            log.debug("DynamoDB update: key={}, updateExpr='{}', condition='{}', EAN={}, EAV={}",
                    idVer.getIdVal(), expr.updateExpr, expr.conditionExpr, expr.ean, expr.eav);

            // 4) Execute the async call
            return dynamoDbAsyncClient.updateItem(request);

        } catch (NoSuchFieldException | IllegalAccessException ex) {
            log.error("Reflection error in doConditionalUpdateAsync: {}", ex.getMessage(), ex);
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Helper to extract 'id' + 'version' fields via reflection.
     * Return them in an object, or null if missing.
     */
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
        return new IdVersionResult(idVal.toString(), incomingVersion);
    }

    /**
     * Reflect on all fields except 'id'/'version', always aliasing them as #r_fieldName.
     * Build:
     *   updateExpr => "SET #r_fieldName = :val, #r_fieldName_ver = :incomingVersion"
     *   conditionExpr => "(attribute_not_exists(#r_fieldName_ver) OR #r_fieldName_ver < :incomingVersion)" AND ...
     */
    private ReflectionExpressions buildUpdateAndConditionExpressions(Object modelObj, long incomingVersion)
            throws IllegalAccessException {

        Class<?> clazz = modelObj.getClass();
        Field[] allFields = clazz.getDeclaredFields();

        StringBuilder updateExpr = new StringBuilder("SET ");
        List<String> conditions = new ArrayList<>();
        Map<String, AttributeValue> eav = new HashMap<>();
        Map<String, String> ean = new HashMap<>();

        // We store the version in the EAV
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

            // Always alias => #r_fieldName
            String alias = "#r_" + fieldName;
            ean.put(alias, fieldName);

            // Also alias <fieldName>_ver => #r_fieldName_ver
            String verName = fieldName + "_ver";
            String verAlias = "#r_" + verName;
            ean.put(verAlias, verName);

            // e.g. :fieldName
            String placeholder = ":" + fieldName;
            eav.put(placeholder, toAttributeValue(fieldValue));

            // Append to update expression
            if (!firstSet) {
                updateExpr.append(", ");
            }
            updateExpr.append(alias).append(" = ").append(placeholder)
                    .append(", ").append(verAlias).append(" = :incomingVersion");
            firstSet = false;

            // condition => "(attribute_not_exists(#r_fieldName_ver) OR #r_fieldName_ver < :incomingVersion)"
            String cond = "(attribute_not_exists(" + verAlias + ") OR "
                    + verAlias + " < :incomingVersion)";
            conditions.add(cond);
        }

        if (firstSet) {
            // no fields to update
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

    /**
     * Build the final UpdateItemRequest, conditionally adding expressionAttributeNames
     * only if ean is non-empty.
     */
    private UpdateItemRequest buildUpdateRequest(String idVal, ReflectionExpressions expr) {
        Map<String, AttributeValue> key = Collections.singletonMap(
                "id", AttributeValue.builder().s(idVal).build()
        );

        UpdateItemRequest.Builder requestBuilder = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .updateExpression(expr.updateExpr)
                .conditionExpression(expr.conditionExpr)
                .expressionAttributeValues(expr.eav);

        // If for some reason the model had no aliasing needed, ean might be empty,
        // but in this approach we always alias fields, so it should rarely be empty.
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

    /**
     * Stores 'idVal' as a string and 'incomingVersion' as long.
     */
    private static class IdVersionResult {
        private final String idVal;
        private final long incomingVersion;

        public IdVersionResult(String idVal, long incomingVersion) {
            this.idVal = idVal;
            this.incomingVersion = incomingVersion;
        }

        public String getIdVal() { return idVal; }
        public long getIncomingVersion() { return incomingVersion; }
    }

    /**
     * A small structure to hold reflection-based results.
     */
    private static class ReflectionExpressions {
        String updateExpr;        // The final SET expression
        String conditionExpr;     // The final condition expression
        Map<String, AttributeValue> eav;  // Expression attribute values
        Map<String, String> ean;          // Expression attribute names
    }
}
