package com.example;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Demonstrates async conditional writes with concurrency checks
 * and also escapes DynamoDB reserved keywords using expression attribute names.
 *
 * The doConditionalUpdateAsync method is refactored into smaller helper methods
 * for clarity and maintainability.
 */
@Slf4j
public class AsyncDynamoDbWriter {

    private final DynamoDbAsyncClient dynamoDbAsyncClient;
    private final String tableName;
    private final List<Object> pendingModels = new ArrayList<>();

    // Example subset of DynamoDB reserved words for demonstration.
    // In production, you should use the complete list if needed.
    private static final Set<String> DYNAMODB_RESERVED_WORDS = Set.of(
            "ABORT","ADD","ALL","ALTER","AND","ANY","AS","ASC","AUTHORIZATION",
            "BATCH","BEGIN","BETWEEN","BIGINT","BINARY","BLOB","BOOLEAN","BY","CASE",
            "CAST","CHAR","COLUMN","COMMIT","COMPARE","CONSISTENCY","CREATE","CROSS","CURRENT_DATE",
            "CURRENT_TIME","CURRENT_TIMESTAMP","CURRENT_USER","DELETE","DESC","DISTINCT",
            "DOUBLE","DROP","EACH","ELSE","END","ESCAPE","EXISTS","FALSE","FLOAT","FOR",
            "FROM","FULL","FUNCTION","GLOB","GROUP","HAVING","IF","IN","INDEX","INNER",
            "INSERT","INT","INTEGER","INTO","IS","ITEM","JOIN","KEY","LEFT","LIKE","LIMIT",
            "LOCK","LONG","MATCH","MERGE","NATURAL","NO","NOT","NULL","NUMBER","NUMERIC",
            "OF","ON","ONLY","OR","ORDER","OUTER","OVER","PARTITION","PRIMARY","RANGE",
            "REAL","RECORD","RELEASE","REPLACE","RIGHT","ROLE","ROLLUP","SELECT","SET",
            "SHOW","SIMILAR","SMALLINT","SOME","START","SUBSTRING","TABLE","THEN","TIME",
            "TIMESTAMP","TO","TRUE","UPDATE","USAGE","USER","USING","VALUES","VARCHAR",
            "VIEW","WHEN","WHERE","WITH"
    );

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
        log.info("All asynchronous writes completed.");
        pendingModels.clear();
    }

    /**
     * Orchestrates reflection-based concurrency checks for a single model object.
     *  - Finds 'id' and 'version' fields.
     *  - Builds UpdateItemRequest with "fieldName = :val, fieldName_ver = :incomingVersion"
     *  - Condition: "(attribute_not_exists(#aliasForFieldName_ver) OR #aliasForFieldName_ver < :incomingVersion)" for each field
     *  - Escapes reserved keywords with ExpressionAttributeNames
     */
    private CompletableFuture<UpdateItemResponse> doConditionalUpdateAsync(Object modelObj) {
        try {
            // 1) Extract partition key (id) and version
            IdVersionResult idVer = extractIdAndVersion(modelObj);
            if (idVer == null) {
                return CompletableFuture.completedFuture(null);
            }

            // 2) Reflect on the object's other fields => build expressions
            ReflectionExpressions expr = buildUpdateAndConditionExpressions(modelObj, idVer.getIncomingVersion());

            // 3) If nothing to update, skip
            if (expr == null) {
                return CompletableFuture.completedFuture(null);
            }

            // 4) Build the final UpdateItemRequest
            UpdateItemRequest request = buildUpdateRequest(idVer.getIdVal(), expr);
            log.debug("DynamoDB update: key={}, updateExpr='{}', condition='{}', EAN={}, EAV={}",
                    idVer.getIdVal(), expr.updateExpr, expr.conditionExpr, expr.ean, expr.eav);

            // 5) Execute asynchronously
            return dynamoDbAsyncClient.updateItem(request);

        } catch (NoSuchFieldException | IllegalAccessException ex) {
            log.error("Reflection error in doConditionalUpdateAsync: {}", ex.getMessage(), ex);
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Attempts to extract the 'id' and 'version' fields via reflection, returning them in an object.
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

        // We'll cast 'version' to a long
        long incomingVersion = ((Number) verVal).longValue();
        return new IdVersionResult(idVal.toString(), incomingVersion);
    }

    /**
     * Reflect on all fields except 'id'/'version'. For each field F:
     *  - Add "F = :FVal, F_ver = :incomingVersion" to updateExpr
     *  - Add condition => "(attribute_not_exists(#aliasForF_ver) OR #aliasForF_ver < :incomingVersion)"
     *  - Store EAN if needed
     */
    private ReflectionExpressions buildUpdateAndConditionExpressions(Object modelObj, long incomingVersion)
            throws IllegalAccessException {

        Class<?> clazz = modelObj.getClass();
        Field[] allFields = clazz.getDeclaredFields();

        StringBuilder updateExpr = new StringBuilder("SET ");
        List<String> conditions = new ArrayList<>();
        Map<String, AttributeValue> eav = new HashMap<>();
        Map<String, String> ean = new HashMap<>();

        // Add version in eav
        eav.put(":incomingVersion", AttributeValue.builder().n(Long.toString(incomingVersion)).build());

        boolean firstSet = true;

        for (Field f : allFields) {
            f.setAccessible(true);
            String fieldName = f.getName();
            // skip id/version
            if ("id".equals(fieldName) || "version".equals(fieldName)) {
                continue;
            }

            Object fieldValue = f.get(modelObj);
            if (fieldValue == null) {
                // skip null fields
                continue;
            }

            // Possibly escape reserved keyword
            String escapedFieldName = escapeReserved(fieldName, ean);
            // for the "ver" column as well
            String versionedName = fieldName + "_ver";
            String escapedVersionedName = escapeReserved(versionedName, ean);

            // add the field placeholder
            String placeholder = ":" + fieldName;
            eav.put(placeholder, toAttributeValue(fieldValue));

            if (!firstSet) {
                updateExpr.append(", ");
            }
            // e.g. "#r_name = :name, #r_name_ver = :incomingVersion"
            updateExpr.append(escapedFieldName).append(" = ").append(placeholder)
                    .append(", ").append(escapedVersionedName).append(" = :incomingVersion");
            firstSet = false;

            // condition => "(attribute_not_exists(#r_name_ver) OR #r_name_ver < :incomingVersion)"
            String cond = "(attribute_not_exists(" + escapedVersionedName + ") OR "
                    + escapedVersionedName + " < :incomingVersion)";
            conditions.add(cond);
        }

        if (firstSet) {
            log.debug("No updatable fields found for object of class={} => skip", clazz.getSimpleName());
            return null; // means we have nothing to update
        }

        ReflectionExpressions expr = new ReflectionExpressions();
        expr.updateExpr = updateExpr.toString();
        expr.conditionExpr = String.join(" AND ", conditions);
        expr.eav = eav;
        expr.ean = ean;

        return expr;
    }

    /**
     * Build the final UpdateItemRequest from the reflection expressions + key ID.
     * Conditionally set expressionAttributeNames only if ean is non-empty.
     */
    private UpdateItemRequest buildUpdateRequest(String idVal, ReflectionExpressions expr) {
        // Build the key
        Map<String, AttributeValue> key = Collections.singletonMap(
                "id", AttributeValue.builder().s(idVal).build()
        );

        UpdateItemRequest.Builder requestBuilder = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .updateExpression(expr.updateExpr)
                .conditionExpression(expr.conditionExpr)
                .expressionAttributeValues(expr.eav);

        // only set EAN if not empty
        if (!expr.ean.isEmpty()) {
            requestBuilder.expressionAttributeNames(expr.ean);
        }

        return requestBuilder.build();
    }

    /**
     * Checks if fieldName is reserved (based on static set), and if so, returns #alias.
     * Otherwise returns the original fieldName. Also populates EAN if an alias is used.
     */
    private String escapeReserved(String fieldName, Map<String, String> ean) {
        String upper = fieldName.toUpperCase();
        if (DYNAMODB_RESERVED_WORDS.contains(upper)) {
            String alias = "#r_" + fieldName;
            ean.put(alias, fieldName);
            return alias;
        }
        return fieldName; // no alias needed
    }

    private AttributeValue toAttributeValue(Object val) {
        if (val instanceof Number) {
            return AttributeValue.builder().n(val.toString()).build();
        }
        return AttributeValue.builder().s(val.toString()).build();
    }

    /**
     * A small struct-like class to store 'id' + 'version' from reflection.
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
     * A small struct to store the results of building the reflection-based
     * update expression, condition expression, attribute values, and names.
     */
    private static class ReflectionExpressions {
        String updateExpr;
        String conditionExpr;
        Map<String, AttributeValue> eav;
        Map<String, String> ean;
    }
}
