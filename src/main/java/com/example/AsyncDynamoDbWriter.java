package com.example;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Demonstrates async conditional writes with concurrency checks,
 * and also escapes DynamoDB reserved keywords using expression attribute names.
 */
@Slf4j
public class AsyncDynamoDbWriter {

    private final DynamoDbAsyncClient dynamoDbAsyncClient;
    private final String tableName;
    private final List<Object> pendingModels = new ArrayList<>();

    // Example subset of reserved keywords (uppercase).
    // In production, include the complete list from AWS docs if needed:
    // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html
    private static final Set<String> DYNAMODB_RESERVED_WORDS = Set.of(
            "ABORT", "ABSOLUTE", "ACTION", "ADD", "ALL", "ALTER", "AND", "ANY", "AS",
            "ASC", "AUTHORIZATION", "BATCH", "BEGIN", "BETWEEN", "BIGINT", "BINARY",
            "BLOB", "BOOLEAN", "BY", "CALL", "CASE", "CAST", "CHAR", "COLUMN", "COMMIT",
            "COMPRESS", "CONNECT", "CONNECTION", "CONSISTENCY", "CONSISTENT", "CONSTRAINT",
            "CREATE", "CROSS", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
            "CURRENT_USER", "CURSOR", "DATABASE", "DATE", "DAY", "DEC", "DECIMAL",
            "DECLARE", "DEFAULT", "DELETE", "DESC", "DESCRIBE", "DISTINCT", "DOUBLE",
            "DROP", "DUMP", "DURATION", "DYNAMIC", "EACH", "ELSE", "END", "ESCAPE",
            "EXCEPT", "EXCLUSIVE", "EXEC", "EXISTS", "EXIT", "EXPLAIN", "FALSE", "FETCH",
            "FILE", "FLOAT", "FOR", "FOREIGN", "FROM", "FULL", "FUNCTION", "GLOB", "GRANT",
            "GROUP", "HAVING", "HOUR", "IF", "ILIKE", "IN", "INDEX", "INNER", "INNTER", "INSERT",
            "INT", "INTEGER", "INTERSECT", "INTO", "IS", "ITEM", "JOIN", "KEY", "LANGUAGE",
            "LAST", "LEADING", "LEFT", "LEVEL", "LIKE", "LIMIT", "LOCAL", "LOCK", "LONG",
            "LOOP", "MATCH", "MATERIALIZED", "MERGE", "MINUS", "MINUTE", "MODIFIES",
            "NATURAL", "NCHAR", "NCLOB", "NEW", "NO", "NOT", "NULL", "NULLIF", "NUMBER",
            "NUMERIC", "OF", "OFFLINE", "OFFSET", "OLD", "ON", "ONLINE", "ONLY", "OPEN",
            "OPTION", "OR", "ORDER", "OUT", "OUTER", "OVER", "PARTIAL", "PASSWORD",
            "PATH", "PERCENT", "PRECISION", "PREPARE", "PRIMARY", "PROCEDURE", "RANGE",
            "REAL", "RECORD", "RECURSIVE", "REF", "REFERENCES", "REGEXP", "REINDEX",
            "RELATIVE", "RELEASE", "RENAME", "REPLACE", "RETURN", "RETURNING", "RIGHT",
            "ROLE", "ROLLBACK", "ROLLUP", "ROW", "ROWS", "SAVEPOINT", "SCHEMA", "SCROLL",
            "SECOND", "SECTION", "SELECT", "SESSION", "SET", "SHOW", "SIMILAR", "SMALLINT",
            "SOME", "START", "STATEMENT", "STATIC", "STORAGE", "STORE", "STRUCT",
            "SUBSTRING", "SUM", "SYMMETRIC", "TABLE", "TEMPORARY", "TERMINATED", "TEXT",
            "THEN", "TIME", "TIMESTAMP", "TO", "TOP", "TRAILING", "TRANSACTION", "TRANSFORM",
            "TRANSLATE", "TRANSLATION", "TREAT", "TRIGGER", "TRIM", "TRUE", "TRUNCATE",
            "UNDER", "UNDO", "UNION", "UNIQUE", "UNKNOWN", "UNLOGGED", "UNTIL", "UPDATE",
            "UPPER", "USAGE", "USER", "USING", "UUID", "VALUES", "VARCHAR", "VARIABLE",
            "VARYING", "VIEW", "VOLATILE", "WHEN", "WHERE", "WHILE", "WINDOW", "WITH",
            "WITHOUT", "WORK", "WRITE", "YEAR", "ZONE"
    );

    public AsyncDynamoDbWriter(DynamoDbAsyncClient client, String tableName) {
        this.dynamoDbAsyncClient = client;
        this.tableName = tableName;
    }

    /**
     * Collect a model object to be eventually updated in DynamoDB.
     */
    public void prepareWrite(Object modelObj) {
        if (modelObj != null) {
            pendingModels.add(modelObj);
        }
    }

    /**
     * Execute concurrency-protected writes in parallel, escaping reserved keywords.
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

        // Wait for all futures to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        log.info("All asynchronous writes completed.");
        pendingModels.clear();
    }

    /**
     * Construct an UpdateItemRequest that:
     *  - Sets each non-id, non-version field => fieldName = :val, fieldName_ver = :incomingVersion
     *  - Uses condition => (attribute_not_exists(#r_fieldName_ver) OR #r_fieldName_ver < :incomingVersion) for all
     *  - Escapes field names that are reserved, using ExpressionAttributeNames (#alias).
     */
    private CompletableFuture<UpdateItemResponse> doConditionalUpdateAsync(Object modelObj) {
        Class<?> clazz = modelObj.getClass();

        try {
            // Reflection to get 'id' + 'version' fields
            Field idField = clazz.getDeclaredField("id");
            Field versionField = clazz.getDeclaredField("version");
            idField.setAccessible(true);
            versionField.setAccessible(true);

            Object idVal = idField.get(modelObj);
            Object verVal = versionField.get(modelObj);

            if (idVal == null || verVal == null) {
                log.warn("Model missing id/version. class={}", clazz.getSimpleName());
                return CompletableFuture.completedFuture(null);
            }

            long incomingVersion = ((Number) verVal).longValue();

            // Key => "id"
            Map<String, AttributeValue> key = new HashMap<>();
            key.put("id", AttributeValue.builder().s(idVal.toString()).build());

            // Expression attribute values
            Map<String, AttributeValue> eav = new HashMap<>();
            eav.put(":incomingVersion", AttributeValue.builder().n(Long.toString(incomingVersion)).build());

            // Expression attribute names (for escaping reserved keywords)
            Map<String, String> ean = new HashMap<>();

            StringBuilder updateExpr = new StringBuilder("SET ");
            List<String> conditions = new ArrayList<>();
            boolean firstSet = true;

            // Reflect on all fields
            Field[] allFields = clazz.getDeclaredFields();
            for (Field f : allFields) {
                f.setAccessible(true);
                String fieldName = f.getName();
                if ("id".equals(fieldName) || "version".equals(fieldName)) {
                    // skip
                    continue;
                }
                Object fieldValue = f.get(modelObj);
                if (fieldValue == null) {
                    // skip null fields
                    continue;
                }

                // 1) Escape the base field name if it's reserved
                String escapedFieldName = escapeDdbKeyword(fieldName, ean);

                // 2) Also escape the <fieldName>_ver name
                String versionedName = fieldName + "_ver";
                String escapedVersionedName = escapeDdbKeyword(versionedName, ean);

                // e.g. fieldName => :val, fieldName_ver => :incomingVersion
                String placeholder = ":" + fieldName;
                eav.put(placeholder, toAttributeValue(fieldValue));

                if (!firstSet) {
                    updateExpr.append(", ");
                }
                updateExpr.append(escapedFieldName).append(" = ").append(placeholder)
                        .append(", ").append(escapedVersionedName).append(" = :incomingVersion");
                firstSet = false;

                // condition => (attribute_not_exists(#alias_for_fieldName_ver) OR #alias_for_fieldName_ver < :incomingVersion)
                conditions.add("(attribute_not_exists(" + escapedVersionedName + ") OR "
                        + escapedVersionedName + " < :incomingVersion)");
            }

            if (firstSet) {
                log.debug("No updatable fields for object with id={}", idVal);
                return CompletableFuture.completedFuture(null);
            }

            // Combine all conditions with AND
            String conditionExpr = String.join(" AND ", conditions);

            UpdateItemRequest request = UpdateItemRequest.builder()
                    .tableName(tableName)
                    .key(key)
                    .updateExpression(updateExpr.toString())
                    .conditionExpression(conditionExpr)
                    .expressionAttributeValues(eav)
                    .expressionAttributeNames(ean)    // <---- assign the name map for escaping
                    .build();

            log.debug("DynamoDB update: key={}, updateExpr='{}', condition='{}', EAN={}, EAV={}",
                    key, updateExpr, conditionExpr, ean, eav);

            return dynamoDbAsyncClient.updateItem(request);

        } catch (NoSuchFieldException | IllegalAccessException ex) {
            log.error("Reflection error for class={}, ex={}", clazz.getSimpleName(), ex.getMessage(), ex);
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * If fieldName is reserved by DynamoDB, return a "#alias" and populate ean with alias->fieldName.
     * Otherwise return fieldName as-is.
     */
    private String escapeDdbKeyword(String fieldName, Map<String, String> ean) {
        String fieldNameUpper = fieldName.toUpperCase();
        if (DYNAMODB_RESERVED_WORDS.contains(fieldNameUpper)) {
            // Create an alias, e.g. "#r_<fieldName>"
            String alias = "#r_" + fieldName;
            ean.put(alias, fieldName);
            return alias;
        } else {
            // Not reserved, return fieldName directly.
            // For safer practice, many devs ALWAYS alias. But we'll do it only if reserved here.
            return fieldName;
        }
    }

    private AttributeValue toAttributeValue(Object val) {
        if (val instanceof Number) {
            return AttributeValue.builder().n(val.toString()).build();
        }
        return AttributeValue.builder().s(val.toString()).build();
    }
}
