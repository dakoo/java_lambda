package com.example;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

import java.lang.reflect.Field;
import java.util.*;

public class DynamoDBWriter {

    private final DynamoDbClient dynamoDbClient;
    private final EnvironmentConfig config;
    private final LambdaLogger logger;

    // Instead of storing write requests, store the model objects
    // We'll do single-item updates with conditions
    private final List<Object> pendingModels = new ArrayList<>();

    public DynamoDBWriter(DynamoDbClient dynamoDbClient,
                          EnvironmentConfig config,
                          LambdaLogger logger) {
        this.dynamoDbClient = dynamoDbClient;
        this.config = config;
        this.logger = logger;
    }

    /**
     * Collect model objects; we will eventually update them in executeBatchWrites().
     */
    public void prepareWrite(Object modelObj) {
        pendingModels.add(modelObj);
    }

    /**
     * Do single-item conditional updates in "batches" of size config.getMaxBatchSize().
     * Each record’s fields are updated with the pattern:
     *   fieldName        = actual field value
     *   fieldName_version = modelObj.version
     *
     * Condition expression ensures:
     *   (attribute_not_exists(fieldName_version) OR fieldName_version < :incomingVersion)
     * for every field.
     * If any existing field’s version >= incoming version, the update fails.
     */
    public void executeBatchWrites() {
        if (pendingModels.isEmpty()) {
            logger.log("No models to write.");
            return;
        }

        int batchSize = config.getMaxBatchSize();
        logger.log("Starting conditional write in batches (batchSize=" + batchSize + ")");

        int processed = 0;
        int total = pendingModels.size();

        for (int i = 0; i < total; i++) {
            Object modelObj = pendingModels.get(i);
            try {
                doConditionalUpdate(modelObj);
                processed++;
            } catch (DynamoDbException ddbEx) {
                logger.log("DynamoDB update failed for item index=" + i
                           + ": " + ddbEx.getMessage());
                // Depending on your logic, you may want to continue or break
            } catch (Exception e) {
                logger.log("Error updating item index=" + i + ": " + e.getMessage());
            }

            // If you want to chunk your logic or do periodic sleeps, you could do that here
            // e.g. if ((i+1) % batchSize == 0) { ... flush? etc. }
        }

        logger.log("Conditional write complete. " + processed + " / " + total + " updated.");
        pendingModels.clear();
    }

    /**
     * Construct and execute a single UpdateItem with a condition expression
     * checking each (non-id, non-version) field’s `<column>_version`.
     */
    private void doConditionalUpdate(Object modelObj) {
        if (!(modelObj instanceof Model)) {
            logger.log("Skipping unknown model: " + modelObj.getClass().getName());
            return;
        }
        Model model = (Model) modelObj;

        // Partition Key = "id"
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", AttributeValue.builder().s(model.getId()).build());

        // We'll build up the UpdateExpression dynamically.
        // "SET fieldA = :valA, fieldA_version = :v, fieldB = :valB, fieldB_version = :v, ..."
        StringBuilder updateExpr = new StringBuilder("SET ");
        // We'll build up a ConditionExpression that ANDs each "attribute_not_exists(...) OR fieldX_version < :incomingVersion"
        List<String> conditions = new ArrayList<>();
        // Expression Attribute Values
        Map<String, AttributeValue> eav = new HashMap<>();

        // We always need the incoming version in the EAV
        eav.put(":incomingVersion", AttributeValue.builder()
                .n(String.valueOf(model.getVersion())).build());

        // Reflect on all declared fields
        Field[] fields = Model.class.getDeclaredFields();
        boolean firstSet = true;
        for (Field f : fields) {
            f.setAccessible(true);
            String fieldName = f.getName();
            if ("id".equals(fieldName) || "version".equals(fieldName)) {
                // skip partition key and the "version" field
                continue;
            }
            try {
                Object fieldValue = f.get(model);
                if (fieldValue == null) {
                    // Maybe skip or set a REMOVE expression, up to you
                    // For now, skip null fields
                    continue;
                }
                String placeholder = ":" + fieldName;  // e.g. :dataField
                eav.put(placeholder, objectToAttributeValue(fieldValue));

                // Build "SET fieldName = :fieldName, fieldName_version = :incomingVersion,"
                if (!firstSet) {
                    updateExpr.append(", ");
                }
                updateExpr.append(fieldName).append(" = ").append(placeholder).append(", ");
                updateExpr.append(fieldName).append("_version = :incomingVersion");
                firstSet = false;

                // Condition for this field:
                // (attribute_not_exists(fieldName_version) OR fieldName_version < :incomingVersion)
                String colVersion = fieldName + "_version";
                conditions.add("(attribute_not_exists(" + colVersion + ") OR " + colVersion + " < :incomingVersion)");

            } catch (IllegalAccessException e) {
                logger.log("Reflection error: " + e.getMessage());
            }
        }

        // If we have no fields to update, just skip
        if (firstSet) {
            logger.log("No updatable fields found for model ID=" + model.getId());
            return;
        }

        // Combine all conditions with AND
        // e.g. "(attr_not_exists(f1_version) OR f1_version < :incomingVersion) AND (attr_not_exists(f2_version) OR ...)"
        String conditionExpr = String.join(" AND ", conditions);

        UpdateItemRequest updateReq = UpdateItemRequest.builder()
                .tableName(config.getDynamoDbTableName())
                .key(key)
                .updateExpression(updateExpr.toString())
                .conditionExpression(conditionExpr)
                .expressionAttributeValues(eav)
                .build();

        // Execute
        logger.log("Updating item ID=" + model.getId()
                   + " with version=" + model.getVersion()
                   + " using condition=" + conditionExpr);

        dynamoDbClient.updateItem(updateReq);
        logger.log("Update succeeded for ID=" + model.getId());
    }

    private AttributeValue objectToAttributeValue(Object value) {
        // Convert a Java Object (String, Number, etc.) to an AttributeValue
        // This is simplistic. You might want more robust logic for various types.
        if (value instanceof String) {
            return AttributeValue.builder().s((String) value).build();
        } else if (value instanceof Number) {
            // e.g. Integer, Double, etc.
            return AttributeValue.builder().n(value.toString()).build();
        } else {
            // Fallback: store the string representation
            return AttributeValue.builder().s(value.toString()).build();
        }
    }
}
