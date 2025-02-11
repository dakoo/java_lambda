package com.example;

public class EnvironmentConfig {
    private final String dynamoDbTableName;
    private final String parserClassName;
    private final String modelClassName;
    private final boolean dryRun;
    private final int maxBatchSize;

    private EnvironmentConfig(String dynamoDbTableName,
                              String parserClassName,
                              String modelClassName,
                              boolean dryRun,
                              int maxBatchSize) {
        this.dynamoDbTableName = dynamoDbTableName;
        this.parserClassName = parserClassName;
        this.modelClassName = modelClassName;
        this.dryRun = dryRun;
        this.maxBatchSize = maxBatchSize;
    }

    public static EnvironmentConfig loadFromSystemEnv() {
        String tableName = System.getenv("DYNAMODB_TABLE_NAME");
        String parserClass = System.getenv("PARSER_CLASS_NAME");
        String modelClass = System.getenv("MODEL_CLASS_NAME");
        String dryRunStr = System.getenv("DRY_RUN");
        String batchSizeStr = System.getenv("MAX_BATCH_SIZE");
        
        // Basic validations
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("DYNAMODB_TABLE_NAME must be set");
        }
        if (parserClass == null || parserClass.isEmpty()) {
            throw new IllegalArgumentException("PARSER_CLASS_NAME must be set");
        }
        if (modelClass == null || modelClass.isEmpty()) {
            throw new IllegalArgumentException("MODEL_CLASS_NAME must be set");
        }
        
        boolean dryRunVal = (dryRunStr != null) ? Boolean.parseBoolean(dryRunStr) : false;
        int batchSizeVal = (batchSizeStr != null) ? Integer.parseInt(batchSizeStr) : 25; // default 25

        return new EnvironmentConfig(tableName, parserClass, modelClass, dryRunVal, batchSizeVal);
    }

    public String getDynamoDbTableName() {
        return dynamoDbTableName;
    }

    public String getParserClassName() {
        return parserClassName;
    }

    public String getModelClassName() {
        return modelClassName;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }
}
