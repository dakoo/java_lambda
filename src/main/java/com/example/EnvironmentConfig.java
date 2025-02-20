package com.example;

import lombok.*;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EnvironmentConfig {
    private String dynamoDbTableName;
    private String parserName;
    private boolean dryRun;
    private int maxBatchSize; // optional, if you chunk the writes

    public static EnvironmentConfig loadFromSystemEnv() {
        String tableName = getenvOrDefault("DYNAMODB_TABLE_NAME", "DefaultTable");
        String parser = getenvOrDefault("PARSER_NAME", "DishParser");
        boolean dry = Boolean.parseBoolean(System.getenv("DRY_RUN"));
        int batch = Integer.parseInt(getenvOrDefault("MAX_BATCH_SIZE", "25"));

        return EnvironmentConfig.builder()
                .dynamoDbTableName(tableName)
                .parserName(parser)
                .dryRun(dry)
                .maxBatchSize(batch)
                .build();
    }

    private static String getenvOrDefault(String key, String defaultVal) {
        String val = System.getenv(key);
        return (val != null && !val.isEmpty()) ? val : defaultVal;
    }
}
