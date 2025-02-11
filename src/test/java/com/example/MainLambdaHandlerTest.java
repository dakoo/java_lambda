package com.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

class MainLambdaHandlerTest {

    // We can store the old environment variables and restore them after if we want.
    private String oldTableName;
    private String oldParserClass;
    private String oldModelClass;
    private String oldDryRun;
    private String oldMaxBatchSize;

    @BeforeEach
    void setUp() {
        // Save old environment variable values so we can restore them later (optional)
        oldTableName = System.getenv("DYNAMODB_TABLE_NAME");
        oldParserClass = System.getenv("PARSER_CLASS_NAME");
        oldModelClass = System.getenv("MODEL_CLASS_NAME");
        oldDryRun = System.getenv("DRY_RUN");
        oldMaxBatchSize = System.getenv("MAX_BATCH_SIZE");

        // Set environment variables for the test
        setEnv("DYNAMODB_TABLE_NAME", "testTable");
        setEnv("PARSER_CLASS_NAME", "com.example.CustomJsonBase64Parser");
        setEnv("MODEL_CLASS_NAME", "com.example.Model");
        setEnv("DRY_RUN", "true");           // let's do a dry-run test
        setEnv("MAX_BATCH_SIZE", "10");
    }

    @AfterEach
    void tearDown() {
        // Restore environment variables
        setEnv("DYNAMODB_TABLE_NAME", oldTableName);
        setEnv("PARSER_CLASS_NAME", oldParserClass);
        setEnv("MODEL_CLASS_NAME", oldModelClass);
        setEnv("DRY_RUN", oldDryRun);
        setEnv("MAX_BATCH_SIZE", oldMaxBatchSize);
    }

    @Test
    void testHandleRequest() {
        // Arrange
        KafkaEvent mockEvent = TestUtils.buildTestKafkaEvent();
        MainLambdaHandler handler = new MainLambdaHandler();
        
        // Mock or null context
        Context context = null; // or build a custom context if needed

        // Act
        String result = handler.handleRequest(mockEvent, context);

        // Assert
        assertNotNull(result);
        System.out.println("Lambda result: " + result);

        // Additional validations
        assertTrue(result.contains("Processed 1 record(s)"), "Should process exactly 1 record");
        assertTrue(result.contains("(DRY_RUN)"), "Should reflect dry-run mode in result message");
    }

    // Utility to set environment variables at runtime (works on many, but not all, JDKs).
    // Some JDK versions do not allow modifying env in-process. If you run into issues,
    // you might have to mock calls to System.getenv() or structure code differently.
    private static void setEnv(String key, String value) {
        try {
            if (value == null) {
                // Remove the variable
                Map<String, String> env = System.getenv();
                env.getClass().getDeclaredMethod("remove", Object.class)
                        .invoke(env, key);
            } else {
                // Set/update the variable
                Map<String, String> env = System.getenv();
                env.getClass().getDeclaredMethod("put", Object.class, Object.class)
                        .invoke(env, key, value);
            }
        } catch (Exception e) {
            // In some environments, this might fail with "InaccessibleObjectException"
            // or "UnsupportedOperationException". You can handle that by mocking or
            // configuring code differently.
            System.err.println("Unable to set env var " + key + " due to: " + e);
        }
    }
}
