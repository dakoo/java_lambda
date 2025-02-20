package com.example;

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;

import java.time.Duration;

public class DynamoDBClientProvider {
    private static final DynamoDbAsyncClient dynamoDbAsyncClient = DynamoDbAsyncClient.builder()
            .httpClient(NettyNioAsyncHttpClient.builder()
                    .maxConcurrency(300)  // Allow up to 300 concurrent requests
                    .connectionAcquisitionTimeout(Duration.ofSeconds(5))  // Wait longer for available connections
                    .connectionTimeToLive(Duration.ofMinutes(2))  // Keep connections alive for reuse
                    .readTimeout(Duration.ofSeconds(10))  // Allow more time for responses
                    .build())
            .overrideConfiguration(ClientOverrideConfiguration.builder()
                    .retryPolicy(RetryPolicy.builder()
                            .numRetries(5)  // Retry up to 5 times
                            .backoffStrategy(FullJitterBackoffStrategy.builder()
                                    .baseDelay(Duration.ofMillis(100))  // Start retry with 100ms delay
                                    .maxBackoffTime(Duration.ofSeconds(2))  // Max wait per retry
                                    .build())
                            .build())
                    .build())
            .build();


    public static DynamoDbAsyncClient getClient() {
        return dynamoDbAsyncClient;  // ✅ Return the singleton client
    }
}
