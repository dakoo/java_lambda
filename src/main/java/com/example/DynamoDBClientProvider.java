package com.example;

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;

import java.time.Duration;

public class DynamoDBClientProvider {
    private static final RetryPolicy retryPolicy =RetryPolicy.builder()
            .numRetries(5)  // Retry up to 5 times
            .backoffStrategy(FullJitterBackoffStrategy.builder()
                    .baseDelay(Duration.ofMillis(100))  // Start with 100ms delay
                    .maxBackoffTime(Duration.ofSeconds(2))  // Max wait per retry
                    .build())
            .build();

    private static final SdkAsyncHttpClient asyncHttpClient = NettyNioAsyncHttpClient.builder()
            .maxConcurrency(300)  // ✅ Increase max concurrent requests
            .connectionAcquisitionTimeout(Duration.ofSeconds(5))  // Wait longer before timeout
            .connectionTimeToLive(Duration.ofMinutes(2))  // Keep connections for reuse
            .readTimeout(Duration.ofSeconds(10))  // Allow more time for responses
            .build();

    private static final DynamoDbAsyncClient dynamoDbAsyncClient = DynamoDbAsyncClient.builder()
            .httpClient(asyncHttpClient)  // ✅ Use Netty async client
            .overrideConfiguration(ClientOverrideConfiguration.builder()
                    .retryPolicy(retryPolicy)
                    .build())
            .build();

    public static DynamoDbAsyncClient getClient() {
        return dynamoDbAsyncClient;  // ✅ Return the singleton client
    }
}
