package com.example;

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;

import java.time.Duration;

public class DynamoDBClientProvider {
    private static final RetryPolicy defaultRetryPolicy = RetryPolicy.defaultRetryPolicy()
            .toBuilder()
            .numRetries(3) // Retry 5 times
            .build();

    private static final SdkAsyncHttpClient asyncHttpClient = NettyNioAsyncHttpClient.builder()
            .maxConcurrency(300)  // ✅ Increase max concurrent requests
            .connectionAcquisitionTimeout(Duration.ofSeconds(2))
            .build();

    private static final DynamoDbAsyncClient dynamoDbAsyncClient = DynamoDbAsyncClient.builder()
            .httpClient(asyncHttpClient)  // ✅ Use Netty async client
            .overrideConfiguration(ClientOverrideConfiguration.builder()
                    .retryPolicy(defaultRetryPolicy)
                    .build())
            .build();

    public static DynamoDbAsyncClient getClient() {
        return dynamoDbAsyncClient;  // ✅ Return the singleton client
    }
}
