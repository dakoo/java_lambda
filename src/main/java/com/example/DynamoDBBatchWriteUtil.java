package com.example;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class to handle chunking a large list of WriteRequests into 
 * sub-batches and calling DynamoDB in a loop.
 */
public class DynamoDBBatchWriteUtil {

    public static void batchWrite(DynamoDbClient dynamoDbClient,
                                  String tableName,
                                  List<WriteRequest> requests,
                                  int batchSize,
                                  LambdaLogger logger) {
        
        if (requests.isEmpty()) {
            logger.log("No items to batch write.");
            return;
        }

        int total = requests.size();
        int start = 0;
        while (start < total) {
            int end = Math.min(start + batchSize, total);
            List<WriteRequest> subList = requests.subList(start, end);
            
            Map<String, List<WriteRequest>> requestItems = new HashMap<>();
            requestItems.put(tableName, new ArrayList<>(subList));
            
            BatchWriteItemRequest batchRequest = BatchWriteItemRequest.builder()
                .requestItems(requestItems)
                .build();
            
            BatchWriteItemResponse batchWriteItemResponse = dynamoDbClient.batchWriteItem(batchRequest);
            
            // Check for unprocessed items, retry logic if needed
            Map<String, List<WriteRequest>> unprocessed = batchWriteItemResponse.unprocessedItems();
            while (unprocessed != null && !unprocessed.isEmpty()) {
                logger.log("Retrying unprocessed items: " + unprocessed.size());
                BatchWriteItemRequest retryRequest = BatchWriteItemRequest.builder()
                    .requestItems(unprocessed)
                    .build();
                batchWriteItemResponse = dynamoDbClient.batchWriteItem(retryRequest);
                unprocessed = batchWriteItemResponse.unprocessedItems();
            }

            start = end;
        }
        
        logger.log("Batch write complete for " + total + " items.");
    }
}
