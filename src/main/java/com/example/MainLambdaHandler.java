package com.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.List;

public class MainLambdaHandler implements RequestHandler<KafkaEvent, String> {

    @Override
    public String handleRequest(KafkaEvent event, Context context) {
        LambdaLogger logger = context.getLogger();

        // 1. Load env config
        EnvironmentConfig config = EnvironmentConfig.loadFromSystemEnv();

        // 2. Log event
        logger.log("Received Kafka event: " + event);

        // 3. Flatten Kafka records
        List<KafkaEvent.KafkaEventRecord> flatRecords = KafkaEventFlattener.flatten(event);
        logger.log("Flattened " + flatRecords.size() + " record(s).");

        // 4. Create parser (reflection)
        RecordParser recordParser = new RecordParser(
                config.getParserClassName(),
                config.getModelClassName()
        );

        // 5. DynamoDB writer
        DynamoDbClient ddbClient = DynamoDbClient.builder().build();
        DynamoDBWriter writer = new DynamoDBWriter(ddbClient, config, logger);

        // 6. For each record, parse (base64 decode + JSON) => Model => add to writer
        int processedCount = 0;
        for (KafkaEvent.KafkaEventRecord r : flatRecords) {
            try {
                Object modelObj = recordParser.parse(r);
                writer.prepareWrite(modelObj);
                processedCount++;
            } catch (Exception e) {
                logger.log("Error parsing record: " + e.getMessage());
            }
        }

        // 7. If dryRun==false, actually update DynamoDB; else skip
        if (!config.isDryRun()) {
            logger.log("Executing conditional updates (max batch size: " + config.getMaxBatchSize() + ")");
            writer.executeBatchWrites();
        } else {
            logger.log("Dry run enabled, skipping DynamoDB writes.");
        }

        String resultMsg = "Processed " + processedCount + " record(s). " +
                           (config.isDryRun() ? "(DRY_RUN)" : "(DONE)");
        logger.log(resultMsg);

        return resultMsg;
    }
}
