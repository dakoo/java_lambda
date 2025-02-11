package com.example;

import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import com.amazonaws.services.lambda.runtime.events.KafkaEvent.KafkaEventRecord;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class TestUtils {

    public static KafkaEvent buildTestKafkaEvent() {
        KafkaEvent event = new KafkaEvent();
        Map<String, List<KafkaEventRecord>> recordsMap = new HashMap<>();

        // For example, we pretend there's a single topic-partition "myTopic-0"
        String topicPartitionKey = "myTopic-0";

        // Create a sample record
        KafkaEventRecord record = new KafkaEventRecord();
        // If your actual event is base64-encoded, you might do that here:
        String payload = "{\"id\":\"test-id\",\"version\":1,\"dataField\":\"testData\",\"description\":\"sample\",\"amount\":100.0}";
        byte[] valueBytes = Base64.getEncoder().encode(payload.getBytes(StandardCharsets.UTF_8));
        record.setValue(valueBytes);
        // or if using setValueAsString, be sure to see how it's used in the code

        // Possibly set other metadata, like offset or timestamp
        record.setPartition(0);
        record.setOffset(1234L);
        record.setTimestamp(System.currentTimeMillis());

        // Put the record in a list
        List<KafkaEventRecord> recordList = new ArrayList<>();
        recordList.add(record);

        // Put the list in the map
        recordsMap.put(topicPartitionKey, recordList);

        // Assign to the event
        event.setRecords(recordsMap);

        return event;
    }
}
