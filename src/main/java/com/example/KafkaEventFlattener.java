package com.example;

import com.amazonaws.services.lambda.runtime.events.KafkaEvent;

import java.util.ArrayList;
import java.util.List;

public class KafkaEventFlattener {


    /**
     * Flatten all Kafka records from all topics/partitions into a single list.
     */
    public static List<KafkaEvent.KafkaEventRecord> flatten(KafkaEvent event) {
        List<KafkaEvent.KafkaEventRecord> flattened = new ArrayList<>();
        
        if (event == null || event.getRecords() == null) {
            return flattened;
        }

        // The event.getRecords() is a map<topic-partition, List<KafkaEventRecord>>
        event.getRecords().forEach((topicPartition, records) -> {
            flattened.addAll(records);
        });

        return flattened;
    }
}
