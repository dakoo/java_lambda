package com.example;

import com.amazonaws.services.lambda.runtime.events.KafkaEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class KafkaEventFlattener {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventFlattener.class);

    public static List<KafkaEvent.KafkaEventRecord> flatten(KafkaEvent event) {
        List<KafkaEvent.KafkaEventRecord> flattened = new ArrayList<>();
        if (event == null || event.getRecords() == null) {
            log.warn("Kafka event or records map is null.");
            return flattened;
        }

        event.getRecords().forEach((topicPartition, records) -> {
            log.debug("Flattening topicPartition='{}' with {} record(s).", topicPartition, records.size());
            flattened.addAll(records);
        });

        return flattened;
    }
}
