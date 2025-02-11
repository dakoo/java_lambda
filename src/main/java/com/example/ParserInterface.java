package com.example;

import com.amazonaws.services.lambda.runtime.events.KafkaEvent;

public interface ParserInterface {
    /**
     * Parse a single KafkaEventRecord into a Java object (the "model").
     */
    Object parseRecord(KafkaEvent.KafkaEventRecord record) throws Exception;
}
