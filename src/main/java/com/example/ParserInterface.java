package com.example;

import com.amazonaws.services.lambda.runtime.events.KafkaEvent;

public interface ParserInterface<T> {
    /**
     * Parse a single KafkaEventRecord into a typed model object T.
     */
    T parseRecord(KafkaEvent.KafkaEventRecord record) throws Exception;
}
