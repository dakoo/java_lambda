package com.example;

import com.amazonaws.services.lambda.runtime.events.KafkaEvent;

public class RecordParser {
    private final ParserInterface parserInstance;
    private final Class<?> modelClass;

    public RecordParser(String parserClassName, String modelClassName) {
        try {
            Class<?> parserCls = Class.forName(parserClassName);
            parserInstance = (ParserInterface) parserCls.getDeclaredConstructor().newInstance();

            modelClass = Class.forName(modelClassName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate parser or model class", e);
        }
    }

    public Object parse(KafkaEvent.KafkaEventRecord record) throws Exception {
        Object result = parserInstance.parseRecord(record);
        if (!modelClass.isInstance(result)) {
            throw new RuntimeException("Parsed object is not an instance of " + modelClass.getName());
        }
        return result;
    }
}
