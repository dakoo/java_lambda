package com.example;

import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class CustomJsonBase64Parser implements ParserInterface {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public Object parseRecord(KafkaEvent.KafkaEventRecord record) throws Exception {
        // 1) Decode the base64 string
        byte[] decodedBytes = Base64.getDecoder().decode(record.getValue()); 
        String jsonStr = new String(decodedBytes, StandardCharsets.UTF_8);

        // 2) Convert JSON => Model
        //    If you want to handle dynamic classes, you can reflect on what modelClass
        //    is expected. Here, for simplicity, we hardcode Model.
        Model model = MAPPER.readValue(jsonStr, Model.class);

        return model;
    }
}
