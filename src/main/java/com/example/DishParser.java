package com.example;

import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

@Slf4j
public class DishParser implements ParserInterface<Dish> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public Dish parseRecord(KafkaEvent.KafkaEventRecord record) throws Exception {
        // 1) Decode base64 key (optional)
        String keyB64 = record.getKey();
        if (keyB64 != null && !keyB64.isEmpty()) {
            byte[] keyBytes = Base64.getDecoder().decode(keyB64);
            String decodedKey = new String(keyBytes, StandardCharsets.UTF_8);
            log.debug("Decoded key: {}", decodedKey);
        }

        // 2) Decode base64 value -> JSON -> Dish
        String valueB64 = record.getValue();
        if (valueB64 == null || valueB64.isEmpty()) {
            log.warn("Empty record value. offset={}, partition={}", record.getOffset(), record.getPartition());
            return null;
        }
        byte[] valBytes = Base64.getDecoder().decode(valueB64);
        String json = new String(valBytes, StandardCharsets.UTF_8);
        log.debug("Decoded JSON: {}", json);

        // 3) Convert JSON -> Dish
        Dish dish = MAPPER.readValue(json, Dish.class);
        return dish;
    }
}
