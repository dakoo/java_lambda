package com.example;

import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import com.example.ParserInterface;
import com.example.model.ItemCatalog;
import com.example.ItemCatalogDTO; // Your Avro-generated class
import com.example.AvroSpecificRecordSerializer; // Hypothetical serializer
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * A parser that:
 * 1. Base64-decodes the Kafka record value.
 * 2. Deserializes Avro bytes -> ItemCatalogDTO (generated class).
 * 3. Maps fields from ItemCatalogDTO -> ItemCatalog (plain model).
 */
@Slf4j
public class ItemCatalogParser implements ParserInterface<ItemCatalog> {

    private final AvroSpecificRecordSerializer serializer;

    public ItemCatalogParser(AvroSpecificRecordSerializer serializer) {
        this.serializer = serializer;
    }

    @Override
    public ItemCatalog parseRecord(KafkaEvent.KafkaEventRecord record) throws Exception {
        // 1) Base64 decode
        String base64Value = record.getValue();
        if (base64Value == null || base64Value.isEmpty()) {
            log.warn("Empty record value. offset={}, partition={}", record.getOffset(), record.getPartition());
            return null;
        }
        byte[] avroBytes = Base64.getDecoder().decode(base64Value.getBytes(StandardCharsets.UTF_8));

        // 2) Deserialize Avro -> ItemCatalogDTO
        ItemCatalogDTO dto = serializer.deserialize(avroBytes, ItemCatalogDTO.class);

        // 3) Convert Avro DTO -> ItemCatalog
        ItemCatalog model = new ItemCatalog();
        model.setVersion(dto.getVersion());
        model.setProductId(dto.getProductId());
        model.setDivisionType(dto.getDivisionType());
        model.setName(dto.getName());
        model.setReconciledAttributes(dto.getReconciledAttributes());
        model.setValid(dto.getValid());
        model.setCreateAt(dto.getCreateAt());
        model.setSequence(dto.getSequence());
        model.setMainImage(dto.getMainImage());

        return model;
    }
}
