package com.example.model;

import com.example.annotations.PartitionKey;
import com.example.annotations.VersionKey;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * The internal model for writing to DynamoDB,
 * annotated with PartitionKey and VersionKey.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ItemCatalog {

    @VersionKey
    private Long version;

    @PartitionKey
    private Long productId;

    private String divisionType;
    private Map<String, String> name;
    private Map<String, Map<String, Map<String, String>>> reconciledAttributes;
    private Boolean valid;
    private Long createAt;
    private Long sequence;
    private String mainImage;
}
