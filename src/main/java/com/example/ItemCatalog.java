package com.example;

import com.example.annotations.PartitionKey;
import com.example.annotations.VersionKey;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ItemCatalog {
    @VersionKey
    private Long version;
    @PartitionKey
    private Long itemId;
    private Long productId;
    private String divisionType;
    private Map<String, String> name;
    private Map<String, Map<String, Map<String, String>>> reconciledAttributes;
    private Boolean valid;
    private Long createdAt;
    private Long sequence;
    private String mainImage;
}
