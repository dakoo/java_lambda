package com.example;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ItemCatalogDTO {
    private Long productId;
    private Long version;
    private String divisionType;
    private Map<String, String> name;
    private Map<String, Map<String, Map<String, String>>> reconciledAttributes;
    private Boolean valid;
    private Long createAt;
    private Long sequence;
    private String mainImage;
}
