package com.example;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;

import java.util.List;
import java.util.Map;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class DishOptionItem {
    private Long id;
    private Long optionId;
    private Map<String, String> names;
    private Boolean quantityChangeable;
    private Integer maxQuantity;
    private Integer minQuantity;
    private String type;
    private Double salePrice;
    private String currencyType;
    private String taxBaseType;
    private String restrictionType;
    private String saleFromAt;
    private String saleToAt;
    private String targetAvailableTime;
    private Boolean deleted;
    private Boolean disposable;
    private Double disposablePrice;
    private Double displayPrice;
}
