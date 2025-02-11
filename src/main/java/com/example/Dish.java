package com.example;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;

import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Dish {
    private Long id;             // partition key
    private Long version;        // concurrency version
    private Long storeId;
    private Map<String, String> names;
    private Map<String, String> descriptions;
    private String taxBaseType;
    private String displayStatus;
    private String targetAvailableTime;
    private Double salePrice;
    private String currencyType;
    private List<String> imagePaths;
    private String saleFromAt;
    private String saleToAt;
    private List<DishOption> dishOptions;
    private List<DishOpenHour> openHours;
    private Boolean disposable;
    private Double disposablePrice;
    private Boolean deleted;
    private Double displayPrice;
}
