package com.example;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;

import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class DishOption {
    private Long id;
    private Long storeId;
    private Map<String, String> names;
    private Map<String, String> descriptions;
    private String type;
    private String exposeStatus;
    private Long defaultDishOptionItemId;
    private List<DishOptionItem> dishOptionItems;
    private Integer maxQuantity;
    private Integer minQuantity;
    private Long minSelect;
    private Long maxSelect;
    private Boolean deleted;
}
