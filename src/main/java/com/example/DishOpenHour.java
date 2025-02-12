package com.example;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class DishOpenHour {
    private Long id;
    private Long dishId;
    private String dayOfWeek;
    private Integer fromHour;
    private Integer fromMinute;
    private Integer toHour;
    private Integer toMinute;
}
