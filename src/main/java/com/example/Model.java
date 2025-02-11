package com.example;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Example model class using Lombok.
 * Lombok will generate getters, setters, no-args constructor,
 * all-args constructor, a builder, and toString()/equals()/hashCode().
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Model {

    // The partition key
    private String id;

    // A global version for the entire record
    private int version;

    // Example data fields
    private String dataField;
    private String description;
    private Double amount;
}
