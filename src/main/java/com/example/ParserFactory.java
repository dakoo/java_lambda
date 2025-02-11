package com.example;

public class ParserFactory {
    public static ParserInterface<?> createParser(String parserName) {
        switch (parserName) {
            case "DishParser":
                // returns a parser that decodes base64 -> JSON -> Dish
                return new DishJsonBase64Parser();

            // Example: if you have another model "Drink", you might do:
            // case "DrinkParser":
            //     return new DrinkJsonBase64Parser();

            default:
                throw new IllegalArgumentException("Unknown parserName: " + parserName);
        }
    }
}
