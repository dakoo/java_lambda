package com.example;

public class ParserFactory {

    /**
     * Given the fully qualified class name of a parser (that implements ParserInterface<?>),
     * use reflection to instantiate it.
     */
    public static ParserInterface<?> createParser(String parserClassName) {
        try {
            Class<?> parserClass = Class.forName(parserClassName);
            Object parserInstance = parserClass.getDeclaredConstructor().newInstance();

            // Cast to ParserInterface<?> so we can return it
            return (ParserInterface<?>) parserInstance;

        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Parser class not found: " + parserClassName, e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate parser: " + parserClassName, e);
        }
    }
}
