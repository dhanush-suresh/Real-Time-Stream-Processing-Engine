package com.file_system;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class Operators {
    public enum OperationType {
        FILTER,
        TRANSFORM,
        FILTERED_TRANSFORM,
        AGGREGATE,
        COLUMN_FILTER
    }

    private Map<String, Integer> aggregateState = new HashMap<>();
    private final String operationId;
    private final OperationType type;
    private Predicate<String> filterCondition;
    private String filterPattern;
    private Function<String, String> transformFunction;
    private Function<String[], String> selectFunction;
    private Function<String, String> keyExtractor;
    private Function<String, Integer> valueExtractor;
    private String aggregateFunction; // "sum", "count", "max", "min"

    // Constructor for Filter
    public Operators(String operationId, Predicate<String> filterCondition) {
        this.operationId = operationId;
        this.type = OperationType.FILTER;
        this.filterCondition = filterCondition;
        this.filterPattern = operationId.split(":")[1];
    }

    // Constructor for Transform
    public Operators(String operationId, Function<String, String> transformFunction) {
        this.operationId = operationId;
        this.type = OperationType.TRANSFORM;
        this.transformFunction = transformFunction;
    }

    // Constructor for FilteredTransform
    public Operators(String operationId, Predicate<String> filterCondition,
            Function<String, String> transformFunction) {
        this.operationId = operationId;
        this.type = OperationType.FILTERED_TRANSFORM;
        this.filterCondition = filterCondition;
        this.transformFunction = transformFunction;
    }

    // Constructor for Aggregate (Counter)
    public Operators(String operationId) {
        this.operationId = operationId;
        this.type = OperationType.AGGREGATE;
        this.aggregateState = new HashMap<>();
        // Initialize counter to 0
        this.aggregateState.put("count", 0);
    }

    // New constructor for Column Filter
    public Operators(String operationId, String columnName, String targetValue) {
        this.operationId = operationId;
        this.type = OperationType.COLUMN_FILTER;
        this.filterCondition = createColumnFilter(columnName, targetValue);
        this.filterPattern = columnName + ":" + targetValue;
    }

    public List<String> process(String input) {
        List<String> results = new ArrayList<>();

        switch (type) {
            case FILTER:
                if (filterCondition.test(input)) {
                    results.add(input);
                }
                break;

            case TRANSFORM:
                results.add(transformFunction.apply(input));
                break;

            case FILTERED_TRANSFORM:
                if (filterCondition.test(input)) {
                    results.add(transformFunction.apply(input));
                }
                break;

            case AGGREGATE:
                // Increment the counter and return the current count
                int currentCount = aggregateState.getOrDefault("count", 0) + 1;
                aggregateState.put("count", currentCount);
                // Return the current count as a result
                results.add(String.valueOf(currentCount));
                System.out.println("Running count at " + Thread.currentThread().getName() + ": " + currentCount);
                break;

            case COLUMN_FILTER:
                if (filterCondition.test(input)) {
                    results.add(input);
                }
                break;
        }

        return results;
    }

    public String getOperationId() {
        return operationId;
    }

    public OperationType getType() {
        return type;
    }

    // Helper method to create common filter operations
    public static Predicate<String> createFilter(String pattern) {
        System.out.println("Inside filter predicate");
        // Remove surrounding quotes if present
        System.out.println("pattern: " + pattern);
        String cleanPattern = pattern;
        if (pattern.startsWith("\"") && pattern.endsWith("\"")) {
            cleanPattern = pattern.substring(1, pattern.length() - 1);
        }
        System.out.println("After removing quotations: " + cleanPattern);
        final String finalPattern = cleanPattern;
        
        return input -> {
            // Case-insensitive pattern matching
            String lowerInput = input.toLowerCase();
            String lowerPattern = finalPattern.toLowerCase();
            boolean matches = lowerInput.contains(lowerPattern);
            if(matches) {
                System.out.println("Filtering line: " + input +
                        "\nPattern: " + lowerPattern +
                        "\nMatches: " + matches);
            }
            return matches;
        };
    }

    public static Function<String[], String> createSelect(String columns) {
        int[] columns_arr = Arrays.stream(columns.split(",")).mapToInt(Integer::parseInt).toArray();

        // Return a function that takes a String array and selects specified columns
        return row -> Arrays.stream(columns_arr)
        .filter(index -> index >= 0 && index < row.length) // Remove invalid indexes
        .mapToObj(index -> row[index]) // Get the column value
        .collect(Collectors.joining(",")); // Join selected values back into a string   
    }

    // Helper method to create common transform operations
    public static Function<String, String> createTransform(String transformType) {
        switch (transformType.toLowerCase()) {
            case "uppercase":
                return String::toUpperCase;
            case "lowercase":
                return String::toLowerCase;
            case "trim":
                return String::trim;
            case "splitintowords":
                return line -> Arrays.stream(line.split("\\s+"))
                        .collect(Collectors.joining("\n"));
            default:
                if (transformType.toLowerCase().startsWith("select:")) {
                    String columns = transformType.substring(7);  // Remove "select:"
                    System.out.println("Creating SELECT transform with columns: " + columns);
                    return input -> {
                        System.out.println("SELECT input: " + input);
                        String[] values = input.split(",");
                        System.out.println("Values array length: " + values.length);
                        StringBuilder result = new StringBuilder();
                        for (String index : columns.split(",")) {
                            int idx = Integer.parseInt(index);
                            System.out.println("Getting value at index " + idx);
                            if (result.length() > 0) {
                                result.append(",");
                            }
                            result.append(values[idx]);
                        }
                        System.out.println("SELECT result: " + result.toString());
                        return result.toString();
                    };
                }
                throw new IllegalArgumentException("Unknown transform type: " + transformType);
        }
    }

    public String serialize() {
        switch (type) {
            case FILTER:
                return this.operationId + "|" + this.type + "|FILTER:" + this.filterPattern;
            case TRANSFORM:
                if (this.operationId.startsWith("TRANSFORM:select:")) {
                    String[] parts = this.operationId.split(":");
                    return this.operationId + "|" + this.type + "|TRANSFORM:select:" + 
                        this.operationId.substring(this.operationId.lastIndexOf(":") + 1);
                } else {
                    return this.operationId + "|" + this.type + "|TRANSFORM:" + this.operationId.split(":")[1];
                }
            case FILTERED_TRANSFORM:
                return this.operationId + "|" + this.type + "|TRANSFORM:" + this.operationId.split(":")[2];
            case COLUMN_FILTER:
                return this.operationId + "|" + this.type + "|COLUMN_FILTER:" + this.filterPattern;
            case AGGREGATE:
                return this.operationId + "|" + this.type + "|AGGREGATE";
            default:
                return this.operationId + "|" + this.type;
        }
    }

    public static Operators deserialize(String data) {
        String[] parts = data.split("\\|");
        String id = parts[0];
        String type = parts[1];
        String pattern = parts[2];
        
        switch (OperationType.valueOf(type)) {
            case FILTER:
                String filterPattern = pattern.split(":")[1].trim();
                return new Operators(id, createFilter(filterPattern));
            case TRANSFORM:
                String transformType = pattern.split(":")[1].trim();
                if (transformType.startsWith("select")) {
                    String indices = pattern.substring(pattern.lastIndexOf(":") + 1).trim();
                    System.out.println("Deserializing SELECT transform with indices: " + indices);
                    return new Operators(id, createTransform("select:" + indices));
                } else {
                    return new Operators(id, createTransform(transformType));
                }
            case FILTERED_TRANSFORM:
                String transformOp = pattern.split(":")[1].trim();
                return new Operators(id, s -> true, createTransform(transformOp));
            case COLUMN_FILTER:
                String[] filterParts = pattern.split(":");
                if (filterParts.length >= 3) {
                    String columnName = filterParts[1].trim();
                    String targetValue = filterParts[2].trim();
                    return new Operators(id, columnName, targetValue);
                }
                throw new IllegalArgumentException("Invalid COLUMN_FILTER format");
            case AGGREGATE:
                return new Operators(id);
            default:
                throw new IllegalArgumentException("Unknown operator type: " + type);
        }
    }

    public String getPattern() {
        return filterPattern;
    }

    // Add new method for column filtering
    private static Predicate<String> createColumnFilter(String columnIndex, String targetValue) {
        try {
            int index = Integer.parseInt(columnIndex);
            return line -> {
                try {
                    String[] values = line.split(",");
                    if (index >= 0 && index < values.length) {
                        return values[index].trim().equals(targetValue);
                    }
                    return false;
                } catch (Exception e) {
                    System.err.println("Error processing line: " + line);
                    return false;
                }
            };
        } catch (NumberFormatException e) {
            System.err.println("Invalid column index: " + columnIndex);
            return line -> false;
        }
    }

}