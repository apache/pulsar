package org.apache.pulsar.functions.utils;

public enum ComponentType {
    FUNCTION("Function"),
    SOURCE("Source"),
    SINK("Sink");

    private final String componentName;

    ComponentType(String componentName) {
        this.componentName = componentName;
    }

    @Override
    public String toString() {
        return componentName;
    }
}
