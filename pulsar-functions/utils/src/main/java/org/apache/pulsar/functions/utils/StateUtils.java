package org.apache.pulsar.functions.utils;

/**
 * Utils for state store.
 */
public final class StateUtils {

    private StateUtils() {}

    /**
     * Convert pulsar tenant and namespace to state storage namespace.
     *
     * @param tenant pulsar tenant
     * @param namespace pulsar namespace
     * @return state storage namespace
     */
    public static String getStateNamespace(String tenant, String namespace) {
        return String.format("%s_%s", tenant, namespace)
            .replace("-", "_");
    }

}
