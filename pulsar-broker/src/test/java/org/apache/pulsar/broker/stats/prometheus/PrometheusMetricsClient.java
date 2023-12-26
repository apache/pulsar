package org.apache.pulsar.broker.stats.prometheus;

import static org.testng.Assert.assertTrue;
import com.google.common.base.MoreObjects;
import com.google.common.base.Splitter;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.restassured.RestAssured;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PrometheusMetricsClient {
    private final String host;
    private final int port;

    public PrometheusMetricsClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @SuppressWarnings("HttpUrlsUsage")
    public Metrics getMetrics() {
        String metrics = RestAssured.given().baseUri("http://" + host).port(port).get("/metrics").asString();
        return new Metrics(parseMetrics(metrics));
    }

    /**
     * Hacky parsing of Prometheus text format. Should be good enough for unit tests
     */
    public static Multimap<String, Metric> parseMetrics(String metrics) {
        Multimap<String, Metric> parsed = ArrayListMultimap.create();

        // Example of lines are
        // jvm_threads_current{cluster="standalone",} 203.0
        // or
        // pulsar_subscriptions_count{cluster="standalone", namespace="public/default",
        // topic="persistent://public/default/test-2"} 0.0
        Pattern pattern = Pattern.compile("^(\\w+)\\{([^\\}]+)\\}\\s([+-]?[\\d\\w\\.-]+)$");
        Pattern tagsPattern = Pattern.compile("(\\w+)=\"([^\"]+)\"(,\\s?)?");

        Splitter.on("\n").split(metrics).forEach(line -> {
            if (line.isEmpty() || line.startsWith("#")) {
                return;
            }

            Matcher matcher = pattern.matcher(line);
            assertTrue(matcher.matches(), "line " + line + " does not match pattern " + pattern);
            String name = matcher.group(1);

            Metric m = new Metric();
            String numericValue = matcher.group(3);
            if (numericValue.equalsIgnoreCase("-Inf")) {
                m.value = Double.NEGATIVE_INFINITY;
            } else if (numericValue.equalsIgnoreCase("+Inf")) {
                m.value = Double.POSITIVE_INFINITY;
            } else {
                m.value = Double.parseDouble(numericValue);
            }
            String tags = matcher.group(2);
            Matcher tagsMatcher = tagsPattern.matcher(tags);
            while (tagsMatcher.find()) {
                String tag = tagsMatcher.group(1);
                String value = tagsMatcher.group(2);
                m.tags.put(tag, value);
            }

            parsed.put(name, m);
        });

        return parsed;
    }

    public static class Metric {
        public Map<String, String> tags = new TreeMap<>();
        public double value;

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("tags", tags).add("value", value).toString();
        }

        public boolean contains(String labelName, String labelValue) {
            String value = tags.get(labelName);
            return value != null && value.equals(labelValue);
        }
    }

    public static class Metrics {
        final Multimap<String, Metric> nameToDataPoints;

        public Metrics(Multimap<String, Metric> nameToDataPoints) {
            this.nameToDataPoints = nameToDataPoints;
        }

        public List<Metric> findByNameAndLabels(String metricName, String labelName, String labelValue) {
            return nameToDataPoints.get(metricName)
                    .stream()
                    .filter(metric -> metric.contains(labelName, labelValue))
                    .toList();
        }
    }
}
