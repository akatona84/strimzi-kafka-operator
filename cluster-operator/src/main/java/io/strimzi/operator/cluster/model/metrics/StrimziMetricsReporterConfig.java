/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.metrics;

/**
 * This class holds Strimzi Metrics Reporter and related configuration keys.
 */
public class StrimziMetricsReporterConfig {
    /** KafkaMetricsReporter fully qualified class name. */
    public static final String KAFKA_CLASS = "io.strimzi.kafka.metrics.KafkaPrometheusMetricsReporter";

    /** ClientMetricsReporter fully qualified class name (for Kafka clients). */
    public static final String CLIENT_CLASS = "io.strimzi.kafka.metrics.prometheus.ClientMetricsReporter";

    /** ServerKafkaMetricsReporter fully qualified class name (for Kafka servers). */
    public static final String SERVER_KAFKA_CLASS = "io.strimzi.kafka.metrics.prometheus.ServerKafkaMetricsReporter";

    /** ServerYammerMetricsReporter fully qualified class name (for Kafka servers). */
    public static final String SERVER_YAMMER_CLASS = "io.strimzi.kafka.metrics.prometheus.ServerYammerMetricsReporter";

    /** Enable HTTP listener (true/false). */
    public static final String LISTENER_ENABLE = "prometheus.metrics.reporter.listener.enable";

    /** Set listener endpoint (e.g. http://:8080). */
    public static final String LISTENER = "prometheus.metrics.reporter.listener";

    /** Set a comma separated list of regexes. */
    public static final String ALLOW_LIST = "prometheus.metrics.reporter.allowlist";

    /** Prefix of the Strimzi Metrics Reporter configurations. */
    public static final String PREFIX = "prometheus.metrics.reporter.";

    /*
     * The OTLP configurations are defined without the prefix, because the HTTP Bridge uses the same names with
     * its own bridge.metrics. prefix.
     */

    /** Enable pushing metrics to an OTLP endpoint (true/false). */
    public static final String OTLP_ENABLE = "otlp.enable";

    /** Set the OTLP endpoint (e.g. http://my-collector:4317). */
    public static final String OTLP_ENDPOINT = "otlp.endpoint";

    /** Set the OTLP transport protocol (grpc or http/protobuf). */
    public static final String OTLP_PROTOCOL = "otlp.protocol";

    /** Set the interval in seconds between two pushes. */
    public static final String OTLP_INTERVAL_SECONDS = "otlp.interval.seconds";

    /** Set a comma separated list of key=value resource attributes. */
    public static final String OTLP_RESOURCE_ATTRIBUTES = "otlp.resource.attributes";

    private StrimziMetricsReporterConfig() { }
}