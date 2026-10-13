/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.metrics;

/**
 * The metrics model.
 */
public interface MetricsModel {
    /**
     * Metrics endpoint port name.
     */
    String METRICS_PORT_NAME = "tcp-prometheus";

    /**
     * Metrics endpoint port number.
     */
    int METRICS_PORT = 9404;

    /**
     * Whether the metrics are exposed on an endpoint in the Prometheus format, which needs the metrics port and the
     * network policy rule.
     *
     * @return True when the metrics are exposed on a Prometheus endpoint. False otherwise.
     */
    default boolean isPrometheusEndpointEnabled() {
        return true;
    }
}