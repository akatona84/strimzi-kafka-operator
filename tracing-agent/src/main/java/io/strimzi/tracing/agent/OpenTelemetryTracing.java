/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.tracing.agent;

import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Distributed tracing initialization based on OpenTelemetry
 */
public class OpenTelemetryTracing implements Tracing {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenTelemetryTracing.class);

    /**
     * Constructor
     */
    public OpenTelemetryTracing() { }

    @Override
    public void initialize() {
        String serviceName = System.getenv("OTEL_SERVICE_NAME");
        if (serviceName != null) {
            LOGGER.info("Initializing OpenTelemetry tracing with service name {}", serviceName);
            AutoConfiguredOpenTelemetrySdk.builder()
                    // Disables the metrics and logs exporters of the tracing SDK only. This has no effect on metrics and
                    // logging in Strimzi. Setting them as system properties would also disable other OpenTelemetry SDKs
                    // in the JVM, such as the one used by the Strimzi Metrics Reporter to push metrics with OTLP.
                    .addPropertiesCustomizer(config -> Map.of(
                            "otel.metrics.exporter", "none",
                            "otel.logs.exporter", "none"))
                    .setResultAsGlobal()
                    .build();
        } else {
            LOGGER.error("OpenTelemetry tracing cannot be initialized because OTEL_SERVICE_NAME environment variable is not defined");
        }
    }
}
