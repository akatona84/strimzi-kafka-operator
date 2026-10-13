/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.metrics;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.strimzi.crdgenerator.annotations.Pattern;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for pushing the metrics collected by the Strimzi Metrics Reporter to an OTLP endpoint.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"endpoint", "protocol", "intervalSeconds", "resourceAttributes"})
@EqualsAndHashCode
@ToString
public class StrimziMetricsReporterOtlp implements UnknownPropertyPreserving {
    private String endpoint;
    private OtlpProtocol protocol;
    private Integer intervalSeconds;
    private Map<String, String> resourceAttributes;
    private Map<String, Object> additionalProperties;

    @Description("The OTLP endpoint the metrics are pushed to, for example `http://my-collector:4317`. " +
            "It must start with `http://` or `https://`. " +
            "With the `http/protobuf` protocol, the `/v1/metrics` path is appended. " +
            "When not set, the standard OpenTelemetry configuration applies, for example the `OTEL_EXPORTER_OTLP_ENDPOINT` environment variable set in the container template.")
    @Pattern("^https?://.+$")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    @Description("The OTLP transport protocol, either `grpc` or `http/protobuf`. " +
            "When not set, the standard OpenTelemetry configuration applies, which defaults to `grpc`.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public OtlpProtocol getProtocol() {
        return protocol;
    }

    public void setProtocol(OtlpProtocol protocol) {
        this.protocol = protocol;
    }

    @Description("The interval in seconds between two pushes. " +
            "When not set, the standard OpenTelemetry configuration applies, which defaults to 60 seconds.")
    @Minimum(1)
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public Integer getIntervalSeconds() {
        return intervalSeconds;
    }

    public void setIntervalSeconds(Integer intervalSeconds) {
        this.intervalSeconds = intervalSeconds;
    }

    @Description("Additional resource attributes added to the pushed metrics. " +
            "They override the attributes set by the operator, such as `service.name`, for the same keys.")
    @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
    public Map<String, String> getResourceAttributes() {
        return resourceAttributes;
    }

    public void setResourceAttributes(Map<String, String> resourceAttributes) {
        this.resourceAttributes = resourceAttributes;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : Map.of();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(2);
        }
        this.additionalProperties.put(name, value);
    }
}
