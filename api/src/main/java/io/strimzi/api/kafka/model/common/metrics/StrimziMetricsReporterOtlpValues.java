/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.metrics;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.ClientTls;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configuration of the Strimzi Metrics Reporter pushing the metrics to an OTLP endpoint.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"allowList", "endpoint", "protocol", "intervalSeconds", "resourceAttributes", "tls", "authentication"})
@EqualsAndHashCode
@ToString
public class StrimziMetricsReporterOtlpValues implements UnknownPropertyPreserving {
    private List<String> allowList;
    private String endpoint;
    private OtlpProtocol protocol;
    private Integer intervalSeconds;
    private List<OtlpResourceAttribute> resourceAttributes;
    private ClientTls tls;
    private OtlpAuthentication authentication;
    private Map<String, Object> additionalProperties;

    @Description("A list of regex patterns to filter the metrics to collect. Should contain at least one element.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public List<String> getAllowList() {
        return allowList;
    }

    public void setAllowList(List<String> allowList) {
        this.allowList = allowList;
    }

    @Description("The OTLP endpoint the metrics are pushed to, in the `http://<HOST>:<PORT>` or `https://<HOST>:<PORT>` format, for example `http://my-collector:4317`. " +
            "With the `http/protobuf` protocol, the `/v1/metrics` path is appended. " +
            "When not set, the standard OpenTelemetry configuration applies, for example the `OTEL_EXPORTER_OTLP_ENDPOINT` environment variable set in the container template. " +
            "Only used with the `strimziMetricsReporterOtlp` type.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    @Description("The OTLP transport protocol, either `grpc` or `http/protobuf`. " +
            "When not set, the standard OpenTelemetry configuration applies, which defaults to `grpc`. " +
            "Only used with the `strimziMetricsReporterOtlp` type.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public OtlpProtocol getProtocol() {
        return protocol;
    }

    public void setProtocol(OtlpProtocol protocol) {
        this.protocol = protocol;
    }

    @Description("The interval in seconds between two pushes. " +
            "When not set, the standard OpenTelemetry configuration applies, which defaults to 60 seconds. " +
            "Only used with the `strimziMetricsReporterOtlp` type.")
    @Minimum(1)
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public Integer getIntervalSeconds() {
        return intervalSeconds;
    }

    public void setIntervalSeconds(Integer intervalSeconds) {
        this.intervalSeconds = intervalSeconds;
    }

    @Description("Resource attributes added to the pushed metrics, for example to identify the pod they come from. " +
            "Each attribute has a `name`, and either a static `value` or a `valueFrom` taking the value from a field of the pod. " +
            "No resource attributes are added by default. " +
            "Only used with the `strimziMetricsReporterOtlp` type.")
    @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
    public List<OtlpResourceAttribute> getResourceAttributes() {
        return resourceAttributes;
    }

    public void setResourceAttributes(List<OtlpResourceAttribute> resourceAttributes) {
        this.resourceAttributes = resourceAttributes;
    }

    @Description("TLS configuration for connecting to an `https://` OTLP endpoint. " +
            "When `trustedCertificates` is not set, the server certificate is verified against the default JVM trust store. " +
            "Only used with the `strimziMetricsReporterOtlp` type.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public ClientTls getTls() {
        return tls;
    }

    public void setTls(ClientTls tls) {
        this.tls = tls;
    }

    @Description("Authentication configuration for connecting to the OTLP endpoint. " +
            "Only used with the `strimziMetricsReporterOtlp` type.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public OtlpAuthentication getAuthentication() {
        return authentication;
    }

    public void setAuthentication(OtlpAuthentication authentication) {
        this.authentication = authentication;
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
