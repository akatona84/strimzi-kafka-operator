/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.metrics;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.CelValidation;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Strimzi Metrics Reporter configuration.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"allowList", "prometheusEndpoint", "otlp"})
@CelValidation(rules = {
    @CelValidation.CelValidationRule(
        // At least one output has to be enabled
        rule = "!has(self.prometheusEndpoint) || !has(self.prometheusEndpoint.enabled) || self.prometheusEndpoint.enabled || has(self.otlp)",
        message = "otlp must be configured when the Prometheus endpoint is disabled"
        )
})
@EqualsAndHashCode
@ToString
public class StrimziMetricsReporterValues implements UnknownPropertyPreserving {
    private List<String> allowList;
    private StrimziMetricsReporterPrometheusEndpoint prometheusEndpoint;
    private StrimziMetricsReporterOtlp otlp;
    private Map<String, Object> additionalProperties;

    @Description("A list of regex patterns to filter the metrics to collect. Should contain at least one element.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public List<String> getAllowList() {
        return allowList;
    }

    public void setAllowList(List<String> allowList) {
        this.allowList = allowList;
    }

    @Description("Configures the endpoint exposing the metrics in the Prometheus format. " +
            "When not set, the endpoint is enabled.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public StrimziMetricsReporterPrometheusEndpoint getPrometheusEndpoint() {
        return prometheusEndpoint;
    }

    public void setPrometheusEndpoint(StrimziMetricsReporterPrometheusEndpoint prometheusEndpoint) {
        this.prometheusEndpoint = prometheusEndpoint;
    }

    @Description("Enables and configures pushing the metrics to an OTLP endpoint, such as an OpenTelemetry Collector. " +
            "The same metrics, filtered by the same `allowList`, are pushed and exposed on the Prometheus endpoint.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public StrimziMetricsReporterOtlp getOtlp() {
        return otlp;
    }

    public void setOtlp(StrimziMetricsReporterOtlp otlp) {
        this.otlp = otlp;
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
