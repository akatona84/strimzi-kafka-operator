/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.metrics;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Strimzi Metrics Reporter pushing the metrics to an OTLP endpoint.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonPropertyOrder({"type", "values"})
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class StrimziMetricsReporterOtlp extends MetricsConfig {
    public static final String TYPE_STRIMZI_METRICS_REPORTER_OTLP = "strimziMetricsReporterOtlp";

    private StrimziMetricsReporterOtlpValues values;

    @Description("Must be `" + TYPE_STRIMZI_METRICS_REPORTER_OTLP + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return TYPE_STRIMZI_METRICS_REPORTER_OTLP;
    }

    // Same description as for the strimziMetricsReporter type, because the CRD merges the values of both types
    @Description("Configuration values for the Strimzi Metrics Reporter.")
    public StrimziMetricsReporterOtlpValues getValues() {
        return values;
    }

    public void setValues(StrimziMetricsReporterOtlpValues values) {
        this.values = values;
    }
}
