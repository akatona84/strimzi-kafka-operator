/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.metrics;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.OneOf;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

/**
 * Resource attribute added to the metrics pushed to an OTLP endpoint.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"name", "value", "valueFrom"})
@OneOf({@OneOf.Alternative(@OneOf.Alternative.Property("value")), @OneOf.Alternative(@OneOf.Alternative.Property("valueFrom"))})
@EqualsAndHashCode
@ToString
public class OtlpResourceAttribute implements UnknownPropertyPreserving {
    private String name;
    private String value;
    private OtlpResourceAttributeSource valueFrom;
    private Map<String, Object> additionalProperties;

    @Description("The name of the resource attribute, for example `k8s.pod.name`.")
    @JsonProperty(required = true)
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Description("The value of the resource attribute.")
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Description("Reference to a field of the pod from which the value of the resource attribute is taken.")
    public OtlpResourceAttributeSource getValueFrom() {
        return valueFrom;
    }

    public void setValueFrom(OtlpResourceAttributeSource valueFrom) {
        this.valueFrom = valueFrom;
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
