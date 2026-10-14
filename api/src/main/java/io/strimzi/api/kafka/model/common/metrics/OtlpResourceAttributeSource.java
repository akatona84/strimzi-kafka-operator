/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.metrics;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.ObjectFieldSelector;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

/**
 * Source of the value of a resource attribute added to the metrics pushed to an OTLP endpoint.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"fieldRef"})
@EqualsAndHashCode
@ToString
public class OtlpResourceAttributeSource implements UnknownPropertyPreserving {
    private ObjectFieldSelector fieldRef;
    private Map<String, Object> additionalProperties;

    @Description("Selects a field of the pod, in the same way as the Kubernetes Downward API for environment variables. " +
            "Supported fields are `metadata.name`, `metadata.namespace`, `metadata.uid`, `metadata.labels['<KEY>']`, `metadata.annotations['<KEY>']`, " +
            "`spec.nodeName`, `spec.serviceAccountName`, `status.hostIP`, `status.hostIPs`, `status.podIP` and `status.podIPs`.")
    @KubeLink(group = "core", version = "v1", kind = "objectfieldselector")
    @JsonProperty(required = true)
    public ObjectFieldSelector getFieldRef() {
        return fieldRef;
    }

    public void setFieldRef(ObjectFieldSelector fieldRef) {
        this.fieldRef = fieldRef;
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
