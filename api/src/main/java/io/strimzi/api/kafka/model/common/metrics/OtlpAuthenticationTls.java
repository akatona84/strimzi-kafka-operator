/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.metrics;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.CertAndKeySecretSource;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Configures TLS client authentication (mTLS) for pushing metrics to an OTLP endpoint.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "certificateAndKey"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class OtlpAuthenticationTls extends OtlpAuthentication {
    public static final String TYPE_TLS = "tls";

    private CertAndKeySecretSource certificateAndKey;

    @Description("Must be `" + TYPE_TLS + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return TYPE_TLS;
    }

    @Description("Reference to the `Secret` which holds the client certificate and private key pair, in PEM format.")
    @JsonProperty(required = true)
    public CertAndKeySecretSource getCertificateAndKey() {
        return certificateAndKey;
    }

    public void setCertificateAndKey(CertAndKeySecretSource certificateAndKey) {
        this.certificateAndKey = certificateAndKey;
    }
}
