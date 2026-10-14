/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.metrics;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.strimzi.api.kafka.model.common.CertAndKeySecretSource;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.HasConfigurableMetrics;
import io.strimzi.api.kafka.model.common.metrics.OtlpAuthenticationTls;
import io.strimzi.api.kafka.model.common.metrics.OtlpResourceAttribute;
import io.strimzi.api.kafka.model.common.metrics.StrimziMetricsReporter;
import io.strimzi.api.kafka.model.common.metrics.StrimziMetricsReporterOtlp;
import io.strimzi.operator.cluster.model.ContainerUtils;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.model.InvalidResourceException;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Model for the Strimzi Metrics Reporter Kafka plugin.
 */
public class StrimziMetricsReporterModel implements MetricsModel {
    /**
     * Prefix of the environment variables holding the values of the OTLP resource attributes taken from the pod.
     * The index of the attribute in the list is appended.
     */
    public static final String ENV_VAR_RESOURCE_ATTRIBUTE_PREFIX = "STRIMZI_OTLP_RESOURCE_ATTRIBUTE_";

    /**
     * Key of the trusted certificates in the internal Secret aggregating the OTLP trusted certificates.
     */
    public static final String OTLP_TRUSTED_CERTS_KEY = "ca.crt";

    private static final String PLACEHOLDER_SECRET_TEMPLATE_KUBE_CONFIG_PROVIDER = "${strimzisecrets:%s/%s:%s}";

    /**
     * The pod fields supported by the Downward API for environment variables.
     */
    private static final Pattern DOWNWARD_API_FIELD_PATH = Pattern.compile(
            "^(metadata\\.name|metadata\\.namespace|metadata\\.uid"
            + "|metadata\\.labels\\['[^']+'\\]|metadata\\.annotations\\['[^']+'\\]"
            + "|spec\\.nodeName|spec\\.serviceAccountName"
            + "|status\\.hostIP|status\\.hostIPs|status\\.podIP|status\\.podIPs)$");

    /**
     * The user-configured allow list, or {@code null} when the user did not set one.
     */
    private final List<String> allowList;
    private final boolean prometheusEndpointEnabled;
    private final StrimziMetricsReporterOtlp otlp;

    /**
     * Stores only the user-configured allow list; the default is resolved later at the call site, as it can
     * depend on the node's role.
     *
     * @param spec Custom resource section configuring metrics.
     */
    public StrimziMetricsReporterModel(HasConfigurableMetrics spec) {
        if (spec.getMetricsConfig() != null) {
            StrimziMetricsReporter config = (StrimziMetricsReporter) spec.getMetricsConfig();
            validate(config);
            this.allowList = config.getValues() != null ? config.getValues().getAllowList() : null;
            this.prometheusEndpointEnabled = config.getValues() == null
                    || config.getValues().getPrometheusEndpoint() == null
                    || config.getValues().getPrometheusEndpoint().getEnabled() == null
                    || config.getValues().getPrometheusEndpoint().getEnabled();
            this.otlp = config.getValues() != null ? config.getValues().getOtlp() : null;
        } else {
            throw new InvalidConfigurationException("Unexpected empty metrics config");
        }
    }

    /**
     * Gets the name of the internal Secret aggregating the trusted certificates of the OTLP endpoint.
     *
     * @param componentName Name of the component, for example my-cluster-kafka
     *
     * @return Name of the Secret
     */
    public static String otlpTrustedCertsSecretName(String componentName) {
        return componentName + "-otlp-trusted-certs";
    }

    /**
     * Gets the comma-separated list of allow regex expressions, falling back to the given default when the user
     * did not configure one.
     *
     * @param defaultAllowList Default allow list to be used when the user did not configure one.
     * @return Comma separated list of allow regex expressions.
     */
    public String getAllowListOrDefault(List<String> defaultAllowList) {
        return String.join(",", allowList != null ? allowList : defaultAllowList);
    }

    @Override
    public boolean isPrometheusEndpointEnabled() {
        return prometheusEndpointEnabled;
    }

    /**
     * Whether the metrics are pushed to an OTLP endpoint.
     *
     * @return True when the metrics are pushed to an OTLP endpoint. False otherwise.
     */
    public boolean isOtlpEnabled() {
        return otlp != null;
    }

    /**
     * Gets the trusted certificates configured for the OTLP endpoint.
     *
     * @return The trusted certificates, or an empty list when none are configured.
     */
    public List<CertSecretSource> otlpTrustedCertificates() {
        if (otlp != null && otlp.getTls() != null && otlp.getTls().getTrustedCertificates() != null) {
            return otlp.getTls().getTrustedCertificates();
        }
        return List.of();
    }

    /**
     * Gets the client certificate and key used for mTLS with the OTLP endpoint.
     *
     * @return The client certificate and key, or null when mTLS is not configured.
     */
    public CertAndKeySecretSource otlpCertificateAndKey() {
        if (otlp != null && otlp.getAuthentication() instanceof OtlpAuthenticationTls tlsAuth) {
            return tlsAuth.getCertificateAndKey();
        }
        return null;
    }

    /**
     * Whether TLS certificates are configured for the OTLP endpoint, either trusted certificates or a client
     * certificate.
     *
     * @return True when TLS certificates are configured. False otherwise.
     */
    public boolean isOtlpTlsConfigured() {
        return !otlpTrustedCertificates().isEmpty() || otlpCertificateAndKey() != null;
    }

    /**
     * Gets the names of the Secrets the pods need to read through the Kubernetes API, using the
     * KubernetesSecretConfigProvider, to load the OTLP TLS certificates.
     *
     * @param componentName Name of the component, used for the name of the internal trusted certificates Secret
     *
     * @return Set with the Secret names, empty when no TLS certificates are configured.
     */
    public Set<String> otlpSecretsToAccess(String componentName) {
        Set<String> secretNames = new LinkedHashSet<>();
        if (!otlpTrustedCertificates().isEmpty()) {
            secretNames.add(otlpTrustedCertsSecretName(componentName));
        }
        if (otlpCertificateAndKey() != null) {
            secretNames.add(otlpCertificateAndKey().getSecretName());
        }
        return secretNames;
    }

    /**
     * Gets the environment variables holding the values of the OTLP resource attributes taken from the pod. They
     * are populated from the Downward API, so a single configuration works for all the pods of a component.
     *
     * @return List of environment variables, empty when no resource attribute uses valueFrom.
     */
    public List<EnvVar> envVars() {
        List<EnvVar> envVars = new ArrayList<>();
        List<OtlpResourceAttribute> attributes = resourceAttributes();
        for (int i = 0; i < attributes.size(); i++) {
            OtlpResourceAttribute attribute = attributes.get(i);
            if (attribute.getValueFrom() != null) {
                envVars.add(ContainerUtils.createEnvVarFromFieldRef(ENV_VAR_RESOURCE_ATTRIBUTE_PREFIX + i,
                        attribute.getValueFrom().getFieldRef().getFieldPath()));
            }
        }
        return envVars;
    }

    /**
     * Gets the OTLP configuration of the Strimzi Metrics Reporter. The keys don't have the reporter prefix, because
     * the HTTP Bridge uses the same keys with its own prefix. Unset values are left out, so that the standard
     * OpenTelemetry configuration applies to them. The TLS certificates are referenced with placeholders of the
     * KubernetesSecretConfigProvider, so they are not stored in the configuration.
     *
     * @param namespace     Namespace of the pods and of the Secrets
     * @param componentName Name of the component, used for the name of the internal trusted certificates Secret
     *
     * @return Map with the OTLP configuration, empty when OTLP is not enabled.
     */
    public Map<String, String> otlpConfiguration(String namespace, String componentName) {
        Map<String, String> configuration = new LinkedHashMap<>();
        if (!isOtlpEnabled()) {
            return configuration;
        }

        configuration.put(StrimziMetricsReporterConfig.OTLP_ENABLE, "true");
        if (otlp.getEndpoint() != null) {
            configuration.put(StrimziMetricsReporterConfig.OTLP_ENDPOINT, otlp.getEndpoint());
        }
        if (otlp.getProtocol() != null) {
            configuration.put(StrimziMetricsReporterConfig.OTLP_PROTOCOL, otlp.getProtocol().toValue());
        }
        if (otlp.getIntervalSeconds() != null) {
            configuration.put(StrimziMetricsReporterConfig.OTLP_INTERVAL_SECONDS, String.valueOf(otlp.getIntervalSeconds()));
        }
        if (!resourceAttributes().isEmpty()) {
            configuration.put(StrimziMetricsReporterConfig.OTLP_RESOURCE_ATTRIBUTES, resourceAttributesConfiguration());
        }
        if (!otlpTrustedCertificates().isEmpty()) {
            configuration.put(StrimziMetricsReporterConfig.OTLP_SSL_TRUSTSTORE_CERTIFICATES,
                    String.format(PLACEHOLDER_SECRET_TEMPLATE_KUBE_CONFIG_PROVIDER, namespace, otlpTrustedCertsSecretName(componentName), OTLP_TRUSTED_CERTS_KEY));
        }
        CertAndKeySecretSource certificateAndKey = otlpCertificateAndKey();
        if (certificateAndKey != null) {
            configuration.put(StrimziMetricsReporterConfig.OTLP_SSL_KEYSTORE_CERTIFICATE_CHAIN,
                    String.format(PLACEHOLDER_SECRET_TEMPLATE_KUBE_CONFIG_PROVIDER, namespace, certificateAndKey.getSecretName(), certificateAndKey.getCertificate()));
            configuration.put(StrimziMetricsReporterConfig.OTLP_SSL_KEYSTORE_KEY,
                    String.format(PLACEHOLDER_SECRET_TEMPLATE_KUBE_CONFIG_PROVIDER, namespace, certificateAndKey.getSecretName(), certificateAndKey.getKey()));
        }

        return configuration;
    }

    private List<OtlpResourceAttribute> resourceAttributes() {
        return otlp != null && otlp.getResourceAttributes() != null ? otlp.getResourceAttributes() : List.of();
    }

    /**
     * Builds the resource attributes configured by the user. The static values are percent-encoded, so they can
     * contain the comma and equals separators. The values taken from the pod are referenced with placeholders of
     * the EnvVarConfigProvider, matching the environment variables returned by {@link #envVars()}.
     */
    private String resourceAttributesConfiguration() {
        List<String> entries = new ArrayList<>();
        List<OtlpResourceAttribute> attributes = resourceAttributes();
        for (int i = 0; i < attributes.size(); i++) {
            OtlpResourceAttribute attribute = attributes.get(i);
            String value = attribute.getValueFrom() != null
                    ? "${strimzienv:" + ENV_VAR_RESOURCE_ATTRIBUTE_PREFIX + i + "}"
                    : encode(attribute.getValue());
            entries.add(encode(attribute.getName()) + "=" + value);
        }
        return String.join(",", entries);
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    /**
     * Validates user configuration.
     *
     * @param config Config to be validated.
     *
     */
    /* test */ static void validate(StrimziMetricsReporter config) {
        List<String> errors = new ArrayList<>();
        if (config.getValues() != null && config.getValues().getAllowList() != null) {
            if (config.getValues().getAllowList().isEmpty()) {
                errors.add("Allowlist should contain at least one element");
            }

            for (String regex : config.getValues().getAllowList()) {
                try {
                    Pattern.compile(regex);
                } catch (PatternSyntaxException pse) {
                    errors.add(String.format("Invalid regex: %s, %s", regex, pse.getDescription()));
                }
            }
        }

        if (config.getValues() != null) {
            if (config.getValues().getPrometheusEndpoint() != null
                    && Boolean.FALSE.equals(config.getValues().getPrometheusEndpoint().getEnabled())
                    && config.getValues().getOtlp() == null) {
                errors.add("otlp must be configured when the Prometheus endpoint is disabled");
            }

            StrimziMetricsReporterOtlp otlp = config.getValues().getOtlp();
            if (otlp != null) {
                if (otlp.getEndpoint() != null
                        && !otlp.getEndpoint().startsWith("http://")
                        && !otlp.getEndpoint().startsWith("https://")) {
                    errors.add("OTLP endpoint must start with http:// or https://");
                }

                if (otlp.getIntervalSeconds() != null && otlp.getIntervalSeconds() < 1) {
                    errors.add("OTLP interval must be at least 1 second");
                }

                if (otlp.getResourceAttributes() != null) {
                    for (OtlpResourceAttribute attribute : otlp.getResourceAttributes()) {
                        validateResourceAttribute(attribute, errors);
                    }
                }
            }
        }

        if (!errors.isEmpty()) {
            throw new InvalidResourceException("Metrics configuration is invalid: " + errors);
        }
    }

    private static void validateResourceAttribute(OtlpResourceAttribute attribute, List<String> errors) {
        if (attribute.getName() == null || attribute.getName().isBlank()) {
            errors.add("OTLP resource attribute name is required");
            return;
        }

        boolean hasValue = attribute.getValue() != null;
        boolean hasValueFrom = attribute.getValueFrom() != null;
        if (hasValue == hasValueFrom) {
            errors.add(String.format("OTLP resource attribute %s must have either value or valueFrom", attribute.getName()));
        } else if (hasValueFrom) {
            String fieldPath = attribute.getValueFrom().getFieldRef() != null ? attribute.getValueFrom().getFieldRef().getFieldPath() : null;
            if (fieldPath == null || !DOWNWARD_API_FIELD_PATH.matcher(fieldPath).matches()) {
                errors.add(String.format("OTLP resource attribute %s uses an unsupported fieldPath: %s", attribute.getName(), fieldPath));
            }
        }
    }
}
