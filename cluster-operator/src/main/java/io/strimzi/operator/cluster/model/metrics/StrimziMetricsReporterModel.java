/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.metrics;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.strimzi.api.kafka.model.common.HasConfigurableMetrics;
import io.strimzi.api.kafka.model.common.metrics.StrimziMetricsReporter;
import io.strimzi.api.kafka.model.common.metrics.StrimziMetricsReporterOtlp;
import io.strimzi.api.kafka.model.common.template.ContainerTemplate;
import io.strimzi.operator.cluster.model.ContainerUtils;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.model.InvalidResourceException;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

/**
 * Model for the Strimzi Metrics Reporter Kafka plugin.
 */
public class StrimziMetricsReporterModel implements MetricsModel {
    /**
     * Environment variable holding the pod name, referenced by the OTLP resource attributes.
     */
    public static final String ENV_VAR_POD_NAME = "STRIMZI_POD_NAME";

    /**
     * Environment variable holding the node name, referenced by the OTLP resource attributes.
     */
    public static final String ENV_VAR_NODE_NAME = "STRIMZI_NODE_NAME";

    /**
     * Environment variable used by OpenTelemetry to set the service name.
     */
    private static final String ENV_VAR_OTEL_SERVICE_NAME = "OTEL_SERVICE_NAME";
    private static final String SERVICE_NAME = "service.name";

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
     * Gets the environment variables referenced by the OTLP resource attributes. They are populated from the
     * Downward API, so a single configuration works for all the pods of a component.
     *
     * @return List of environment variables, empty when OTLP is not enabled.
     */
    public List<EnvVar> envVars() {
        if (!isOtlpEnabled()) {
            return List.of();
        }
        return List.of(
                ContainerUtils.createEnvVarFromFieldRef(ENV_VAR_POD_NAME, "metadata.name"),
                ContainerUtils.createEnvVarFromFieldRef(ENV_VAR_NODE_NAME, "spec.nodeName"));
    }

    /**
     * Gets the OTLP configuration of the Strimzi Metrics Reporter. The keys don't have the reporter prefix, because
     * the HTTP Bridge uses the same keys with its own prefix. Unset values are left out, so that the standard
     * OpenTelemetry configuration applies to them.
     *
     * @param namespace         Namespace of the pods
     * @param serviceName       Name of the custom resource, used as the service.name resource attribute
     * @param containerName     Name of the container
     * @param containerTemplate Template of the container, used to check whether the user set OTEL_SERVICE_NAME
     *
     * @return Map with the OTLP configuration, empty when OTLP is not enabled.
     */
    public Map<String, String> otlpConfiguration(String namespace, String serviceName, String containerName, ContainerTemplate containerTemplate) {
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
        configuration.put(StrimziMetricsReporterConfig.OTLP_RESOURCE_ATTRIBUTES,
                resourceAttributes(namespace, serviceName, containerName, isOtelServiceNameSet(containerTemplate)));

        return configuration;
    }

    /**
     * Builds the resource attributes following the OpenTelemetry semantic conventions for Kubernetes. The values only
     * known inside the pod are referenced with placeholders of the EnvVarConfigProvider. The user attributes are
     * percent-encoded, so they can contain the comma and equals separators.
     */
    private String resourceAttributes(String namespace, String serviceName, String containerName, boolean otelServiceNameSet) {
        String podName = "${strimzienv:" + ENV_VAR_POD_NAME + "}";

        Map<String, String> attributes = new LinkedHashMap<>();
        attributes.put("k8s.namespace.name", namespace);
        attributes.put("k8s.pod.name", podName);
        attributes.put("k8s.container.name", containerName);
        attributes.put("k8s.node.name", "${strimzienv:" + ENV_VAR_NODE_NAME + "}");
        if (!otelServiceNameSet) {
            // Otherwise the metrics share the service name configured by the user for tracing
            attributes.put(SERVICE_NAME, serviceName);
        }
        attributes.put("service.namespace", namespace);
        attributes.put("service.instance.id", namespace + "." + podName + "." + containerName);

        if (otlp.getResourceAttributes() != null) {
            otlp.getResourceAttributes().forEach((key, value) -> attributes.put(encode(key), encode(value)));
        }

        return attributes.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(","));
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private static boolean isOtelServiceNameSet(ContainerTemplate containerTemplate) {
        return containerTemplate != null
                && containerTemplate.getEnv() != null
                && containerTemplate.getEnv().stream().anyMatch(env -> ENV_VAR_OTEL_SERVICE_NAME.equals(env.getName()));
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
            }
        }

        if (!errors.isEmpty()) {
            throw new InvalidResourceException("Metrics configuration is invalid: " + errors);
        }
    }
}
