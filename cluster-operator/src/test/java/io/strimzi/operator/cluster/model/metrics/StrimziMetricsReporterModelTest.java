/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.metrics;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.strimzi.api.kafka.model.common.metrics.OtlpProtocol;
import io.strimzi.api.kafka.model.common.metrics.StrimziMetricsReporter;
import io.strimzi.api.kafka.model.common.metrics.StrimziMetricsReporterBuilder;
import io.strimzi.api.kafka.model.common.template.ContainerEnvVarBuilder;
import io.strimzi.api.kafka.model.common.template.ContainerTemplate;
import io.strimzi.api.kafka.model.common.template.ContainerTemplateBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectSpecBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaClusterSpecBuilder;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.model.InvalidResourceException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class StrimziMetricsReporterModelTest {
    @Test
    public void testDisabled() {
        InvalidConfigurationException ex = assertThrows(InvalidConfigurationException.class, () -> new StrimziMetricsReporterModel(new KafkaConnectSpecBuilder().build()));
        assertThat(ex.getMessage(), is("Unexpected empty metrics config"));
    }

    @Test
    public void testEnabled() {
        StrimziMetricsReporter metricsConfig = new StrimziMetricsReporterBuilder()
                .withNewValues()
                    .withAllowList(List.of("kafka_log.*", "kafka_network.*"))
                .endValues()
                .build();
        StrimziMetricsReporterModel metrics = new StrimziMetricsReporterModel(new KafkaClusterSpecBuilder()
                .withMetricsConfig(metricsConfig).build());

        assertThat(metrics.getAllowListOrDefault(List.of(".*")).isEmpty(), is(false));
        assertThat(metrics.getAllowListOrDefault(List.of(".*")), is("kafka_log.*,kafka_network.*"));
    }

    @Test
    public void testDefaultUsedWhenNotConfigured() {
        StrimziMetricsReporterModel metrics = new StrimziMetricsReporterModel(new KafkaClusterSpecBuilder()
                .withMetricsConfig(new StrimziMetricsReporterBuilder().build()).build());

        assertThat(metrics.getAllowListOrDefault(List.of("kafka_server_raft.*", "kafka_cluster_partition.*")),
                is("kafka_server_raft.*,kafka_cluster_partition.*"));
    }

    @Test
    public void testValidation() {
        assertDoesNotThrow(() -> StrimziMetricsReporterModel.validate(new StrimziMetricsReporterBuilder()
                .withNewValues()
                    .withAllowList(List.of("kafka_log.*", "kafka_network.*"))
                .endValues()
                .build())
        );

        InvalidResourceException ise0 = assertThrows(InvalidResourceException.class, () -> StrimziMetricsReporterModel.validate(
                new StrimziMetricsReporterBuilder()
                        .withNewValues()
                            .withAllowList(List.of())
                        .endValues()
                        .build())
        );
        assertThat(ise0.getMessage(), is("Metrics configuration is invalid: [Allowlist should contain at least one element]"));

        InvalidResourceException ise1 = Assertions.assertThrows(InvalidResourceException.class, () -> StrimziMetricsReporterModel.validate(
                new StrimziMetricsReporterBuilder()
                        .withNewValues()
                            .withAllowList(List.of("kafka_network.*", "kafka_log.***", "[a+"))
                        .endValues()
                        .build())
        );
        assertThat(ise1.getMessage(), is("Metrics configuration is invalid: [Invalid regex: kafka_log.***, Dangling meta character '*', Invalid regex: [a+, Unclosed character class]"));
    }

    @Test
    public void testPrometheusEndpointEnabledByDefault() {
        StrimziMetricsReporterModel metrics = new StrimziMetricsReporterModel(new KafkaClusterSpecBuilder()
                .withMetricsConfig(new StrimziMetricsReporterBuilder().build()).build());

        assertThat(metrics.isPrometheusEndpointEnabled(), is(true));
        assertThat(metrics.isOtlpEnabled(), is(false));
        assertThat(metrics.envVars(), is(empty()));
        assertThat(metrics.otlpConfiguration("my-namespace", "my-cluster", "kafka", null), is(anEmptyMap()));
    }

    @Test
    public void testOtlpConfiguration() {
        StrimziMetricsReporterModel metrics = new StrimziMetricsReporterModel(new KafkaClusterSpecBuilder()
                .withMetricsConfig(new StrimziMetricsReporterBuilder()
                        .withNewValues()
                            .withNewPrometheusEndpoint()
                                .withEnabled(false)
                            .endPrometheusEndpoint()
                            .withNewOtlp()
                                .withEndpoint("http://my-collector:4317")
                                .withProtocol(OtlpProtocol.GRPC)
                                .withIntervalSeconds(30)
                            .endOtlp()
                        .endValues()
                        .build())
                .build());

        assertThat(metrics.isPrometheusEndpointEnabled(), is(false));
        assertThat(metrics.isOtlpEnabled(), is(true));
        assertThat(metrics.envVars().stream().map(EnvVar::getName).toList(),
                is(List.of(StrimziMetricsReporterModel.ENV_VAR_POD_NAME, StrimziMetricsReporterModel.ENV_VAR_NODE_NAME)));
        assertThat(metrics.envVars().get(0).getValueFrom().getFieldRef().getFieldPath(), is("metadata.name"));
        assertThat(metrics.envVars().get(1).getValueFrom().getFieldRef().getFieldPath(), is("spec.nodeName"));

        assertThat(metrics.otlpConfiguration("my-namespace", "my-cluster", "kafka", null), is(Map.of(
                StrimziMetricsReporterConfig.OTLP_ENABLE, "true",
                StrimziMetricsReporterConfig.OTLP_ENDPOINT, "http://my-collector:4317",
                StrimziMetricsReporterConfig.OTLP_PROTOCOL, "grpc",
                StrimziMetricsReporterConfig.OTLP_INTERVAL_SECONDS, "30",
                StrimziMetricsReporterConfig.OTLP_RESOURCE_ATTRIBUTES, "k8s.namespace.name=my-namespace,"
                        + "k8s.pod.name=${strimzienv:STRIMZI_POD_NAME},"
                        + "k8s.container.name=kafka,"
                        + "k8s.node.name=${strimzienv:STRIMZI_NODE_NAME},"
                        + "service.name=my-cluster,"
                        + "service.namespace=my-namespace,"
                        + "service.instance.id=my-namespace.${strimzienv:STRIMZI_POD_NAME}.kafka")));
    }

    @Test
    public void testOtlpConfigurationLeavesUnsetValuesOut() {
        StrimziMetricsReporterModel metrics = new StrimziMetricsReporterModel(new KafkaClusterSpecBuilder()
                .withMetricsConfig(new StrimziMetricsReporterBuilder()
                        .withNewValues()
                            .withNewOtlp()
                            .endOtlp()
                        .endValues()
                        .build())
                .build());

        assertThat(metrics.isPrometheusEndpointEnabled(), is(true));
        assertThat(metrics.otlpConfiguration("my-namespace", "my-cluster", "kafka", null).keySet(),
                is(Set.of(StrimziMetricsReporterConfig.OTLP_ENABLE, StrimziMetricsReporterConfig.OTLP_RESOURCE_ATTRIBUTES)));
    }

    @Test
    public void testOtlpUserResourceAttributes() {
        StrimziMetricsReporterModel metrics = new StrimziMetricsReporterModel(new KafkaClusterSpecBuilder()
                .withMetricsConfig(new StrimziMetricsReporterBuilder()
                        .withNewValues()
                            .withNewOtlp()
                                .withResourceAttributes(Map.of(
                                        "service.name", "my-service",
                                        "team", "a,b=c d"))
                            .endOtlp()
                        .endValues()
                        .build())
                .build());

        String attributes = metrics.otlpConfiguration("my-namespace", "my-cluster", "kafka", null)
                .get(StrimziMetricsReporterConfig.OTLP_RESOURCE_ATTRIBUTES);
        // The user value replaces the operator one and is percent-encoded
        assertThat(attributes.contains("service.name=my-service,"), is(true));
        assertThat(attributes.contains("service.name=my-cluster"), is(false));
        assertThat(attributes.contains("team=a%2Cb%3Dc%20d"), is(true));
    }

    @Test
    public void testOtlpServiceNameFromContainerTemplate() {
        StrimziMetricsReporterModel metrics = new StrimziMetricsReporterModel(new KafkaClusterSpecBuilder()
                .withMetricsConfig(new StrimziMetricsReporterBuilder()
                        .withNewValues()
                            .withNewOtlp()
                            .endOtlp()
                        .endValues()
                        .build())
                .build());
        ContainerTemplate template = new ContainerTemplateBuilder()
                .withEnv(new ContainerEnvVarBuilder().withName("OTEL_SERVICE_NAME").withValue("my-tracing-service").build())
                .build();

        // The service name configured for tracing applies to the metrics as well
        String attributes = metrics.otlpConfiguration("my-namespace", "my-cluster", "kafka", template)
                .get(StrimziMetricsReporterConfig.OTLP_RESOURCE_ATTRIBUTES);
        assertThat(attributes.contains("service.name="), is(false));
        assertThat(attributes.contains("service.instance.id="), is(true));
    }

    @Test
    public void testOtlpValidation() {
        assertDoesNotThrow(() -> StrimziMetricsReporterModel.validate(new StrimziMetricsReporterBuilder()
                .withNewValues()
                    .withNewPrometheusEndpoint()
                        .withEnabled(false)
                    .endPrometheusEndpoint()
                    .withNewOtlp()
                        .withEndpoint("https://my-collector:4318")
                    .endOtlp()
                .endValues()
                .build())
        );

        InvalidResourceException ise0 = assertThrows(InvalidResourceException.class, () -> StrimziMetricsReporterModel.validate(
                new StrimziMetricsReporterBuilder()
                        .withNewValues()
                            .withNewPrometheusEndpoint()
                                .withEnabled(false)
                            .endPrometheusEndpoint()
                        .endValues()
                        .build())
        );
        assertThat(ise0.getMessage(), is("Metrics configuration is invalid: [otlp must be configured when the Prometheus endpoint is disabled]"));

        InvalidResourceException ise1 = assertThrows(InvalidResourceException.class, () -> StrimziMetricsReporterModel.validate(
                new StrimziMetricsReporterBuilder()
                        .withNewValues()
                            .withNewOtlp()
                                .withEndpoint("my-collector:4317")
                                .withIntervalSeconds(0)
                            .endOtlp()
                        .endValues()
                        .build())
        );
        assertThat(ise1.getMessage(), is("Metrics configuration is invalid: [OTLP endpoint must start with http:// or https://, OTLP interval must be at least 1 second]"));
    }
}
