/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.metrics;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.ObjectFieldSelectorBuilder;
import io.strimzi.api.kafka.model.common.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.common.metrics.OtlpProtocol;
import io.strimzi.api.kafka.model.common.metrics.OtlpResourceAttribute;
import io.strimzi.api.kafka.model.common.metrics.OtlpResourceAttributeBuilder;
import io.strimzi.api.kafka.model.common.metrics.StrimziMetricsReporter;
import io.strimzi.api.kafka.model.common.metrics.StrimziMetricsReporterBuilder;
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
        assertThat(metrics.isOtlpTlsConfigured(), is(false));
        assertThat(metrics.envVars(), is(empty()));
        assertThat(metrics.otlpSecretsToAccess("my-cluster-kafka"), is(empty()));
        assertThat(metrics.otlpConfiguration("my-namespace", "my-cluster-kafka"), is(anEmptyMap()));
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
        // No resource attributes are set by default, so no environment variables are needed
        assertThat(metrics.envVars(), is(empty()));
        assertThat(metrics.otlpConfiguration("my-namespace", "my-cluster-kafka"), is(Map.of(
                StrimziMetricsReporterConfig.OTLP_ENABLE, "true",
                StrimziMetricsReporterConfig.OTLP_ENDPOINT, "http://my-collector:4317",
                StrimziMetricsReporterConfig.OTLP_PROTOCOL, "grpc",
                StrimziMetricsReporterConfig.OTLP_INTERVAL_SECONDS, "30")));
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
        assertThat(metrics.otlpConfiguration("my-namespace", "my-cluster-kafka").keySet(),
                is(Set.of(StrimziMetricsReporterConfig.OTLP_ENABLE)));
    }

    @Test
    public void testOtlpResourceAttributes() {
        StrimziMetricsReporterModel metrics = new StrimziMetricsReporterModel(new KafkaClusterSpecBuilder()
                .withMetricsConfig(new StrimziMetricsReporterBuilder()
                        .withNewValues()
                            .withNewOtlp()
                                .withResourceAttributes(
                                        new OtlpResourceAttributeBuilder().withName("team").withValue("a,b=c d").build(),
                                        fieldRefAttribute("k8s.pod.name", "metadata.name"),
                                        fieldRefAttribute("k8s.node.name", "spec.nodeName"),
                                        new OtlpResourceAttributeBuilder().withName("service.name").withValue("my-service").build())
                            .endOtlp()
                        .endValues()
                        .build())
                .build());

        // One environment variable per attribute taken from the pod, named after the index of the attribute
        List<EnvVar> envVars = metrics.envVars();
        assertThat(envVars.stream().map(EnvVar::getName).toList(), is(List.of("STRIMZI_OTLP_RESOURCE_ATTRIBUTE_1", "STRIMZI_OTLP_RESOURCE_ATTRIBUTE_2")));
        assertThat(envVars.get(0).getValueFrom().getFieldRef().getFieldPath(), is("metadata.name"));
        assertThat(envVars.get(1).getValueFrom().getFieldRef().getFieldPath(), is("spec.nodeName"));

        // Static values are percent-encoded, the values from the pod are referenced with placeholders
        assertThat(metrics.otlpConfiguration("my-namespace", "my-cluster-kafka").get(StrimziMetricsReporterConfig.OTLP_RESOURCE_ATTRIBUTES),
                is("team=a%2Cb%3Dc%20d,"
                        + "k8s.pod.name=${strimzienv:STRIMZI_OTLP_RESOURCE_ATTRIBUTE_1},"
                        + "k8s.node.name=${strimzienv:STRIMZI_OTLP_RESOURCE_ATTRIBUTE_2},"
                        + "service.name=my-service"));
    }

    @Test
    public void testOtlpTls() {
        StrimziMetricsReporterModel metrics = new StrimziMetricsReporterModel(new KafkaClusterSpecBuilder()
                .withMetricsConfig(new StrimziMetricsReporterBuilder()
                        .withNewValues()
                            .withNewOtlp()
                                .withEndpoint("https://my-collector:4317")
                                .withNewTls()
                                    .withTrustedCertificates(new CertSecretSourceBuilder().withSecretName("collector-ca").withCertificate("ca.crt").build())
                                .endTls()
                                .withNewOtlpAuthenticationTls()
                                    .withNewCertificateAndKey()
                                        .withSecretName("my-client")
                                        .withCertificate("tls.crt")
                                        .withKey("tls.key")
                                    .endCertificateAndKey()
                                .endOtlpAuthenticationTls()
                            .endOtlp()
                        .endValues()
                        .build())
                .build());

        assertThat(metrics.isOtlpTlsConfigured(), is(true));
        assertThat(StrimziMetricsReporterModel.otlpTrustedCertsSecretName("my-cluster-kafka"), is("my-cluster-kafka-otlp-trusted-certs"));
        assertThat(metrics.otlpSecretsToAccess("my-cluster-kafka"), is(Set.of("my-cluster-kafka-otlp-trusted-certs", "my-client")));

        Map<String, String> configuration = metrics.otlpConfiguration("my-namespace", "my-cluster-kafka");
        assertThat(configuration.get(StrimziMetricsReporterConfig.OTLP_SSL_TRUSTSTORE_CERTIFICATES),
                is("${strimzisecrets:my-namespace/my-cluster-kafka-otlp-trusted-certs:ca.crt}"));
        assertThat(configuration.get(StrimziMetricsReporterConfig.OTLP_SSL_KEYSTORE_CERTIFICATE_CHAIN),
                is("${strimzisecrets:my-namespace/my-client:tls.crt}"));
        assertThat(configuration.get(StrimziMetricsReporterConfig.OTLP_SSL_KEYSTORE_KEY),
                is("${strimzisecrets:my-namespace/my-client:tls.key}"));
    }

    @Test
    public void testOtlpTrustedCertificatesOnly() {
        StrimziMetricsReporterModel metrics = new StrimziMetricsReporterModel(new KafkaClusterSpecBuilder()
                .withMetricsConfig(new StrimziMetricsReporterBuilder()
                        .withNewValues()
                            .withNewOtlp()
                                .withNewTls()
                                    .withTrustedCertificates(new CertSecretSourceBuilder().withSecretName("collector-ca").withPattern("*.crt").build())
                                .endTls()
                            .endOtlp()
                        .endValues()
                        .build())
                .build());

        assertThat(metrics.otlpCertificateAndKey(), is((Object) null));
        assertThat(metrics.otlpSecretsToAccess("my-connect-connect"), is(Set.of("my-connect-connect-otlp-trusted-certs")));
        Map<String, String> configuration = metrics.otlpConfiguration("my-namespace", "my-connect-connect");
        assertThat(configuration.containsKey(StrimziMetricsReporterConfig.OTLP_SSL_TRUSTSTORE_CERTIFICATES), is(true));
        assertThat(configuration.containsKey(StrimziMetricsReporterConfig.OTLP_SSL_KEYSTORE_KEY), is(false));
    }

    private static OtlpResourceAttribute fieldRefAttribute(String name, String fieldPath) {
        return new OtlpResourceAttributeBuilder()
                .withName(name)
                .withNewValueFrom()
                    .withFieldRef(new ObjectFieldSelectorBuilder().withFieldPath(fieldPath).build())
                .endValueFrom()
                .build();
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
                        .withResourceAttributes(
                                fieldRefAttribute("a", "metadata.name"),
                                fieldRefAttribute("b", "metadata.labels['strimzi.io/cluster']"),
                                fieldRefAttribute("c", "status.podIP"))
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

        InvalidResourceException ise2 = assertThrows(InvalidResourceException.class, () -> StrimziMetricsReporterModel.validate(
                new StrimziMetricsReporterBuilder()
                        .withNewValues()
                            .withNewOtlp()
                                .withResourceAttributes(
                                        fieldRefAttribute("a", "spec.containers[0].image"),
                                        new OtlpResourceAttributeBuilder().withName("b").build())
                            .endOtlp()
                        .endValues()
                        .build())
        );
        assertThat(ise2.getMessage(), is("Metrics configuration is invalid: [OTLP resource attribute a uses an unsupported fieldPath: spec.containers[0].image, "
                + "OTLP resource attribute b must have either value or valueFrom]"));
    }
}
