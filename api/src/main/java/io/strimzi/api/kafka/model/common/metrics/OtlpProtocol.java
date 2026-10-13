/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.metrics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * OTLP transport protocols supported by the Strimzi Metrics Reporter.
 */
public enum OtlpProtocol {
    GRPC,
    HTTP_PROTOBUF;

    @JsonCreator
    public static OtlpProtocol forValue(String value) {
        switch (value) {
            case "grpc":
                return GRPC;
            case "http/protobuf":
                return HTTP_PROTOBUF;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        switch (this) {
            case GRPC:
                return "grpc";
            case HTTP_PROTOBUF:
                return "http/protobuf";
            default:
                return null;
        }
    }
}
