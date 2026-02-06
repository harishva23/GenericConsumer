package com.example.GenericConsumer.enums;

import lombok.Getter;

@Getter
public enum KafkaDeserializerTypes {

    STRING_DESERIALIZER("org.apache.kafka.common.serialization.StringDeserializer"), AVRO_DESERIALIZER("io.confluent.kafka.serializers.KafkaAvroDeserializer"), JSON_DESERIALIZER("io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer"), PROTOBUF_DESERIALIZER("io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer");

    private final String className;

    KafkaDeserializerTypes(String className) {
        this.className = className;
    }
}