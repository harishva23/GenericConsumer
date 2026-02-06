package com.example.GenericConsumer.clients;

import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.example.GenericConsumer.enums.KafkaDeserializerTypes;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;



@Component
public class KafkaConsumerClient {

    public static final String FALSE = "false";
    private static final String GROUP_ID = "car-consumer";
    private static final String CLIENT_ID = "car-consumer1";
    private static final String SECURITY_PROTOCOL = "SASL_PLAINTEXT";
    private static final String SASL_MECHANISM = "SCRAM-SHA-512";
    private static final String SASL_JAAS_CONFIG = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
    private final String bootstrapServerUrl;

    public KafkaConsumerClient(@Value("${kafka.bootstrap.server.url}") String bootstrapServerUrl) {
        this.bootstrapServerUrl = bootstrapServerUrl;
    }

    public <K, V> KafkaConsumer<K, V> getDefaultConsumer(String username, String password, @Value("${schema.registry.url}") String schemaRegistryUrl,KafkaDeserializerTypes keDeserializerTypes, KafkaDeserializerTypes valueDeserializerTypes){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerUrl);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keDeserializerTypes.getClassName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerTypes.getClassName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 30000);
        props.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, SECURITY_PROTOCOL);
        props.setProperty(SaslConfigs.SASL_MECHANISM, SASL_MECHANISM);
        props.setProperty(SaslConfigs.SASL_JAAS_CONFIG, String.format(SASL_JAAS_CONFIG, username, password));
        props.setProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.setProperty("basic.auth.credentials.source", "USER_INFO");
        props.setProperty("basic.auth.user.info", username + ":" + password);
        props.put("specific.protobuf.value.type", "com.example.schema.CarProto$Car");
    
        return new KafkaConsumer<>(props);
    }
}
