package com.example.GenericConsumer.services;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Service;

import com.example.GenericConsumer.clients.KafkaConsumerClient;
import com.example.GenericConsumer.enums.KafkaDeserializerTypes;
import com.example.schema.CarProto.Car;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class CarConsumerService implements SmartLifecycle {

    private final KafkaConsumerClient kafkaConsumerClient;

    @Value("${kafka.username}")
    private String username;

    @Value("${kafka.password}")
    private String password;

    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    private static final String PROTO_TOPIC = "test-schema-car-protobuf";
    
    private ExecutorService executorService;
    private KafkaConsumer<String, Car> consumer;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile boolean started = false;

    @Override
    public void start() {
        if (started) {
            return;
        }
        
        log.info("Starting Kafka consumer in virtual thread...");
        
        // Create thread executor
        executorService = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r);
            thread.setName("kafka-consumer-thread");
            thread.setDaemon(false);
            return thread;
        });
        
        running.set(true);
        started = true;
        
        // Submit consumer task to executor
        executorService.submit(this::consumeCar);
        
        log.info("Kafka consumer started successfully");
    }

    private void consumeCar() {
        try {
            consumer = kafkaConsumerClient.getDefaultConsumer(
                username, 
                password, 
                schemaRegistryUrl, 
                KafkaDeserializerTypes.STRING_DESERIALIZER, 
                KafkaDeserializerTypes.PROTOBUF_DESERIALIZER
            );
            
            consumer.subscribe(Collections.singleton(PROTO_TOPIC));
            log.info("Subscribed to topic: {}", PROTO_TOPIC);
            
            while (running.get()) {
                try {
                    ConsumerRecords<String, Car> records = consumer.poll(Duration.ofMillis(1000));
                    
                    for (ConsumerRecord<String, Car> carRecord : records) {
                        log.info("Consumed Car Record - Key: {}, Value: {}, Partition: {}, Offset: {}", 
                            carRecord.key(),
                            carRecord.value(), 
                            carRecord.partition(),
                            carRecord.offset()
                        );
                    }
                    
                    if (!records.isEmpty()) {
                        consumer.commitAsync();
                    }
                    
                } catch (WakeupException e) {
                    log.info("Consumer wakeup called, shutting down...");
                    break;
                }
            }
            
        } catch (Exception e) {
            log.error("Error in consumer loop", e);
        } finally {
            log.info("Closing Kafka consumer...");
            if (consumer != null) {
                try {
                    consumer.close(Duration.ofSeconds(5));
                } catch (Exception e) {
                    log.error("Error closing consumer", e);
                }
            }
            log.info("Kafka consumer closed");
        }
    }

    @Override
    public void stop() {
        log.info("Shutting down Kafka consumer service...");
        running.set(false);
        if (consumer != null) {
            consumer.wakeup();
        }
        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)) {
                    log.warn("Executor did not terminate in time, forcing shutdown");
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                log.error("Shutdown interrupted", e);
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        started = false;
        log.info("Kafka consumer service shutdown complete");
    }

    @Override
    public boolean isRunning() {
        return started;
    }

    @Override
    public int getPhase() {
        // Return a phase value to control startup order
        // Higher values start later and stop earlier
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean isAutoStartup() {
        // Return true to start automatically when Spring context is ready
        return true;
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }
}