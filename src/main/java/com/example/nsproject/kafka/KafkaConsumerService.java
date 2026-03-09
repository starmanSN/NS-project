package com.example.myservice.kafka;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;

@Slf4j
public class KafkaConsumerService {

    private final KafkaConfig config;
    private KafkaConsumer<String, String> consumer;
    private Thread consumerThread;
    private volatile boolean running = true;

    public KafkaConsumerService(KafkaConfig config) {
        this.config = config;
    }

    @PostConstruct
    public void start() {
        consumer = new KafkaConsumer<>(config.getConsumerProps());
        consumer.subscribe(Collections.singletonList(config.getTopic()));

        consumerThread = new Thread(() -> {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Получено: key=%s, value=%s, offset=%d%n",
                            record.key(), record.value(), record.offset());
                }
            }
        });
        consumerThread.start();
    }

    @PreDestroy
    public void stop() {
        running = false;
        try {
            consumer.wakeup();
            consumerThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            consumer.close();
        }
    }
}