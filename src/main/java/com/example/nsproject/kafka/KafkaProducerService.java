package com.example.myservice.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class KafkaProducerService {

    private final KafkaConfig config;
    private KafkaProducer<String, String> producer;

    public KafkaProducerService(KafkaConfig config) {
        this.config = config;
        this.producer = new KafkaProducer<>(config.getProducerProps());
    }

    public void sendMessage(String message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(config.getTopic(), message);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.info("Ошибка при отправке: " + exception.getMessage());
            } else {
                log.info("Сообщение отправлено - offset: " + metadata.offset());
            }
        });
    }

    public void close() {
        producer.close();
    }
}