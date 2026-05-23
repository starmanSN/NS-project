package com.example.nsproject.service;

import com.example.nsproject.data.MessageEntry;
import com.example.nsproject.repository.MessageEntryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaMessageService {

    private final MessageEntryRepository repository;
    private final ObjectMapper mapper = new ObjectMapper();
    private final DelayManager delayManager;

    @KafkaListener(topics = "test_topic", concurrency = "3")
    public void listen(String message) {
        try {
            log.info("{} - [Read from Kafka] {}", now(), message);

            JsonNode node = mapper.readTree(message);
            String msgUuid = node.get("msg_uuid").asText();
            boolean head = node.get("head").asBoolean();

            long delay = delayManager.getDelay();
            log.info("Delay before writing to the database: {} ms", delay);

            try {
                TimeUnit.MILLISECONDS.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("The thread was interrupted during the delay.");
                return;
            }
            MessageEntry entry = new MessageEntry();
            entry.setMsgUuid(msgUuid);
            entry.setHead(head);
            entry.setTimeRq(Instant.now().getEpochSecond());
            repository.save(entry);

            log.info("{} - [Write to DB] {{ \"msgUuid\": \"{}\", \"head\": {}, \"timeRq\": \"{}\" }}",
                    now(), entry.getMsgUuid(), entry.getHead(), entry.getTimeRq());

        } catch (Exception ex) {
            log.error("Failed to process message: {}", message, ex);
        }
    }

    private String now() {
        return DateTimeFormatter
                .ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                .withZone(ZoneId.systemDefault())
                .format(Instant.now());
    }
}
