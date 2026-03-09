package com.example.myservice.kafka;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    private final KafkaProducerService producerService;

    public KafkaController(KafkaConfig config) {
        this.producerService = new KafkaProducerService(config);
    }

    @PostMapping("/send")
    public String sendMessage(@RequestParam String message) {
        producerService.sendMessage(message);
        return "Сообщение отправлено!";
    }
}