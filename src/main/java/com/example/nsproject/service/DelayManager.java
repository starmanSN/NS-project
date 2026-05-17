package com.example.nsproject.service;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
public class DelayManager {

    @Autowired
    private Environment environment;

    private final AtomicLong delayMillis = new AtomicLong(1000L);

    @PostConstruct
    public void init() {
        long configDelay = environment.getProperty("app.kafka.delay-ms", Long.class, 1000L);
        delayMillis.set(configDelay);
        log.info("DelayManager initialized with delay: {} ms", configDelay);
    }

    public long getDelay() {
        return delayMillis.get();
    }

    public void setDelay(long newDelayMillis) {
        if (newDelayMillis < 0) {
            throw new IllegalArgumentException("Delay cannot be negative: " + newDelayMillis);
        }
        long old = delayMillis.getAndSet(newDelayMillis);
        log.info("Delay changed from {} ms to {} ms", old, newDelayMillis);
    }
}

