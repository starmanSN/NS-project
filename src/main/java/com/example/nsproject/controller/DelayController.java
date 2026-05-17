package com.example.nsproject.controller;

import com.example.nsproject.service.DelayManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/delay")
public class DelayController {

    @Autowired
    private DelayManager delayManager;

    @PutMapping
    public ResponseEntity<String> updateDelay(@RequestParam("ms") long delayMs) {
        delayManager.setDelay(delayMs);
        return ResponseEntity.ok("Delay updated to: " + delayMs + " ms");
    }

    @GetMapping
    public ResponseEntity<Long> getCurrentDelay() {
        return ResponseEntity.ok(delayManager.getDelay());
    }
}