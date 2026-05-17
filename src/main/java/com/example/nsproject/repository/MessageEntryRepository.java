package com.example.nsproject.repository;

import com.example.nsproject.data.MessageEntry;
import org.springframework.data.jpa.repository.JpaRepository;

public interface MessageEntryRepository extends JpaRepository<MessageEntry, Long> {
}