package com.banking.accountservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountEventConsumer {

    private final AccountService accountService;

    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(
            @Payload Map<String, Object> payload
    ) {
        try {
            String receiverAccount = (String) payload.get("receiverAccountNumber");
            BigDecimal amount = new BigDecimal(payload.get("amount").toString());

            log.info("Crediting account : {} with amount : {}", receiverAccount, amount);
            accountService.creditBalance(receiverAccount, amount);
        } catch (Exception e) {
            log.error("Error crediting amount : {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(@Payload Map<String, Object> payload) {
        try {
            String accountNumber = (String) payload.get("accountNumber");
            log.info("Fraud Detected - blocking account : {}", accountNumber);
            accountService.blockAccount(accountNumber);
        } catch (Exception e) {
            log.error("Error blocking account : {}", e.getMessage());
        }
    }
}
