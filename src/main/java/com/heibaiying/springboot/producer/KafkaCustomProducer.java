package com.heibaiying.springboot.producer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * Kafka producer used by the demo endpoints.
 */
@Component
@Slf4j
public class KafkaCustomProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaCustomProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String topic, String message) {
        kafkaTemplate.send(topic, message).whenComplete((sendResult, throwable) -> {
            if (throwable != null) {
                log.error("Failed to send message to topic {}", topic, throwable);
            } else {
                log.info("Message sent to topic {}: {}", topic, sendResult);
            }
        });
    }
}
