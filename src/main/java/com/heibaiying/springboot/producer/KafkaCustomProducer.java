package com.heibaiying.springboot.producer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

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
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, message);

        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onFailure(Throwable throwable) {
                log.error("Failed to send message to topic {}", topic, throwable);
            }

            @Override
            public void onSuccess(SendResult<String, String> sendResult) {
                log.info("Message sent to topic {}: {}", topic, sendResult);
            }
        });
    }
}
