package com.heibaiying.springboot.consumer;

import com.heibaiying.springboot.constant.Topic;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @author : heibaiying
 * @description : kafka 简单消息消费者
 */

@Component
@Slf4j
public class KafkaSimpleConsumer {

    // 简单消费者
    @KafkaListener(groupId = "simpleGroup", topics = Topic.SIMPLE)
    public void consume(ConsumerRecord<String, String> record) {
        log.info("消费者收到消息: {}; topic: {}", record.value(), record.topic());

        /*  
	         * 发送结果:SendResult [producerRecord=ProducerRecord(topic=spring.boot.kafka.simple, partition=null, headers=RecordHeaders(headers = [], isReadOnly = true), key=null, value=hello spring boot kafka, timestamp=null), recordMetadata=spring.boot.kafka.simple-0@4]
	        消费者收到消息:hello spring boot kafka; topic:spring.boot.kafka.simple
         * 如果关闭自动提交，可通过 ConsumerAwareMessageListener 取得 consumer 后手工提交。
         */
    }
}
