package com.heibaiying.springboot.controller;

import com.heibaiying.springboot.bean.Programmer;
import com.heibaiying.springboot.constant.Topic;
import com.heibaiying.springboot.producer.KafkaCustomProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.util.Date;

/**
 * @author : heibaiying
 * @description :  测试消息发送
 */
@Slf4j
@RestController
public class SendMsgController {

    private final KafkaCustomProducer producer;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public SendMsgController(KafkaCustomProducer producer,
                             KafkaTemplate<String, String> kafkaTemplate,
                             ObjectMapper objectMapper) {
        this.producer = producer;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /***
     * 发送消息体为基本类型的消息  http://127.0.0.1:8080/sendSimple
     */

    @GetMapping("sendSimple")
    public void sendSimple() {
        producer.sendMessage(Topic.SIMPLE, "hello spring boot kafka");
    }

    /***
     * 发送消息体为bean的消息  http://127.0.0.1:8080/sendBean
     */
    @GetMapping("sendBean")
    public void sendBean() throws JacksonException {
        Programmer programmer = new Programmer("xiaoming", 12, 21212.33f, new Date());
        producer.sendMessage(Topic.BEAN, objectMapper.writeValueAsString(programmer));
    }


    /***
     * 多消费者组、组中多消费者对同一主题的消费情况   http://127.0.0.1:19091/sendGroup
     */
    @GetMapping("sendGroup")
    public void sendGroup() {
        for (int i = 0; i < 4; i++) {
            kafkaTemplate.send(Topic.GROUP, i % 4, "key", "hello group " + i)
                    .whenComplete((sendResult, throwable) -> {
                        if (throwable != null) {
                            log.error("发送消息失败", throwable);
                        } else {
                            log.info("发送结果: {}", sendResult);
                        }
                    });
        }
    }
}
