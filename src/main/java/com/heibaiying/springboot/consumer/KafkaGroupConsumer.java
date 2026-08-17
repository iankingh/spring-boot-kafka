package com.heibaiying.springboot.consumer;

import com.heibaiying.springboot.constant.Topic;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

/**
 * @author : heibaiying
 * @description : kafka 消费者组
 * <p>
 * 
 *  consumer1-1 收到消息:hello group 1
	consumer1-3 收到消息:hello group 1
	consumer2-1 收到消息:hello group 1
	consumer1-1 收到消息:hello group 0
	发送结果:SendResult [producerRecord=ProducerRecord(topic=spring.boot.kafka.newGroup, partition=1, headers=RecordHeaders(headers = [], isReadOnly = true), key=key, value=hello group 1, timestamp=null), recordMetadata=spring.boot.kafka.newGroup-1@1]
	consumer1-3 收到消息:hello group 0
	发送结果:SendResult [producerRecord=ProducerRecord(topic=spring.boot.kafka.newGroup, partition=0, headers=RecordHeaders(headers = [], isReadOnly = true), key=key, value=hello group 0, timestamp=null), recordMetadata=spring.boot.kafka.newGroup-0@1]
	consumer2-1 收到消息:hello group 0
	consumer1-2 收到消息:hello group 2
	consumer1-2 收到消息:hello group 3
	consumer2-1 收到消息:hello group 2
	consumer2-1 收到消息:hello group 3
	发送结果:SendResult [producerRecord=ProducerRecord(topic=spring.boot.kafka.newGroup, partition=3, headers=RecordHeaders(headers = [], isReadOnly = true), key=key, value=hello group 3, timestamp=null), recordMetadata=spring.boot.kafka.newGroup-3@1]
	发送结果:SendResult [producerRecord=ProducerRecord(topic=spring.boot.kafka.newGroup, partition=2, headers=RecordHeaders(headers = [], isReadOnly = true), key=key, value=hello group 2, timestamp=null), recordMetadata=spring.boot.kafka.newGroup-2@1]

 * 
 * 多个消费者群组可以共同读取同一个主题，彼此之间互不影响。
 */
@Component
@Slf4j
public class KafkaGroupConsumer {

    // 分组1 中的消费者1
    @KafkaListener(id = "consumer1-1", groupId = "group1", topicPartitions =
            {@TopicPartition(topic = Topic.GROUP, partitions = {"0", "1"})
            })
    public void consumer1_1(ConsumerRecord<String, String> record) {
        log.info("consumer1-1 收到消息: {}", record.value());
    }

    // 分组1 中的消费者2
    @KafkaListener(id = "consumer1-2", groupId = "group1", topicPartitions =
            {@TopicPartition(topic = Topic.GROUP, partitions = {"2", "3"})
            })
    public void consumer1_2(ConsumerRecord<String, String> record) {
        log.info("consumer1-2 收到消息: {}", record.value());
    }

    // 分组1 中的消费者3
    @KafkaListener(id = "consumer1-3", groupId = "group1", topicPartitions =
            {@TopicPartition(topic = Topic.GROUP, partitions = {"0", "1"})
            })
    public void consumer1_3(ConsumerRecord<String, String> record) {
        log.info("consumer1-3 收到消息: {}", record.value());
    }

//    // 分组1 中的消费者3
//    @KafkaListener(id = "consumer1-4", groupId = "group1",  topics = Topic.GROUP)
//    public void consumer1_4(ConsumerRecord<String, Object> record) {
//        System.out.println("consumer1-4 收到消息:" + record.value());
//    }
    
    // 分组2 中的消费者
    @KafkaListener(id = "consumer2-1", groupId = "group2", topics = Topic.GROUP)
    public void consumer2_1(ConsumerRecord<String, String> record) {
        log.info("consumer2-1 收到消息: {}", record.value());
    }
}
