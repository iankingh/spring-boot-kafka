package com.heibaiying.springboot;

import com.heibaiying.springboot.bean.Programmer;
import com.heibaiying.springboot.constant.Topic;
import com.heibaiying.springboot.consumer.KafkaBeanConsumer;
import com.heibaiying.springboot.consumer.KafkaGroupConsumer;
import com.heibaiying.springboot.consumer.KafkaSimpleConsumer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.server.common.MetadataVersion;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.MockMvc;
import tools.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = "spring.kafka.listener.concurrency=1")
@AutoConfigureMockMvc
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class KafkaMessagingIntegrationTest {
    private static KafkaClusterTestKit cluster;

    @BeforeAll
    static void startCluster() throws Exception {
        // Use released metadata and a stable controller instead of the testkit's experimental defaults.
        TestKitNodes nodes = new TestKitNodes.Builder(
                BootstrapMetadata.fromVersion(MetadataVersion.latestProduction(), "integration-test"))
                .setCombined(true).setNumBrokerNodes(2).setNumControllerNodes(1).build();
        cluster = new KafkaClusterTestKit.Builder(nodes)
                .setConfigProp("group.initial.rebalance.delay.ms", "0")
                .setConfigProp("offsets.topic.replication.factor", "2")
                .setConfigProp("controlled.shutdown.enable", "false")
                .build();
        try {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
            try (Admin admin = Admin.create(cluster.clientProperties())) {
                admin.createTopics(List.of(
                        new NewTopic(Topic.SIMPLE, 10, (short) 2),
                        new NewTopic(Topic.BEAN, 10, (short) 2),
                        new NewTopic(Topic.GROUP, 10, (short) 2)))
                        .all().get(20, TimeUnit.SECONDS);
            }
        } catch (Exception error) {
            try {
                cluster.close();
            } catch (Exception cleanupError) {
                error.addSuppressed(cleanupError);
            }
            cluster = null;
            throw error;
        }
    }

    @AfterAll
    static void stopCluster() throws Exception {
        if (cluster != null) {
            cluster.close();
        }
    }

    @DynamicPropertySource
    static void brokerAddress(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers",
                () -> cluster.clientProperties().getProperty("bootstrap.servers"));
    }

    @Autowired
    private KafkaListenerEndpointRegistry listeners;

    @Autowired
    private MockMvc mvc;

    @Autowired
    private ObjectMapper mapper;

    @MockitoSpyBean
    private KafkaSimpleConsumer simpleConsumer;

    @MockitoSpyBean
    private KafkaBeanConsumer beanConsumer;

    @MockitoSpyBean
    private KafkaGroupConsumer groupConsumer;

    @Test
    @Timeout(90)
    void endpointsSendSerializedMessagesToRealListenersAndExplicitPartitions() throws Exception {
        for (var listener : listeners.getListenerContainers()) {
            var partitions = listener.getContainerProperties().getTopicPartitions();
            ContainerTestUtils.waitForAssignment(listener, partitions == null ? 10 : partitions.length);
        }
        CompletableFuture<ConsumerRecord<String, String>> decodedBean = new CompletableFuture<>();
        doAnswer(invocation -> {
            Object result = invocation.callRealMethod();
            decodedBean.complete(invocation.getArgument(0));
            return result;
        }).when(beanConsumer).consumer(any());

        mvc.perform(get("/sendSimple")).andExpect(status().isOk());
        ArgumentCaptor<ConsumerRecord<String, String>> simple = recordCaptor();
        verify(simpleConsumer, timeout(20000)).consume(simple.capture());
        assertEquals(Topic.SIMPLE, simple.getValue().topic());
        assertEquals("hello spring boot kafka", simple.getValue().value());

        mvc.perform(get("/sendBean")).andExpect(status().isOk());
        // Completion follows the real listener's successful JSON decode, not just its invocation.
        ConsumerRecord<String, String> beanRecord = decodedBean.get(20, TimeUnit.SECONDS);
        assertEquals(Topic.BEAN, beanRecord.topic());
        Programmer bean = mapper.readValue(beanRecord.value(), Programmer.class);
        assertEquals("xiaoming", bean.getName());
        assertEquals(12, bean.getAge());
        assertEquals(21212.33f, bean.getSalary());
        assertNotNull(bean.getBirthday());

        mvc.perform(get("/sendGroup")).andExpect(status().isOk());
        ArgumentCaptor<ConsumerRecord<String, String>> first = recordCaptor();
        ArgumentCaptor<ConsumerRecord<String, String>> second = recordCaptor();
        ArgumentCaptor<ConsumerRecord<String, String>> duplicate = recordCaptor();
        ArgumentCaptor<ConsumerRecord<String, String>> all = recordCaptor();
        verify(groupConsumer, timeout(20000).times(2)).consumer1_1(first.capture());
        verify(groupConsumer, timeout(20000).times(2)).consumer1_2(second.capture());
        verify(groupConsumer, timeout(20000).times(2)).consumer1_3(duplicate.capture());
        verify(groupConsumer, timeout(20000).times(4)).consumer2_1(all.capture());
        assertPartitions(first, Map.of(0, "hello group 0", 1, "hello group 1"));
        assertPartitions(duplicate, Map.of(0, "hello group 0", 1, "hello group 1"));
        assertPartitions(second, Map.of(2, "hello group 2", 3, "hello group 3"));
        assertPartitions(all, Map.of(0, "hello group 0", 1, "hello group 1",
                2, "hello group 2", 3, "hello group 3"));
    }

    private static void assertPartitions(ArgumentCaptor<ConsumerRecord<String, String>> captor,
                                         Map<Integer, String> expected) {
        Map<Integer, String> actual = new HashMap<>();
        for (ConsumerRecord<String, String> record : captor.getAllValues()) {
            assertEquals(Topic.GROUP, record.topic());
            assertEquals("key", record.key());
            actual.put(record.partition(), record.value());
        }
        assertEquals(expected.size(), captor.getAllValues().size());
        assertEquals(expected, actual);
    }

    private static ArgumentCaptor<ConsumerRecord<String, String>> recordCaptor() {
        return ArgumentCaptor.captor();
    }
}
