package ru.n1ppl3.thrift.kafka.jdbc.saver.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import ru.n1ppl3.thrift.kafka.jdbc.saver.TestUtils;
import ru.n1ppl3.thrift.kafka.jdbc.saver.configuration.SpringKafkaConfiguration;
import ru.n1ppl3.thrift.kafka.jdbc.saver.service.RetryableUserLogsService;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static ru.n1ppl3.thrift.kafka.jdbc.saver.SharedTestData.r3;

@Slf4j
@EmbeddedKafka
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {SpringKafkaConfiguration.class, MyListener.class})
@TestPropertySource(locations = {"classpath:application.properties", "classpath:application-test.properties"},
    properties = {"spring.kafka.consumer.group-id=MyListenerTest-group", "my.topic.name=${MyListenerTest.topic}"}) // чтобы не было пересечений с другим тестом
class MyListenerTest {

    private static final String TOPIC_NAME = MyListenerTest.class.getSimpleName() + "Topic-" + Instant.now().toEpochMilli();
    static {
        System.setProperty("MyListenerTest.topic", TOPIC_NAME);
    }


    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;
    @Autowired
    private KafkaListenerEndpointRegistry registry;
    @MockBean
    private RetryableUserLogsService retryableUserLogsService;


    @Test
    void listenerTest() throws Exception {
        doReturn(1).when(retryableUserLogsService).tryToSaveRecordsToDb(anyList());

        ContainerTestUtils.waitForAssignment(registry.getAllListenerContainers().iterator().next(), embeddedKafka.getPartitionsPerTopic());
        log.info("ASSIGNMENT COMPLETE! partitionsPerTopic = " + embeddedKafka.getPartitionsPerTopic());

        TestUtils.sendToListener(embeddedKafka.getBrokersAsString(), TOPIC_NAME, r3.deepCopy());

        assertEquals(1, MyListener.waitUntilRecordsReceived(1, 5_000));
        verify(retryableUserLogsService).tryToSaveRecordsToDb(anyList());
    }

}
