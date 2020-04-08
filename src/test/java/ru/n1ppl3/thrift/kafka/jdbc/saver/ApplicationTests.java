package ru.n1ppl3.thrift.kafka.jdbc.saver;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.ActiveProfiles;
import ru.n1ppl3.thrift.kafka.jdbc.saver.entity.UserLogsRecord;
import ru.n1ppl3.thrift.kafka.jdbc.saver.kafka.consumer.MyListener;
import ru.n1ppl3.thrift.kafka.jdbc.saver.repository.UserLogsRepository;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static ru.n1ppl3.thrift.kafka.jdbc.saver.SharedTestData.r3;
import static ru.n1ppl3.thrift.kafka.jdbc.saver.TestUtils.generateRemoteAddr;

@Slf4j
@EmbeddedKafka
@SpringBootTest
@ActiveProfiles("test")
class ApplicationTests {

	@Value("${my.topic.name}")
	private String topicName;
	@Autowired
	private EmbeddedKafkaBroker embeddedKafka;
	@Autowired
	private KafkaListenerEndpointRegistry registry;
	@Autowired
	private UserLogsRepository userLogsRepository;


	@Test
	void integrationTest_sendToTopic_checkRecordInDb() throws Exception {
		ContainerTestUtils.waitForAssignment(registry.getAllListenerContainers().iterator().next(), embeddedKafka.getPartitionsPerTopic());
		log.info("ASSIGNMENT COMPLETE! partitionsPerTopic = " + embeddedKafka.getPartitionsPerTopic());

		String remoteAddr = generateRemoteAddr();

		TestUtils.sendToListener(embeddedKafka.getBrokersAsString(), topicName, r3.deepCopy().setRemoteAddr(remoteAddr));

		MyListener.waitUntilRecordsReceived(1, 5_000);

		List<UserLogsRecord> recordsInDb = userLogsRepository.findAllByRemoteAddr(remoteAddr);
		assertEquals(1, recordsInDb.size());
	}


	void test() {
		// check'нуть ситуацию, когда сохранение в БД падает
	}

}
