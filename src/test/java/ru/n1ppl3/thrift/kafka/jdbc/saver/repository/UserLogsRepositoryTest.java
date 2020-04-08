package ru.n1ppl3.thrift.kafka.jdbc.saver.repository;

import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import ru.n1ppl3.thrift.kafka.jdbc.saver.configuration.SpringJdbcConfiguration;
import ru.n1ppl3.thrift.kafka.jdbc.saver.entity.UserLogsRecord;

import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static ru.n1ppl3.thrift.kafka.jdbc.saver.SharedTestData.r1;
import static ru.n1ppl3.thrift.kafka.jdbc.saver.SharedTestData.r2;
import static ru.n1ppl3.thrift.kafka.jdbc.saver.TestUtils.generateRemoteAddr;

@Slf4j
@ExtendWith(SpringExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestPropertySource(locations = {"classpath:application.properties", "classpath:application-test.properties"})
@ContextConfiguration(classes = {SpringJdbcConfiguration.class, UserLogsRepository.class})
class UserLogsRepositoryTest {


    @Autowired
    private UserLogsRepository userLogsRepository;


    @Test
    @Order(0)
    void getRecordsCountTest() {
        Long recordsCount = userLogsRepository.getRecordsCount();
        assertTrue(recordsCount >= 0, "recordsCount=" + recordsCount);
    }

    @Test
    @Order(10)
    void saveOneTest() {
        UserLogsRecord c2 = r2.deepCopy().withNewMessageId();
        assertEquals(1, userLogsRepository.saveOne(c2));
        // duplicate
        DuplicateKeyException dke = assertThrows(DuplicateKeyException.class, () -> userLogsRepository.saveOne(c2));
        log.info("Got exception as expected:", dke);
    }

    @Test
    @Order(20)
    void saveOneMergedTest() {
        UserLogsRecord c2 = r2.deepCopy().withNewMessageId();
        assertEquals(1, userLogsRepository.saveOneMerged(c2));
        // duplicate
        assertEquals(0, userLogsRepository.saveOneMerged(c2));
    }

    @Test
    @Order(30)
    void saveManyTest() {
        String remoteAddr = generateRemoteAddr();
        UserLogsRecord c1 = r1.deepCopy().setRemoteAddr(remoteAddr).withNewMessageId();
        UserLogsRecord c2 = r2.deepCopy().setRemoteAddr(remoteAddr).withNewMessageId();

        assertEquals(2, userLogsRepository.saveMany(asList(c1, c2)));
        checkInsertedRecords(remoteAddr, c1, c2);
    }

    @Test
    @Order(40)
    void saveManyNativeTest() {
        String remoteAddr = generateRemoteAddr();
        UserLogsRecord c1 = r1.deepCopy().setRemoteAddr(remoteAddr).withNewMessageId();
        UserLogsRecord c2 = r2.deepCopy().setRemoteAddr(remoteAddr).withNewMessageId();

        assertEquals(2, userLogsRepository.saveManyNative(asList(c1, c2)));
        checkInsertedRecords(remoteAddr, c1, c2);
    }

    @Test
    @Order(50)
    void saveManyMergedTest() {
        String remoteAddr = generateRemoteAddr();
        UserLogsRecord c1 = r1.deepCopy().setRemoteAddr(remoteAddr).withNewMessageId();
        UserLogsRecord c2 = r2.deepCopy().setRemoteAddr(remoteAddr).withNewMessageId();

        assertEquals(2, userLogsRepository.saveManyMerged(asList(c1, c2)));
        assertEquals(3, userLogsRepository.saveManyMerged(asList(c1, c2, c1)));
        checkInsertedRecords(remoteAddr, c1, c2);
    }

    @Test
    @Order(60)
    void saveManyMergedNativeTest() {
        String remoteAddr = generateRemoteAddr();
        UserLogsRecord c1 = r1.deepCopy().setRemoteAddr(remoteAddr).withNewMessageId();
        UserLogsRecord c2 = r2.deepCopy().setRemoteAddr(remoteAddr).withNewMessageId();

        assertEquals(2, userLogsRepository.saveManyMergedNative(asList(c1, c2)));
        assertEquals(3, userLogsRepository.saveManyMergedNative(asList(c1, c2, c1)));
        checkInsertedRecords(remoteAddr, c1, c2);
    }


    private void checkInsertedRecords(String remoteAddr, UserLogsRecord r1, UserLogsRecord r2) {
        List<UserLogsRecord> found = userLogsRepository.findAllByRemoteAddr(remoteAddr);
        assertEquals(2, found.size());

        Assertions.assertThat(found.get(0)).usingRecursiveComparison().isEqualTo(r1);
        Assertions.assertThat(found.get(1)).usingRecursiveComparison().isEqualTo(r2);
    }

}
