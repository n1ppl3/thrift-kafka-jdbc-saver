package ru.n1ppl3.thrift.kafka.jdbc.saver.service;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import ru.n1ppl3.thrift.kafka.jdbc.saver.configuration.SpringJdbcConfiguration;
import ru.n1ppl3.thrift.kafka.jdbc.saver.entity.UserLogsRecord;
import ru.n1ppl3.thrift.kafka.jdbc.saver.repository.UserLogsRepository;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static ru.n1ppl3.thrift.kafka.jdbc.saver.SharedTestData.r2;

@ExtendWith(SpringExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestPropertySource(locations = {"classpath:application.properties", "classpath:application-test.properties"})
@ContextConfiguration(classes = {SpringJdbcConfiguration.class, UserLogsRepository.class, UserLogsService.class})
class UserLogsServiceTest {

    private static final int COUNT = 100;
    private static final List<UserLogsRecord> RECORDS0 = newRecordsList(COUNT);
    private static final List<UserLogsRecord> RECORDS1 = newRecordsList(COUNT);
    private static final List<UserLogsRecord> RECORDS2 = newRecordsList(COUNT);
    private static final List<UserLogsRecord> RECORDS3 = newRecordsList(COUNT);
    private static final List<UserLogsRecord> RECORDS4 = newRecordsList(COUNT);
    private static final List<UserLogsRecord> RECORDS5 = newRecordsList(COUNT);
    private static final List<UserLogsRecord> RECORDS6 = newRecordsList(COUNT);

    @Autowired
    private UserLogsService userLogsService;

    @Test
    @Order(0)
    void getRecordsCountTest() {
        userLogsService.getRecordsCount(); // чисто прогреть пул
    }

    @Test
    @Order(100)
    void saveAllAsBatchTest() {
        assertEquals(COUNT, userLogsService.saveAllAsBatch(RECORDS0));
    }

    @Test
    @Order(200)
    void saveAllAsBatchNativeTest() {
        assertEquals(COUNT, userLogsService.saveAllAsBatchNative(RECORDS1));
    }

    @Test
    @Order(300)
    void saveAllAsBatchMergedTest() {
        assertEquals(COUNT, userLogsService.saveAllAsBatchMerged(RECORDS2));
    }

    @Test
    @Order(301)
    void saveAllAsBatchMergedTest2() { // все дубли
        assertEquals(COUNT, userLogsService.saveAllAsBatchMerged(RECORDS2));
    }

    @Test
    @Order(400)
    void saveAllAsBatchMergedNativeTest() {
        assertEquals(COUNT, userLogsService.saveAllAsBatchMergedNative(RECORDS3));
    }

    @Test
    @Order(500)
    void saveAllOneTxManyInsertsTest() {
        assertEquals(COUNT, userLogsService.saveAllOneTxManyInserts(RECORDS4));
    }

    @Test
    @Order(600)
    void saveAllOneTxManyInsertsMergedTest() {
        assertEquals(COUNT, userLogsService.saveAllOneTxManyInsertsMerged(RECORDS5));
    }

    @Test
    @Order(700)
    void saveAllTxPerInsertTest() {
        assertEquals(COUNT, userLogsService.saveAllTxPerInsert(RECORDS6));
    }


    private static List<UserLogsRecord> newRecordsList(int count) {
        List<UserLogsRecord> result = new ArrayList<>(count);
        for (int i=0; i < count; i++) {
            result.add(r2.deepCopy().withNewMessageId());
        }
        return result;
    }

}
