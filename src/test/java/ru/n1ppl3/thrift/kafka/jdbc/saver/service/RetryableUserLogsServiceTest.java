package ru.n1ppl3.thrift.kafka.jdbc.saver.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import ru.n1ppl3.thrift.kafka.jdbc.saver.configuration.SpringRetryConfiguration;
import ru.n1ppl3.thrift.kafka.jdbc.saver.entity.UserLogsRecord;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {SpringRetryConfiguration.class})
class RetryableUserLogsServiceTest {


    @Autowired
    private RetryTemplate retryTemplate;
    @MockBean
    private UserLogsService userLogsService;


    @Test
    void tryToSaveRecordsToDb_checkRetry() throws Exception {
        var retryableUserLogsService = new RetryableUserLogsService(retryTemplate, userLogsService);

        List<UserLogsRecord> userLogsRecordList = Collections.emptyList();

        // 2 провала и 3 удачный
        Mockito.doThrow(new RuntimeException("1"))
                .doThrow(new RuntimeException("2"))
                .doReturn(17)
                .when(userLogsService).saveAllAsBatch(userLogsRecordList);

        assertEquals(17, retryableUserLogsService.tryToSaveRecordsToDb(userLogsRecordList));

        // все 3 попытки провалились
        Mockito.doThrow(new RuntimeException("3"))
                .doThrow(new RuntimeException("4"))
                .doThrow(new RuntimeException("5"))
                .when(userLogsService).saveAllAsBatch(userLogsRecordList);

        var thrown = assertThrows(RuntimeException.class, () -> retryableUserLogsService.tryToSaveRecordsToDb(userLogsRecordList));
        assertEquals("5", thrown.getLocalizedMessage());
    }

}
