package ru.n1ppl3.thrift.kafka.jdbc.saver.service;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;
import ru.n1ppl3.thrift.kafka.jdbc.saver.entity.UserLogsRecord;

import java.util.List;

@Slf4j
@Service
@AllArgsConstructor
public class RetryableUserLogsService {

    private final RetryTemplate retryTemplate;
    private final UserLogsService userLogsService;

    // If you need retry capabilities when you use a batch listener, we recommend that you use a RetryTemplate within the listener itself.
    public Integer tryToSaveRecordsToDb(List<UserLogsRecord> userLogsRecordList) throws Exception {
        return retryTemplate.execute((RetryCallback<Integer, Exception>) context -> {
            try {
                return userLogsService.saveAllAsBatch(userLogsRecordList);
            } catch (Exception e) {
                log.warn(e.getLocalizedMessage() + " (try#" + context.getRetryCount() + ")", e);
                throw e;
            }
        });
    }
}
