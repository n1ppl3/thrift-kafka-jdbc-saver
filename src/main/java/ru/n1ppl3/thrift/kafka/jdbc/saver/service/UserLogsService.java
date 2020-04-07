package ru.n1ppl3.thrift.kafka.jdbc.saver.service;

import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import ru.n1ppl3.thrift.kafka.jdbc.saver.entity.UserLogsRecord;
import ru.n1ppl3.thrift.kafka.jdbc.saver.repository.UserLogsRepository;

import java.util.List;

@Service
public class UserLogsService {


    private final UserLogsRepository userLogsRepository;
    private final TransactionTemplate transactionTemplate;

    public UserLogsService(PlatformTransactionManager transactionManager, UserLogsRepository userLogsRepository) {
        this.userLogsRepository = userLogsRepository;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }


    @Transactional
    public Long getRecordsCount() {
        return userLogsRepository.getRecordsCount();
    }

    @Transactional
    public int saveAllAsBatch(List<UserLogsRecord> records) {
        return userLogsRepository.saveMany(records);
    }

    @Transactional
    public int saveAllAsBatchNative(List<UserLogsRecord> records) {
        return userLogsRepository.saveManyNative(records);
    }

    @Transactional
    public int saveAllAsBatchMerged(List<UserLogsRecord> records) {
        return userLogsRepository.saveManyMerged(records);
    }

    @Transactional
    public int saveAllAsBatchMergedNative(List<UserLogsRecord> records) {
        return userLogsRepository.saveManyMergedNative(records);
    }

    @Transactional
    public int saveAllOneTxManyInserts(List<UserLogsRecord> records) {
        int result = 0;
        for (UserLogsRecord record : records) {
            result += userLogsRepository.saveOne(record);
        }
        return result;
    }

    @Transactional
    public int saveAllOneTxManyInsertsMerged(List<UserLogsRecord> records) {
        int result = 0;
        for (UserLogsRecord record : records) {
            result += userLogsRepository.saveOneMerged(record);
        }
        return result;
    }

    public int saveAllTxPerInsert(List<UserLogsRecord> records) {
        int result = 0;
        for (UserLogsRecord record : records) {
            Integer updateCount = transactionTemplate.execute(status -> userLogsRepository.saveOne(record));
            assert updateCount != null;
            result += updateCount;
        }
        return result;
    }
}
