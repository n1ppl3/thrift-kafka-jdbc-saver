package ru.n1ppl3.thrift.kafka.jdbc.saver.kafka.consumer;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import ru.n1ppl3.thrift.kafka.jdbc.saver.entity.TUserLogRecordV1;
import ru.n1ppl3.thrift.kafka.jdbc.saver.entity.UserLogsRecord;
import ru.n1ppl3.thrift.kafka.jdbc.saver.service.RetryableUserLogsService;

import java.util.List;
import java.util.concurrent.atomic.LongAdder;

@Slf4j
@Component
@AllArgsConstructor
public class MyListener {

    // количество записей, полученных всеми потребителями
    private static LongAdder receivedCount = new LongAdder();


    private final RetryableUserLogsService retryableUserLogsService;


    @KafkaListener(idIsGroup = false, id = "myListenerId", clientIdPrefix = "myClientIdPrefix", topics = "${my.topic.name}")
    public void listen(ConsumerRecords<String, TUserLogRecordV1> records, Acknowledgment acknowledgment) throws Exception {
        log.info("received {} new record(s)!", records.count());

        List<UserLogsRecord> userLogsRecordList = RecordsConverter.transformRecords(records);
        log.info("saved {} new record(s)!", retryableUserLogsService.tryToSaveRecordsToDb(userLogsRecordList));

        acknowledgment.acknowledge(); // TODO FIXME возможно это неэффективно

        receivedCount.add(records.count());
    }


    // for tests
    public static long waitUntilRecordsReceived(int recordToReceive, long millisToWait) throws InterruptedException {
        int passedMillis = 0;
        while (receivedCount.longValue() < recordToReceive) {
            Thread.sleep(500);
            passedMillis += 500;
            if (passedMillis > millisToWait) {
                throw new InterruptedException("still didn't receive " + recordToReceive + " record(s) after " + millisToWait + " millis of waiting...");
            }
        }
        return receivedCount.longValue();
    }

}
