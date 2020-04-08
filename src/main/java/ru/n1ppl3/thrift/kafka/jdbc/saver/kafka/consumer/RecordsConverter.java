package ru.n1ppl3.thrift.kafka.jdbc.saver.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import ru.n1ppl3.thrift.kafka.jdbc.saver.entity.TUserLogRecordV1;
import ru.n1ppl3.thrift.kafka.jdbc.saver.entity.UserLogsRecord;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public abstract class RecordsConverter {


    public static List<UserLogsRecord> transformRecords(ConsumerRecords<String, TUserLogRecordV1> records) {
        List<UserLogsRecord> result = new ArrayList<>(records.count());
        for (ConsumerRecord<String, TUserLogRecordV1> record : records) {
            log.info("ConsumerRecord: {}", record);
            result.add(convertRecord(record));
        }
        return result;
    }


    public static UserLogsRecord convertRecord(ConsumerRecord<String, TUserLogRecordV1> record) {
        String key = record.key();
        TUserLogRecordV1 value = record.value(); // ClassCastException если там другой тип
        return new UserLogsRecord()
                .setOper(value.getOper())
                .setRemoteAddr(value.getRemoteAddr())
                .setComment(value.getComment())
                .setSuccess(value.isSuccess())
                .setPrsId(value.getPrsId())
                .setDateIn(Instant.ofEpochMilli(value.getDateIn().getEpochMillis()))
                .setAllAddr(value.getAllAddr())
                .setUa(value.getUa())
                .setSid(value.getSid())
                .setObjectType(value.getObjectType())
                .setObjectId(value.getObjectId())
                .setMessageId(key);
    }

}
