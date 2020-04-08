package ru.n1ppl3.thrift.kafka.jdbc.saver;

import ru.n1ppl3.thrift.kafka.jdbc.saver.entity.TInstant;
import ru.n1ppl3.thrift.kafka.jdbc.saver.entity.TUserLogRecordV1;
import ru.n1ppl3.thrift.kafka.jdbc.saver.entity.UserLogsRecord;

import java.time.Instant;
import java.time.LocalDateTime;

import static java.time.ZoneOffset.UTC;
import static org.apache.commons.lang3.StringUtils.repeat;


public abstract class SharedTestData {

    public static Instant instant = LocalDateTime.of(2017, 3, 26, 12, 13, 14).toInstant(UTC);

    // сущность только с обязательными полями
    public static UserLogsRecord r1 = new UserLogsRecord()
            .setOper(1043)
            .setComment("Getting founding source balances authorization check")
            .setSuccess(true)
            .setPrsId(142)
            .setDateIn(instant);

    // сущность с максимальным значением всех строк
    public static UserLogsRecord r2 = new UserLogsRecord()
            .setOper(1043)
            .setRemoteAddr(repeat("a", 17))
            .setComment(repeat("b", 1000))
            .setSuccess(false)
            .setPrsId(142)
            .setDateIn(instant)
            .setAllAddr(repeat("c", 50))
            .setUa(repeat("d", 4000))
            .setSid(repeat("e", 100))
            .setObjectType(repeat("f", 30))
            .setObjectId(repeat("g", 40));

    public static TUserLogRecordV1 r3 = new TUserLogRecordV1()
            .setOper(123)
            .setRemoteAddr("my_remote")
            .setComment("my_comment")
            .setSuccess(true)
            .setPrsId(456)
            .setDateIn(new TInstant(instant.toEpochMilli()))
            .setAllAddr("my_all_addr")
            .setUa("my_ua")
            .setSid("my_sid")
            .setObjectType("my_object_type")
            .setObjectId("my_object_id");
}
