package ru.n1ppl3.thrift.kafka.jdbc.saver.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import ru.n1ppl3.thrift.kafka.jdbc.saver.entity.UserLogsRecord;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static ru.n1ppl3.thrift.kafka.jdbc.saver.SharedTestData.instant;
import static ru.n1ppl3.thrift.kafka.jdbc.saver.SharedTestData.r3;


class RecordsConverterTest {


    @Test
    void convertRecordTest() {
        String key = "my_key";
        var in = new ConsumerRecord<>("doesnt_matter", 0, 0, key, r3.deepCopy());
        UserLogsRecord out = RecordsConverter.convertRecord(in);
        assertEquals(123, out.getOper());
        assertEquals("my_remote", out.getRemoteAddr());
        assertEquals("my_comment", out.getComment());
        assertEquals(true, out.isSuccess());
        assertEquals(456, out.getPrsId());
        assertEquals(instant, out.getDateIn());
        assertEquals("my_all_addr", out.getAllAddr());
        assertEquals("my_ua", out.getUa());
        assertEquals("my_sid", out.getSid());
        assertEquals("my_object_type", out.getObjectType());
        assertEquals("my_object_id", out.getObjectId());
        assertEquals(key, out.getMessageId());
    }

}
