package ru.n1ppl3.thrift.kafka.jdbc.saver.kafka;

import lombok.AllArgsConstructor;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import ru.n1ppl3.thrift.kafka.jdbc.saver.utils.ThriftSerde;

import java.util.Arrays;

@AllArgsConstructor
public class ThriftDeserializer<F extends TFieldIdEnum, T extends TBase<T,F>> implements Deserializer<T> {

    private final Class<T> clazz;

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            return ThriftSerde.deserialize(data, clazz);
        } catch (TException e) {
            throw new SerializationException("deserialize error: " + Arrays.toString(data), e);
        }
    }
}
