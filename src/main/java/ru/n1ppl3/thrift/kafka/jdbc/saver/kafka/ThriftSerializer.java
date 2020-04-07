package ru.n1ppl3.thrift.kafka.jdbc.saver.kafka;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import ru.n1ppl3.thrift.kafka.jdbc.saver.utils.ThriftSerde;

public class ThriftSerializer<T extends TBase<T,?>> implements Serializer<T> {

    @Override
    public byte[] serialize(String topic, T data) {
        try {
            return ThriftSerde.serialize(data);
        } catch (TException e) {
            throw new SerializationException("error serializing " + data, e);
        }
    }
}
