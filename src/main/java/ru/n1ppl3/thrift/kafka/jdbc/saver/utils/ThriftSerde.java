package ru.n1ppl3.thrift.kafka.jdbc.saver.utils;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Constructor;


public abstract class ThriftSerde {

    private static TCompactProtocol.Factory protocolFactory = new TCompactProtocol.Factory();
    private static TSerializer tSerializer = new TSerializer(protocolFactory);
    private static TDeserializer tDeserializer = new TDeserializer(protocolFactory);

    /**
     * thriftObject to bytes
     */
    public static byte[] serialize(TBase<?, ?> thriftObject) throws TException {
        return tSerializer.serialize(thriftObject);
    }

    /**
     * bytes to thriftObject
     */
    public static <F extends TFieldIdEnum, T extends TBase<T,F>> T deserialize(byte[] bytes, Class<T> _class) throws TException {
        try {
            Constructor<T> constructor = ReflectionUtils.accessibleConstructor(_class);
            T instance = constructor.newInstance();
            tDeserializer.deserialize(instance, bytes);
            return instance;
        } catch (ReflectiveOperationException e) {
            throw new TException("error instantiating " + _class, e);
        }
    }
}
